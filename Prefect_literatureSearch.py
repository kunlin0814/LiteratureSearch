"""
Prefect-based LiteratureSearch workflow.

High-level flow:
1. Load config (tiered or explicit PubMed query, time window, retmax, API keys).
2. Run PubMed eSearch (history mode) to retrieve PMIDs and WebEnv/QueryKey.
3. Validate the result set (hit count and gold-set coverage).
4. If no results, stop early.
5. Fetch metadata via PubMed eSummary and XML via eFetch (abstracts, MeSH, GEO/SRA).
6. Normalize into unified article records with a DedupeKey (DOI or PMID).
7. Query the Notion database and build a {DedupeKey -> page_id} index.
8. Classify each record as “to_create” (new) or “to_update” (existing).
9. Enrich new records with Gemini 2.5 Flash (relevance score, summary, methods, data types) when a GOOGLE_API_KEY is available.
10. Create new Notion pages and update existing ones (unless running in --dry-run mode).

- Environment variables:
    NCBI_API_KEY, NCBI_EMAIL, NOTION_TOKEN, NOTION_DB_ID, GOOGLE_API_KEY
"""

import os
import time
import json
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Dict, Tuple, Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dateutil import parser as date_parser
from prefect import flow, task, get_run_logger
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()  # This loads the variables from .env into os.environ

# Configure Gemini
genai.configure(api_key=os.environ.get("GOOGLE_API_KEY"))


# -------------------------
# 1. CONFIG & UTILS
# -------------------------

def _make_session(max_retries: int = 5, backoff_factor: float = 1.0) -> requests.Session:
    """
    Create a requests session with retry/backoff on 429/5xx and connection errors.
    """
    session = requests.Session()
    retry = Retry(
        total=max_retries,
        connect=max_retries,
        read=max_retries,
        status=max_retries,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST", "PATCH"]),
        backoff_factor=backoff_factor,
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _truncate(text: Optional[str], limit: int = 2000) -> str:
    """Safe truncation to avoid Notion API 400 errors."""
    if not text:
        return ""
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


@task
def get_config(
    query_term: Optional[str] = None,
    rel_date_days: Optional[int] = None,
    retmax: Optional[int] = None,
    dry_run: Optional[bool] = None,
    tier: Optional[int] = 1,
) -> Dict[str, Any]:
    """
    Equivalent to the n8n Config nodes.
    """
    # Tiered default queries

    # Tier 1: prostate-focused, high precision
    tier1_query = """
    ("Prostatic Neoplasms"[MeSH Terms] 
    OR prostate[tiab] 
    OR prostatic[tiab] 
    OR "prostate cancer"[tiab])
    AND
    ("spatial transcriptom*"[tiab] OR "spatial gene expression"[tiab] 
    OR "spatial multiomic*"[tiab] OR "spatial omics"[tiab] 
    OR Visium[tiab] OR Xenium[tiab] OR CosMx[tiab] OR GeoMx[tiab]
    OR "Slide-seq"[tiab] OR "SlideSeq"[tiab] 
    OR "spatial ATAC"[tiab] OR "spatial-ATAC"[tiab] 
    OR "single-cell"[tiab] OR "single cell"[tiab] 
    OR "single-nucleus"[tiab] OR "single nucleus"[tiab] 
    OR scRNA*[tiab] OR snRNA*[tiab] OR scATAC*[tiab] OR snATAC*[tiab] 
    OR multiome[tiab] OR "10x multiome"[tiab] 
    OR pseudotime[tiab] OR "trajectory inference"[tiab] OR "RNA velocity"[tiab])
    AND ("Journal Article"[pt] 
    NOT "Review"[pt] 
    NOT "Editorial"[pt] 
    NOT "Comment"[pt] 
    NOT "Letter"[pt] 
    NOT "News"[pt] 
    NOT "Case Reports"[pt])
    AND english[la]
    NOT "Preprint"[Publication Type]
"""

    # Tier 2: broader cancer, method/technology-oriented
    tier2_query = """
    ("Neoplasms"[MeSH Terms]
    OR cancer[tiab]
    OR cancers[tiab]
    OR carcinoma[tiab]
    OR carcinomas[tiab]
    OR tumor[tiab]
    OR tumors[tiab]
    OR malignan*[tiab])
    AND
    ("spatial transcriptom*"[tiab] OR "spatial gene expression"[tiab] 
    OR "spatial multiomic*"[tiab] OR "spatial omics"[tiab] 
    OR Visium[tiab] OR Xenium[tiab] OR CosMX[tiab] OR GeoMx[tiab] 
    OR "Slide-seq"[tiab] OR "SlideSeq"[tiab] 
    OR "spatial ATAC"[tiab] OR "spatial-ATAC"[tiab] 
    OR "single-cell"[tiab] OR "single cell"[tiab] 
    OR "single-nucleus"[tiab] OR "single nucleus"[tiab] 
    OR scRNA*[tiab] OR snRNA*[tiab] OR scATAC*[tiab] OR snATAC*[tiab] 
    OR multiome[tiab] OR "10x multiome"[tiab] 
    OR pseudotime[tiab] OR "trajectory inference"[tiab] OR "RNA velocity"[tiab])
    AND (
    "Journal Article"[pt] 
    NOT "Review"[pt] 
    NOT "Editorial"[pt] 
    NOT "Comment"[pt] 
    NOT "Letter"[pt] 
    NOT "News"[pt] 
    NOT "Case Reports"[pt]
    )
    AND english[la]
    NOT "Preprint"[Publication Type]
    """

    # Choose query: explicit > tiered defaults
    if query_term:
        resolved_query = query_term
    else:
        if tier == 2:
            resolved_query = tier2_query
        else:
            resolved_query = tier1_query
    cfg = {
        "QUERY_TERM": resolved_query,
        "RETMAX": int(retmax) if retmax is not None else 200,
        "RELDATE_DAYS": int(rel_date_days) if rel_date_days is not None else 365,
        "NCBI_API_KEY": os.environ.get("NCBI_API_KEY", ""),
        "EMAIL": os.environ.get("NCBI_EMAIL", ""),
        "DATETYPE": os.environ.get("NCBI_DATETYPE", "pdat"),
        "HISTORICAL_MEDIAN": 500,
        "GOLD_SET": [
            "36750562",  # Example PMID
            "10.1038/s41467-023-36325-2",  # Example DOI
        ],
        "NOTION_TOKEN": os.environ.get("NOTION_TOKEN", ""),
        "NOTION_DB_ID": os.environ.get("NOTION_DB_ID", ""),
        "DRY_RUN": bool(dry_run) if dry_run is not None else False,
        "EUTILS_BATCH": 200,
        "EUTILS_TOOL": "prefect-litsearch",
    }
    return cfg


# -------------------------
# 2. PubMed eSearch
# -------------------------

@task(retries=2, retry_delay_seconds=10)
def pubmed_esearch(cfg: Dict[str, Any]) -> Dict[str, Any]:
    logger = get_run_logger()
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    session = _make_session()
    params = {
        "db": "pubmed",
        "retmode": "json",
        "retmax": cfg["RETMAX"],
        "sort": "pub+date",
        "term": cfg["QUERY_TERM"],
        "email": cfg["EMAIL"],
        "api_key": cfg["NCBI_API_KEY"],
        "datetype": cfg.get("DATETYPE", "pdat"),
        "reldate": cfg["RELDATE_DAYS"],
        "usehistory": "y",
        "tool": cfg.get("EUTILS_TOOL", "prefect-litsearch"),
    }

    logger.info(f"ESearch → query: {cfg['QUERY_TERM']}")
    resp = session.get(base_url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    result = data.get("esearchresult", {})
    count = int(result.get("count", 0))
    ids = result.get("idlist", [])
    webenv = result.get("webenv")
    query_key = result.get("querykey")

    logger.info(f"ESearch count={count}, ids(first_page)={len(ids)}")
    return {"count": count, "ids": ids, "webenv": webenv, "query_key": query_key}


# -------------------------
# 2.5 PMC Full Text Retrieval
# -------------------------

def _extract_pmc_sections(xml_text: str) -> str:
    """
    Parses PMC XML to extract Abstract, Methods, and Results.
    Discards Introduction, Discussion, References to save tokens.
    """
    try:
        root = ET.fromstring(xml_text)
        
        # 1. Abstract
        abstract_parts = []
        for abst in root.findall(".//abstract"):
            for p in abst.findall(".//p"):
                if p.text:
                    abstract_parts.append(p.text.strip())
        abstract_text = " ".join(abstract_parts)

        # 2. Body Sections
        methods_text = []
        results_text = []
        
        # Iterate over all sections in the body
        data_code_text = []
        
        for sec in root.findall(".//sec"):
            title_elem = sec.find("title")
            title = title_elem.text.lower() if (title_elem is not None and title_elem.text) else ""
            sec_type = sec.attrib.get("sec-type", "").lower()
            
            # Get all text in this section
            # (naive: itertext)
            full_sec_text = "".join(sec.itertext()).strip()
            if not full_sec_text:
                continue
                
            if "methods" in title or "methods" in sec_type:
                methods_text.append(f"--- Section: {title_elem.text if title_elem is not None else 'Methods'} ---\n{full_sec_text}")
            elif "results" in title or "results" in sec_type:
                results_text.append(f"--- Section: {title_elem.text if title_elem is not None else 'Results'} ---\n{full_sec_text}")
            elif "data availability" in title or "code availability" in title or "availability" in title:
                data_code_text.append(f"--- Section: {title_elem.text if title_elem is not None else 'Availability'} ---\n{full_sec_text}")

        # Combine
        final_parts = []
        if abstract_text:
            final_parts.append(f"ABSTRACT:\n{abstract_text}")
        if methods_text:
            final_parts.append(f"METHODS:\n" + "\n\n".join(methods_text))
        if results_text:
            final_parts.append(f"RESULTS:\n" + "\n\n".join(results_text))
        if data_code_text:
            final_parts.append(f"DATA & CODE AVAILABILITY:\n" + "\n\n".join(data_code_text))
            
        return "\n\n".join(final_parts)
    except Exception as e:
        return f"Error parsing XML: {str(e)}"


# -------------------------
# 3. Validate Results
# -------------------------

@task
def validate_results(esearch_out: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
    logger = get_run_logger()
    count = esearch_out["count"]
    ids = esearch_out["ids"]

    historical_median = cfg["HISTORICAL_MEDIAN"]
    drop_threshold = historical_median * 0.5
    jump_threshold = historical_median * 2

    gold_set = set(cfg["GOLD_SET"])
    found_gold = any(i in gold_set for i in ids)
    gold_missing = bool(gold_set) and not found_gold

    status = "OK"
    if count == 0 or count < drop_threshold or count > jump_threshold or gold_missing:
        status = "ALERT"

    logger.info(f"Validation → count={count}, gold_missing={gold_missing}, status={status}")

    return {
        "validation": {
            "count": count,
            "ids": ids,
            "goldMissing": gold_missing,
            "status": status,
        }
    }


# -------------------------
# 4. PubMed eSummary + eFetch
# -------------------------


@task(retries=2, retry_delay_seconds=10)
def pubmed_esummary_history(cfg: Dict[str, Any], esearch_out: Dict[str, Any], batch_size: int, start_offset: int = 0) -> Dict[str, Any]:
    """Fetch eSummary data using WebEnv/query_key history."""
    logger = get_run_logger()
    webenv = esearch_out.get("webenv")
    query_key = esearch_out.get("query_key")
    if not webenv or not query_key:
        logger.info("No history context; skipping esummary.")
        return {}

    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
    session = _make_session()
    
    params = {
        "db": "pubmed",
        "retmode": "json",
        "retstart": start_offset,
        "retmax": batch_size,
        "query_key": query_key,
        "WebEnv": webenv,
        "email": cfg["EMAIL"],
        "api_key": cfg["NCBI_API_KEY"],
        "tool": cfg.get("EUTILS_TOOL", "prefect-litsearch"),
    }
    logger.info(f"ESummary batch start={start_offset} size={batch_size}")
    resp = session.get(base_url, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


@task(retries=2, retry_delay_seconds=10)
def pubmed_efetch_abstracts_by_ids(cfg: Dict[str, Any], pmids: List[str]) -> Dict[str, Dict[str, Optional[str]]]:
    logger = get_run_logger()
    if not pmids:
        return {}

    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    session = _make_session()
    batch_size = int(cfg.get("EUTILS_BATCH", 200))
    efetch_map: Dict[str, Dict[str, Optional[str]]] = {}

    for i in range(0, len(pmids), batch_size):
        batch = pmids[i:i+batch_size]
        params = {
            "db": "pubmed",
            "retmode": "xml",
            "id": ",".join(batch),
            "email": cfg["EMAIL"],
            "api_key": cfg["NCBI_API_KEY"],
            "tool": cfg.get("EUTILS_TOOL", "prefect-litsearch"),
        }
        logger.info(f"EFetch (IDs) batch start={i} size={len(batch)}")
        resp = session.get(base_url, params=params, timeout=60)
        resp.raise_for_status()
        xml_text = resp.text
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            xml_text_wrapped = f"<PubmedArticleSet>{xml_text}</PubmedArticleSet>"
            root = ET.fromstring(xml_text_wrapped)

        for art in root.findall(".//PubmedArticle"):
            pmid_elem = art.find(".//PMID")
            pmid = pmid_elem.text.strip() if pmid_elem is not None else None
            if not pmid:
                continue
            # serialize this article as XML for Gemini enrichment
            article_xml = ET.tostring(art, encoding="unicode")
            # Abstract
            abst_elems = art.findall(".//AbstractText")
            abstract = " ".join((e.text or "").strip() for e in abst_elems if e.text) or None

            # Extract PMCID if present in ArticleIdList
            pmcid = None
            for aid in art.findall(".//ArticleId"):
                if aid.attrib.get("IdType") == "pmc":
                    pmcid = aid.text.strip()
                    break

            # --- Extract GEO and SRA from DataBankList and ReferenceList ---
            geo_accessions = set()
            sra_accessions = set()
            
            # Method 1: DataBankList (structured metadata)
            data_bank_list_elem = art.find(".//DataBankList")
            if data_bank_list_elem is not None:
                for databank in data_bank_list_elem.findall(".//DataBank"):
                    db_name_elem = databank.find("DataBankName")
                    db_name = db_name_elem.text.strip() if db_name_elem is not None and db_name_elem.text else ""
                    acc_list_elem = databank.find("AccessionNumberList")
                    if acc_list_elem is not None:
                        accs = [acc.text.strip() for acc in acc_list_elem.findall("AccessionNumber") if acc.text]
                        if db_name.upper() == "GEO":
                            geo_accessions.update(accs)
                        elif db_name.upper() == "SRA":
                            sra_accessions.update(accs)
            
            # Method 2: ReferenceList (fallback for papers that cite data as references)
            # Many newer papers include GEO/SRA accessions in reference citations
            # Note: ReferenceList is under PubmedData, not MedlineCitation, so we search from parent
            import re
            # Get the parent PubmedArticle element to access PubmedData/ReferenceList
            # Since we're iterating through PubmedArticle elements, we can search within them
            ref_list_elem = None
            # Try to find ReferenceList - could be at different nesting levels
            for possible_ref_list in [art.find(".//ReferenceList"), art.find("../PubmedData/ReferenceList" if hasattr(art, 'find') else None)]:
                if possible_ref_list is not None:
                    ref_list_elem = possible_ref_list
                    break
            
            # If still not found, try getting it from the article XML directly
            if ref_list_elem is None:
                # Parse the full article as string to find ReferenceList anywhere
                article_str = ET.tostring(art, encoding='unicode')
                if '<ReferenceList>' in article_str:
                    # Create a temporary element to parse the full article structure
                    temp_root = ET.fromstring(f'<temp>{article_str}</temp>')
                    ref_list_elem = temp_root.find('.//ReferenceList')
            
            if ref_list_elem is not None:
                for ref in ref_list_elem.findall('.//Reference'):
                    citation_elem = ref.find('Citation')
                    if citation_elem is not None:
                        # Citation may have child elements like <i>, so use itertext() to get all text
                        citation_text = ''.join(citation_elem.itertext())
                        # Extract GEO accessions (GSE followed by digits)
                        geo_matches = re.findall(r'GSE\d+', citation_text)
                        geo_accessions.update(geo_matches)
                        # Extract SRA/BioProject accessions
                        sra_matches = re.findall(r'(?:PRJNA|SRP|SRR|SRX|SRS)\d+', citation_text)
                        sra_accessions.update(sra_matches)
            
            geo_list = ", ".join(sorted(geo_accessions)) if geo_accessions else ""
            sra_project = ", ".join(sorted(sra_accessions)) if sra_accessions else ""

            # Metadata extraction (Mesh, etc) - reused logic could be refactored but keeping inline for safety
            mesh_terms_set = set()
            major_mesh_set = set()
            mesh_heading_entries = []
            mesh_terms = ""
            major_mesh = ""

            for mh in art.findall(".//MeshHeading"):
                desc = mh.find("DescriptorName")
                if desc is None: 
                    continue
                desc_text = desc.text
                major_topic_yn = desc.attrib.get("MajorTopicYN", "N")

                qualifiers = []
                for q in mh.findall("QualifierName"):
                    if q.text:
                        qualifiers.append(q.text)
                        if q.attrib.get("MajorTopicYN", "N") == "Y":
                            major_topic_yn = "Y"

                if qualifiers:
                    entry = f"{desc_text} ({', '.join(qualifiers)})"
                else:
                    entry = desc_text
                mesh_heading_entries.append(entry)
                mesh_terms_set.add(desc_text)
                if major_topic_yn == "Y":
                    major_mesh_set.add(desc_text)

            mesh_heading_list = "; ".join(mesh_heading_entries) if mesh_heading_entries else ""
            if mesh_terms_set:
                mesh_terms = "; ".join(sorted(mesh_terms_set))
            if major_mesh_set:
                major_mesh = "; ".join(sorted(major_mesh_set))

            efetch_map[pmid] = {
                "Abstract": abstract,
                "ArticleXML": article_xml,
                "PMCID": pmcid,
                "GEO_List": geo_list,
                "SRA_Project": sra_project,
                "MeshHeadingList": mesh_heading_list,
                "MeSH_Terms": mesh_terms,
                "Major_MeSH": major_mesh,
            }

        time.sleep(0.5)

    return efetch_map



# -------------------------
# 3. Validate Results
# -------------------------

@task
def validate_results(esearch_out: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
    logger = get_run_logger()
    count = esearch_out["count"]
    ids = esearch_out["ids"]

    historical_median = cfg["HISTORICAL_MEDIAN"]
    drop_threshold = historical_median * 0.5
    jump_threshold = historical_median * 2

    gold_set = set(cfg["GOLD_SET"])
    found_gold = any(i in gold_set for i in ids)
    gold_missing = bool(gold_set) and not found_gold

    status = "OK"
    if count == 0 or count < drop_threshold or count > jump_threshold or gold_missing:
        status = "ALERT"

    logger.info(f"Validation → count={count}, gold_missing={gold_missing}, status={status}")

    return {
        "validation": {
            "count": count,
            "ids": ids,
            "goldMissing": gold_missing,
            "status": status,
        }
    }


# -------------------------
# 4. PubMed eSummary + eFetch
# -------------------------




@task(retries=2, retry_delay_seconds=10)
def pubmed_efetch_abstracts_history(cfg: Dict[str, Any], esearch_out: Dict[str, Any], total: int) -> Dict[str, Dict[str, Optional[str]]]:
    logger = get_run_logger()
    webenv = esearch_out.get("webenv")
    query_key = esearch_out.get("query_key")
    if not webenv or not query_key or total == 0:
        logger.info("No history context; skipping efetch history.")
        return {}

    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    session = _make_session()
    batch = int(cfg.get("EUTILS_BATCH", 200))
    efetch_map: Dict[str, Dict[str, Optional[str]]] = {}

    for start in range(0, total, batch):
        params = {
            "db": "pubmed",
            "retmode": "xml",
            "retstart": start,
            "retmax": min(batch, total - start),
            "query_key": query_key,
            "WebEnv": webenv,
            "email": cfg["EMAIL"],
            "api_key": cfg["NCBI_API_KEY"],
            "tool": cfg.get("EUTILS_TOOL", "prefect-litsearch"),
        }
        logger.info(f"EFetch batch start={start} size={batch}")
        resp = session.get(base_url, params=params, timeout=60)
        resp.raise_for_status()
        xml_text = resp.text
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            xml_text_wrapped = f"<PubmedArticleSet>{xml_text}</PubmedArticleSet>"
            root = ET.fromstring(xml_text_wrapped)

        for art in root.findall(".//PubmedArticle"):
            pmid_elem = art.find(".//PMID")
            pmid = pmid_elem.text.strip() if pmid_elem is not None else None
            if not pmid:
                continue
            # serialize this article as XML for Gemini enrichment
            article_xml = ET.tostring(art, encoding="unicode")
            # Abstract
            abst_elems = art.findall(".//AbstractText")
            abstract = " ".join((e.text or "").strip() for e in abst_elems if e.text) or None

            # --- Extract PMCID ---
            pmcid = None
            article_id_list = art.find(".//ArticleIdList")
            if article_id_list is not None:
                for aid in article_id_list.findall("ArticleId"):
                    if aid.attrib.get("IdType") == "pmc":
                        pmcid = aid.text.strip()
                        break

            # --- Extract GEO and SRA ---
            geo_list = ""
            sra_project = ""
            data_bank_list_elem = art.find(".//DataBankList")
            if data_bank_list_elem is not None:
                geo_accessions = set()
                sra_accessions = set()
                for databank in data_bank_list_elem.findall(".//DataBank"):
                    db_name_elem = databank.find("DataBankName")
                    db_name = db_name_elem.text.strip() if db_name_elem is not None and db_name_elem.text else ""
                    acc_list_elem = databank.find("AccessionNumberList")
                    if acc_list_elem is not None:
                        accs = [acc.text.strip() for acc in acc_list_elem.findall("AccessionNumber") if acc.text]
                        if db_name.upper() == "GEO":
                            geo_accessions.update(accs)
                        elif db_name.upper() == "SRA":
                            sra_accessions.update(accs)
                if geo_accessions:
                    geo_list = ", ".join(sorted(geo_accessions))
                if sra_accessions:
                    sra_project = ", ".join(sorted(sra_accessions))

            # --- Extract MeSH ---
            mesh_heading_list = ""
            mesh_terms = ""
            major_mesh = ""
            mesh_heading_entries = []
            mesh_terms_set = set()
            major_mesh_set = set()
            mesh_heading_list_elem = art.find(".//MeshHeadingList")
            if mesh_heading_list_elem is not None:
                for mesh_heading in mesh_heading_list_elem.findall("MeshHeading"):
                    desc_elem = mesh_heading.find("DescriptorName")
                    if desc_elem is None or desc_elem.text is None:
                        continue
                    desc_text = desc_elem.text.strip()
                    major_topic_yn = desc_elem.attrib.get("MajorTopicYN", "N")
                    qualifiers = [q.text.strip() for q in mesh_heading.findall("QualifierName") if q.text]
                    if qualifiers:
                        entry = f"{desc_text} ({', '.join(qualifiers)})"
                    else:
                        entry = desc_text
                    mesh_heading_entries.append(entry)
                    mesh_terms_set.add(desc_text)
                    if major_topic_yn == "Y":
                        major_mesh_set.add(desc_text)
                if mesh_heading_entries:
                    mesh_heading_list = "; ".join(mesh_heading_entries)
                if mesh_terms_set:
                    mesh_terms = "; ".join(sorted(mesh_terms_set))
                if major_mesh_set:
                    major_mesh = "; ".join(sorted(major_mesh_set))

            efetch_map[pmid] = {
                "Abstract": abstract,
                "PMCID": pmcid,
                "GEO_List": geo_list,
                "SRA_Project": sra_project,
                "MeshHeadingList": mesh_heading_list,
                "MeSH_Terms": mesh_terms,
                "Major_MeSH": major_mesh,
                "RawXML": article_xml,
            }
        time.sleep(0.34)

    return efetch_map


@task(retries=2, retry_delay_seconds=10)
def fetch_pmc_fulltext(cfg: Dict[str, Any], efetch_map: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    1. Identify records with a PMCID.
    2. Batch fetch full text XML from PMC.
    3. Extract Abstract + Methods + Results.
    Returns: {PMID: {"full_text": str, "pmcid": str, "used": bool}}
    """
    logger = get_run_logger()
    
    # Identify PMCIDs to fetch
    pmid_to_pmcid = {}
    pmcid_to_pmid = {}
    
    for pmid, data in efetch_map.items():
        pmcid = data.get("PMCID")
        if pmcid:
            pmid_to_pmcid[pmid] = pmcid
            pmcid_to_pmid[pmcid] = pmid
            
    if not pmcid_to_pmid:
        logger.info("No PMCIDs found in this batch.")
        return {}

    pmcids = list(pmcid_to_pmid.keys())
    logger.info(f"Found {len(pmcids)} PMC full-text candidates.")
    
    session = _make_session()
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    batch_size = 50 # Smaller batch for full text XML
    
    results = {}

    for i in range(0, len(pmcids), batch_size):
        batch = pmcids[i:i+batch_size]
        # Strip 'PMC' prefix for efetch
        batch_ids = [pid.replace("PMC", "") for pid in batch]
        
        params = {
            "db": "pmc",
            "retmode": "xml",
            "id": ",".join(batch_ids),
            "email": cfg["EMAIL"],
            "api_key": cfg["NCBI_API_KEY"],
        }
        
        try:
            resp = session.get(base_url, params=params, timeout=120)
            resp.raise_for_status()
            
            # Parse the big XML response
            # It will contain multiple <article> nodes
            root = ET.fromstring(resp.text)
            
            for article in root.findall(".//article"):
                # Find the PMCID of this article to map back to PMID
                # Usually in <front><article-meta><article-id pub-id-type="pmc">
                this_pmcid = None
                for aid in article.findall(".//article-id"):
                    if aid.attrib.get("pub-id-type") in ["pmc", "pmcid"]:
                        this_pmcid = aid.text.strip()
                        if not this_pmcid.startswith("PMC"):
                            this_pmcid = "PMC" + this_pmcid
                        break
                
                if not this_pmcid or this_pmcid not in pmcid_to_pmid:
                    # Fallback: try to find PMID?
                    continue
                
                pmid = pmcid_to_pmid[this_pmcid]
                
                # Serialize to string to pass to our extractor
                article_xml_str = ET.tostring(article, encoding="unicode")
                extracted_text = _extract_pmc_sections(article_xml_str)
                
                results[pmid] = {
                    "full_text": extracted_text,
                    "pmcid": this_pmcid,
                    "used": True
                }
                
            time.sleep(0.5)
            
        except Exception as e:
            logger.error(f"Error fetching PMC batch {i}: {e}")
            
    logger.info(f"Successfully extracted full text for {len(results)} articles.")
    return results


# -------------------------
# 5. Normalize + Parse
# -------------------------

@task
def normalize_records(
    esummary_json: Dict[str, Any],
    efetch_data: Any,
) -> List[Dict[str, Any]]:
    logger = get_run_logger()
    records: Dict[str, Dict[str, Any]] = {}

    # ---- from eSummary ----
    if esummary_json:
        result = esummary_json.get("result", {})
        for pmid, rec in result.items():
            if not pmid.isdigit():
                continue
            title = rec.get("title")
            journal = rec.get("fulljournalname") or rec.get("source")
            pubdate_raw = rec.get("pubdate") or rec.get("sortpubdate")
            doi = None
            for id_obj in rec.get("articleids", []):
                if id_obj.get("idtype") == "doi":
                    doi = id_obj.get("value")
                    break
            pub_types = []
            for pt in rec.get("pubtype", []):
                if pt:
                    pub_types.append(pt.strip())

            pubdate = None
            if pubdate_raw:
                try:
                    pubdate = date_parser.parse(pubdate_raw, default=datetime(1900, 1, 1)).date()
                except Exception:
                    pubdate = None

            authors_list = rec.get("authors", [])
            author_names = []
            for author in authors_list:
                name = author.get("name")
                if name:
                    author_names.append(name)
                else:
                    lastname = author.get("lastname")
                    firstname = author.get("firstname")
                    combined = " ".join(filter(None, [lastname, firstname]))
                    if combined:
                        author_names.append(combined)
            authors_str = ", ".join(author_names) if author_names else None

            records[pmid] = {
                "PMID": pmid,
                "Title": title,
                "Journal": journal,
                "PubDate": pubdate_raw,
                "PubDateParsed": pubdate,
                "DOI": doi,
                "URL": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
                "Abstract": None,
                "Authors": authors_str,
                "GEO_List": "",
                "SRA_Project": "",
                "MeshHeadingList": "",
                "MeSH_Terms": "",
                "Major_MeSH": "",
                "PublicationTypes": "; ".join(pub_types),
            }

    # ---- from eFetch (abstracts map) ----
    if isinstance(efetch_data, dict) and efetch_data:
        for pmid, entry in efetch_data.items():
            if isinstance(entry, dict):
                abstract = entry.get("Abstract")
                geo_list = entry.get("GEO_List", "")
                sra_project = entry.get("SRA_Project", "")
                mesh_heading_list = entry.get("MeshHeadingList", "")
                mesh_terms = entry.get("MeSH_Terms", "")
                major_mesh = entry.get("Major_MeSH", "")
            else:
                abstract = entry
                geo_list = ""
                sra_project = ""
                mesh_heading_list = ""
                mesh_terms = ""
                major_mesh = ""
            if pmid not in records:
                records[pmid] = {
                    "PMID": pmid,
                    "Title": None,
                    "Journal": None,
                    "PubDate": None,
                    "PubDateParsed": None,
                    "DOI": None,
                    "URL": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
                    "Abstract": abstract,
                    "Authors": None,
                    "GEO_List": geo_list,
                    "SRA_Project": sra_project,
                    "MeshHeadingList": mesh_heading_list,
                    "MeSH_Terms": mesh_terms,
                    "Major_MeSH": major_mesh,
                    "PublicationTypes": "",
                }
            else:
                records[pmid]["Abstract"] = abstract
                records[pmid]["GEO_List"] = geo_list
                records[pmid]["SRA_Project"] = sra_project
                records[pmid]["MeshHeadingList"] = mesh_heading_list
                records[pmid]["MeSH_Terms"] = mesh_terms
                records[pmid]["Major_MeSH"] = major_mesh

    out: List[Dict[str, Any]] = []
    for pmid, rec in records.items():
        doi = rec.get("DOI")
        dedupe_key = doi if doi else f"PMID:{pmid}"
        rec["DedupeKey"] = dedupe_key
        out.append(rec)

    logger.info(f"Normalized {len(out)} records.")
    return out


# -------------------------
# 6. Gemini Enrichment (Native JSON Mode)
# -------------------------

@task(retries=3, retry_delay_seconds=5)
def gemini_enrich_records(
    records: List[Dict[str, Any]],
    efetch_data: Dict[str, Dict[str, Optional[str]]],
    pmc_fulltext_map: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    logger = get_run_logger()

    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        logger.warning("GOOGLE_API_KEY not set; skipping Gemini enrichment.")
        return records

    # Schema definition
    response_schema = {
        "type": "OBJECT",
        "properties": {
            "RelevanceScore": {"type": "INTEGER"},
            "WhyRelevant": {"type": "STRING"},
            "StudySummary": {"type": "STRING"},
            "Methods": {"type": "STRING"},
            "KeyFindings": {"type": "STRING"},
            "DataTypes": {"type": "STRING"},
        },
        "required": ["RelevanceScore", "WhyRelevant", "StudySummary", "Methods", "KeyFindings", "DataTypes"]
    }

    SYSTEM_INSTRUCTION = """You are a PhD-level bioinformatics curator specializing in cancer biology, spatial and single-cell technologies, and computational methods.
You will be given the text of a scientific paper (Abstract, Methods, Results, and Data/Code Availability).

Your task is to extract key information and assess the paper's relevance to a database of "Prostate Cancer + Spatial/Single-Cell/Multiomics" research.

FIELDS TO EXTRACT:
1. RelevanceScore (0-100):
   - 100: Directly uses spatial transcriptomics (Visium, Xenium, CosMx, etc.) or single-cell multiomics on PROSTATE cancer samples.
   - 80-90: Prostate cancer scRNA-seq/snRNA-seq (no spatial) OR Spatial methods development (not prostate specific but highly relevant method).
   - 50-70: Prostate cancer bulk sequencing, or general cancer spatial reviews.
   - <50: Irrelevant (e.g., different organ, purely clinical without omics).

2. DataTypes:
   - Comma-separated list of specific technologies used.
   - Examples: "10x Visium", "Xenium", "CosMx", "scRNA-seq", "snRNA-seq", "scATAC-seq", "Multiome", "Stereo-seq", "Slide-seq".
   - Be specific.

3. Methods:
   - Concise summary (2-3 sentences) of the experimental and computational methods.

4. KeyFindings:
   - Concise summary (2-3 sentences) of the main biological or technical findings.

5. WhyRelevant:
   - Explicitly state *why* the paper is useful for this database in 1–2 sentences.
   - Mention both tumor context (e.g., prostate vs. breast vs. pan-cancer) and technology/method relevance.
   - If relevance is mainly method-level (e.g., new trajectory algorithm or CNV-calling pipeline), say so clearly.

6. StudySummary:
   - Concise summary of the overall study.

CRITICAL INSTRUCTIONS:
- If the provided text is empty or contains NO abstract/methods/results (e.g., only a title), set RelevanceScore to 0 and write "No abstract or full text available" in WhyRelevant. Leave other fields empty.
- CONSISTENCY CHECK: If you write that the paper is relevant in WhyRelevant, RelevanceScore MUST be > 0. Do not give a score of 0 if you say it is relevant.
- Do not leave StudySummary or KeyFindings empty unless there is absolutely no text to analyze.

OUTPUT:
Return JSON only matching the provided schema the user defined:
{ "RelevanceScore": int, "WhyRelevant": str, "StudySummary": str, "Methods": str, "KeyFindings": str, "DataTypes": str }.
No markdown, no extra keys, no conversation."""

    try:
        model = genai.GenerativeModel(
            model_name="gemini-2.5-flash",
            system_instruction=SYSTEM_INSTRUCTION,
            generation_config={
                "temperature": 0.1,
                "response_mime_type": "application/json",
                "response_schema": response_schema,
            }
        )
    except Exception as e:
        logger.error(f"Model init failed: {e}")
        return records

    KNOWN_DATA_TYPES = {
        "scrna-seq", "scatac-seq", "scdna","spatial transcriptomics", "10x visium",
        "xenium", "cosmx", "geomx", "slide-seq", "h&e", "wgs", "wes",
        "bulk rna-seq", "chip-seq", "atac-seq", "cite-seq", "multiome",
        "cnv", "snrna-seq", "snatac-seq", "merfish", "seqfish"
    }

    enriched: List[Dict[str, Any]] = []
    
    for rec in records:
        pmid = str(rec.get("PMID", "")).strip()
        
        # Check for full text first
        full_text_entry = pmc_fulltext_map.get(pmid)
        full_text_used = False
        
        if full_text_entry and full_text_entry.get("full_text"):
            text_to_analyze = f"Analysis based on Full Text (Abstract + Methods + Results + Data/Code Availability):\n\n{full_text_entry['full_text']}"
            full_text_used = True
        else:
            # Fallback to Abstract XML
            # We need to extract text from the XML if possible, or just use what we have
            # The `efetch_data` has "Abstract" key which is already cleaned text
            abstract_text = efetch_data.get(pmid, {}).get("Abstract", "")
            if not abstract_text:
                 text_to_analyze = "Title: " + str(rec.get("Title", "")) + "\n\n(No Abstract Available)"
            else:
                text_to_analyze = f"Analysis based on Abstract:\n\n{abstract_text}"

        try:
            prompt = f"Analyze this text for PMID {pmid}:\n\n{text_to_analyze}"
            resp = model.generate_content(prompt)
            parsed = json.loads(resp.text)
            
            # Defaulting logic if keys missing
            parsed.setdefault("RelevanceScore", 0)
            parsed.setdefault("WhyRelevant", "Analysis failed or returned empty.")
            parsed.setdefault("StudySummary", "")
            parsed.setdefault("Methods", "")
            parsed.setdefault("KeyFindings", "")
            parsed.setdefault("DataTypes", "")

            data_types_raw = parsed.get("DataTypes", "")
            if data_types_raw:
                raw_types = [t.strip().lower() for t in data_types_raw.replace(";", ",").split(",") if t.strip()]
                normalized_types = []
                for dt in raw_types:
                    # Check if known
                    matched = False
                    for known in KNOWN_DATA_TYPES:
                        if known in dt:
                            normalized_types.append(known)
                            matched = True
                            break
                    if not matched:
                        normalized_types.append(dt)
                parsed["DataTypes"] = ", ".join(list(dict.fromkeys(normalized_types)))

            relevance_score = parsed.get("RelevanceScore", 0)
            why_relevant = parsed.get("WhyRelevant", "")
            
            # Consistency Check (Self-Correction)
            if relevance_score == 0 and ("highly relevant" in why_relevant.lower() or "relevant to prostate" in why_relevant.lower()):
                # If text says relevant but score is 0, bump it to 50 (Medium)
                relevance_score = 50
                parsed["RelevanceScore"] = 50
            
            methods_str = parsed.get("Methods", "").lower()
            key_findings_str = parsed.get("KeyFindings", "").lower()
            
            # Pipeline Confidence Logic
            confidence = "Low"
            if full_text_used:
                confidence = "High"
            else:
                # Abstract only
                # If score >= 80, Medium
                if relevance_score >= 80:
                    confidence = "Medium"
                else:
                    # Check keywords
                    strong_keywords = ["spatial", "visium", "xenium", "cosmx", "scrna", "snrna", "multiome"]
                    if any(k in methods_str for k in strong_keywords) or any(k in key_findings_str for k in strong_keywords):
                        confidence = "Medium"

            rec.update({
                "RelevanceScore": relevance_score,
                "WhyRelevant": parsed.get("WhyRelevant", ""),
                "StudySummary": parsed.get("StudySummary", ""),
                "Methods": parsed.get("Methods", ""),
                "KeyFindings": parsed.get("KeyFindings", ""),
                "DataTypes": parsed.get("DataTypes", ""),
                "PipelineConfidence": confidence,
                "FullTextUsed": full_text_used
            })
            
            time.sleep(1) # Rate limiting
            
        except Exception as e:
            logger.warning(f"Gemini enrichment failed for PMID={pmid}: {e}")
            rec["WhyRelevant"] = f"Error during AI enrichment: {str(e)}"
            # Ensure other fields are not None to avoid Notion errors if schema expects string
            rec.setdefault("RelevanceScore", 0)
            rec.setdefault("StudySummary", "")
            rec.setdefault("Methods", "")
            rec.setdefault("KeyFindings", "")
            rec.setdefault("DataTypes", "")
            rec.setdefault("PipelineConfidence", "Low")
            rec.setdefault("FullTextUsed", False)
            
        enriched.append(rec)

    return enriched


# -------------------------
# 7. Gold Validation
# -------------------------

@task
def validate_goldset(records: List[Dict[str, Any]], cfg: Dict[str, Any]) -> Dict[str, Any]:
    logger = get_run_logger()
    gold = set(str(x).strip() for x in cfg.get("GOLD_SET", []) if str(x).strip())
    if not gold:
        return {"goldMissing": False, "missing": []}

    seen: set = set()
    for r in records:
        pmid = str(r.get("PMID", "")).strip()
        doi = (r.get("DOI") or "").strip()
        if pmid:
            seen.add(pmid)
        if doi:
            seen.add(doi)
    missing = sorted(list(gold - seen))
    gold_missing = len(missing) > 0
    logger.info(f"Gold validation → missing={len(missing)}")
    return {"goldMissing": gold_missing, "missing": missing}


# -------------------------
# 8. Notion Utils
# -------------------------

def _notion_page_properties_from_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    title = rec.get("Title") or rec.get("PMID")
    doi = rec.get("DOI")
    pmid = rec.get("PMID")
    url = rec.get("URL")
    journal = rec.get("Journal")
    pubdate = rec.get("PubDateParsed")
    authors = rec.get("Authors")
    abstract = rec.get("Abstract")
    mesh_heading_list = rec.get("MeshHeadingList", "")
    mesh_terms = rec.get("MeSH_Terms", "")
    major_mesh = rec.get("Major_MeSH", "")

    mesh_terms_list = [t.strip().replace(",", " -") for t in mesh_terms.split(";") if t.strip()] if mesh_terms else []
    major_mesh_list = [t.strip().replace(",", " -") for t in major_mesh.split(";") if t.strip()] if major_mesh else []

    # Base properties (always included)
    props: Dict[str, Any] = {
        "Title": {"title": [{"text": {"content": title or "Untitled"}}]},
        "DOI": {"rich_text": ([{"text": {"content": _truncate(doi)}}] if doi else [])},
        "PMID": {"rich_text": ([{"text": {"content": _truncate(pmid)}}] if pmid else [])},
        "URL": {"url": url},
        "Journal": {"rich_text": ([{"text": {"content": _truncate(journal)}}] if journal else [])},
        "Abstract": {"rich_text": ([{"text": {"content": _truncate(abstract)}}] if abstract else [])},
        "Authors": {"rich_text": ([{"text": {"content": _truncate(authors)}}] if authors else [])},
        "MeshHeadingList": {"rich_text": ([{"text": {"content": _truncate(mesh_heading_list)}}] if mesh_heading_list else [])},
        "MeSH_Terms": {"multi_select": ([{"name": t} for t in mesh_terms_list] if mesh_terms_list else [])},
        "Major_MeSH": {"multi_select": ([{"name": t} for t in major_mesh_list] if major_mesh_list else [])},
        "DedupeKey": {"rich_text": [{"text": {"content": _truncate(rec.get("DedupeKey", ""))}}]},
        "LastChecked": {"date": {"start": datetime.utcnow().isoformat()}},
        "PublicationTypes": {"rich_text": ([{"text": {"content": _truncate(rec.get("PublicationTypes", ""))}}] if rec.get("PublicationTypes") else [])},
    }

    if pubdate:
        props["PubDate"] = {"date": {"start": pubdate.isoformat()}}

    # AI-generated fields - only include if present in record
    # These are only set by gemini_enrich_records, so they won't exist for records that skip enrichment
    
    if "RelevanceScore" in rec:
        props["RelevanceScore"] = {"number": rec["RelevanceScore"]}
    
    if "PipelineConfidence" in rec:
        props["PipelineConfidence"] = {"multi_select": [{"name": rec["PipelineConfidence"]}]}
    
    if "FullTextUsed" in rec:
        props["FullTextUsed"] = {"checkbox": bool(rec["FullTextUsed"])}
    
    if "StudySummary" in rec and rec["StudySummary"]:
        props["StudySummary"] = {"rich_text": [{"text": {"content": _truncate(rec["StudySummary"])}}]}
    
    if "WhyRelevant" in rec and rec["WhyRelevant"]:
        props["WhyRelevant"] = {"rich_text": [{"text": {"content": _truncate(rec["WhyRelevant"])}}]}
    
    if "Methods" in rec and rec["Methods"]:
        props["Methods"] = {"rich_text": [{"text": {"content": _truncate(rec["Methods"])}}]}
    
    if "KeyFindings" in rec and rec["KeyFindings"]:
        props["KeyFindings"] = {"rich_text": [{"text": {"content": _truncate(rec["KeyFindings"])}}]}
    
    if "DataTypes" in rec and rec["DataTypes"]:
        data_types_list = [t.strip().replace(",", " -") for t in rec["DataTypes"].replace(";", ",").split(",") if t.strip()]
        if data_types_list:
            props["DataTypes"] = {"multi_select": [{"name": dt} for dt in data_types_list]}
    
    if "GEO_List" in rec and rec["GEO_List"]:
        props["GEO_List"] = {"rich_text": [{"text": {"content": _truncate(rec["GEO_List"])}}]}
    
    if "SRA_Project" in rec and rec["SRA_Project"]:
        props["SRA_Project"] = {"rich_text": [{"text": {"content": _truncate(rec["SRA_Project"])}}]}

    # Remove empty rich_text blocks
    for k in list(props.keys()):
        v = props[k]
        if isinstance(v, dict) and "rich_text" in v and not v["rich_text"]:
            props.pop(k)

    return props


@task(retries=2, retry_delay_seconds=10)
def notion_build_index(cfg: Dict[str, Any]) -> Dict[str, str]:
    logger = get_run_logger()
    token = cfg["NOTION_TOKEN"]
    db_id = cfg["NOTION_DB_ID"]
    if not token or not db_id:
        logger.warning("Notion token or DB ID missing; index will be empty.")
        return {}

    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }

    url = "https://api.notion.com/v1/databases/{}/query".format(db_id)
    session = _make_session()
    index: Dict[str, str] = {}

    has_more = True
    payload: Dict[str, Any] = {}
    while has_more:
        resp = session.post(url, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        for page in data.get("results", []):
            props = page.get("properties", {})
            dedupe_prop = props.get("DedupeKey")
            if dedupe_prop and dedupe_prop.get("rich_text"):
                text = "".join(t["plain_text"] for t in dedupe_prop["rich_text"])
                if text:
                    index[text] = page["id"]

        has_more = data.get("has_more", False)
        payload["start_cursor"] = data.get("next_cursor")

    logger.info(f"Notion index size: {len(index)}")
    return index


@task
def classify_records(
    records: List[Dict[str, Any]],
    index: Dict[str, str],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    to_create = []
    to_update = []

    for rec in records:
        key = rec.get("DedupeKey")
        page_id = index.get(key)
        if page_id:
            rec_with_id = dict(rec)
            rec_with_id["page_id"] = page_id
            to_update.append(rec_with_id)
        else:
            to_create.append(rec)

    return to_create, to_update


@task(retries=2, retry_delay_seconds=10)
def notion_create_pages(cfg: Dict[str, Any], records: List[Dict[str, Any]]) -> Dict[str, int]:
    logger = get_run_logger()
    if cfg["DRY_RUN"]:
        logger.info(f"[DRY_RUN] Would create {len(records)} Notion pages.")
        return {"created": 0}

    token = cfg["NOTION_TOKEN"]
    db_id = cfg["NOTION_DB_ID"]
    if not token or not db_id:
        logger.warning("Notion token or DB ID missing; skipping create.")
        return {"created": 0}

    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    session = _make_session()
    url = "https://api.notion.com/v1/pages"
    created = 0

    for rec in records:
        props = _notion_page_properties_from_record(rec)
        payload = {"parent": {"database_id": db_id}, "properties": props}
        resp = session.post(url, headers=headers, json=payload, timeout=30)
        if resp.status_code >= 300:
            logger.warning(f"Create failed for PMID={rec.get('PMID')}: {resp.text}")
        else:
            created += 1
        
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "1"))
            time.sleep(retry_after)
        else:
            time.sleep(0.2)
    return {"created": created}


@task(retries=2, retry_delay_seconds=10)
def notion_update_pages(cfg: Dict[str, Any], records: List[Dict[str, Any]]) -> Dict[str, int]:
    logger = get_run_logger()
    if cfg["DRY_RUN"]:
        logger.info(f"[DRY_RUN] Would update {len(records)} Notion pages.")
        return {"updated": 0}

    token = cfg["NOTION_TOKEN"]
    if not token:
        logger.warning("Notion token missing; skipping update.")
        return {"updated": 0}

    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    session = _make_session()
    updated = 0
    for rec in records:
        page_id = rec.get("page_id")
        if not page_id:
            continue
        props = _notion_page_properties_from_record(rec)
        url = f"https://api.notion.com/v1/pages/{page_id}"
        payload = {"properties": props}
        resp = session.patch(url, headers=headers, json=payload, timeout=30)
        if resp.status_code >= 300:
            logger.warning(f"Update failed for page_id={page_id}, PMID={rec.get('PMID')}: {resp.text}")
        else:
            updated += 1
        
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "1"))
            time.sleep(retry_after)
        else:
            time.sleep(0.2)
    return {"updated": updated}


# -------------------------
# MAIN FLOW
# -------------------------

@flow(name="LiteratureSearch-Prefect")
def literature_search_flow(
    query_term: Optional[str] = None,
    rel_date_days: Optional[int] = None,
    retmax: Optional[int] = None,
    dry_run: Optional[bool] = None,
    tier: Optional[int] = 1,
):
    logger = get_run_logger()
    cfg = get_config(
        query_term=query_term,
        rel_date_days=rel_date_days,
        retmax=retmax,
        dry_run=dry_run,
        tier=tier,
    )

    # 1. Build Notion Index FIRST
    index = notion_build_index(cfg)
    
    # 2. Search
    esearch_out = pubmed_esearch(cfg)
    validation = validate_results(esearch_out, cfg)
    
    count = validation["validation"]["count"]
    if count == 0:
        logger.info("No results from PubMed; stopping flow.")
        return

    # 3. Smart Pagination Loop
    # Goal: Find `cfg["RETMAX"]` *new* papers.
    # We will try up to 3 extra times if we hit duplicates.
    
    target_new_count = cfg["RETMAX"]
    new_pmids = []
    update_pmids = []
    all_esummary_results = {} # Accumulate esummary data for normalization
    
    max_retries = 3
    current_retstart = 0
    
    # First fetch size is target
    current_fetch_size = target_new_count
    
    for attempt in range(max_retries + 1):
        logger.info(f"--- Smart Search Attempt {attempt+1}/{max_retries+1} (Start={current_retstart}, Fetch={current_fetch_size}) ---")
        
        # Fetch batch of metadata (eSummary)
        # Note: We use the history server, so we just need to advance retstart
        # We need to ensure we don't go past total count
        if current_retstart >= count:
            logger.info("Reached end of search results.")
            break
            
        # Clamp fetch size
        this_batch_size = min(current_fetch_size, count - current_retstart)
        
        # Get eSummary for this batch
        # We use the existing function but need to be careful about how it returns data
        # It returns a dict with "result": {uid: {...}, uids: [...]}
        esummary_json = pubmed_esummary_history(cfg, esearch_out, this_batch_size, start_offset=current_retstart)
        
        if not esummary_json or "result" not in esummary_json:
            logger.warning("Failed to get eSummary data.")
            break
            
        result_data = esummary_json.get("result", {})
        uids = result_data.get("uids", [])
        
        if not uids:
            logger.info("No UIDs returned in this batch.")
            break
            
        # Check against Notion Index
        batch_new = []
        batch_update = []
        
        for pmid in uids:
            # Add to accumulator
            all_esummary_results[pmid] = result_data[pmid]
            
            # Check duplication (using PMID or DOI if available in summary? Index is keyed by DedupeKey)
            # We construct a temp DedupeKey to check. 
            # Note: eSummary has 'elocationid' which might be DOI, or 'articleids'.
            # Let's try to find DOI.
            rec = result_data[pmid]
            doi = None
            for id_obj in rec.get("articleids", []):
                if id_obj.get("idtype") == "doi":
                    doi = id_obj.get("value")
                    break
            
            key = doi if doi else f"PMID:{pmid}"
            
            if key in index:
                batch_update.append({"PMID": pmid, "DedupeKey": key, "page_id": index[key]})
            else:
                batch_new.append(pmid)
        
        new_pmids.extend(batch_new)
        update_pmids.extend(batch_update)
        
        logger.info(f"Batch result: {len(batch_new)} new, {len(batch_update)} existing.")
        
        if len(new_pmids) >= target_new_count:
            logger.info(f"Found enough new papers ({len(new_pmids)} >= {target_new_count}). Stopping search.")
            break
            
        # Prepare for next iteration
        current_retstart += this_batch_size
        # For subsequent retries, fetch a fixed chunk (e.g., 30) to try to find more
        current_fetch_size = 30
        
    # Trim to target if we over-fetched
    new_pmids = new_pmids[:target_new_count]
    logger.info(f"Final Selection: {len(new_pmids)} new papers to process.")

    if not new_pmids and not update_pmids:
        logger.info("No papers found (new or existing). Stopping.")
        return

    # 4. Fetch Abstracts & Full Text for NEW papers only
    # We need to construct a map for normalization
    # For existing papers, we don't need to fetch abstracts/fulltext unless we want to re-process them.
    # Current requirement: "just update notion db lastcheck time" for existing.
    
    abstracts_map = {}
    pmc_fulltext_map = {}
    
    if new_pmids:
        logger.info(f"Fetching abstracts for {len(new_pmids)} new papers...")
        abstracts_map = pubmed_efetch_abstracts_by_ids(cfg, new_pmids)
        
        # Extract PMCIDs from the fetched abstracts map to drive full text fetch
        pmc_candidates = []
        for pmid, data in abstracts_map.items():
            if data.get("PMCID"):
                pmc_candidates.append(data["PMCID"])
        
        if pmc_candidates:
            logger.info(f"Fetching PMC full text for {len(pmc_candidates)} candidates...")
            # We need to pass a map of {PMID: PMCID} or just list of PMCIDs?
            # The existing fetch_pmc_fulltext takes a cfg and... wait, it took abstracts_map before.
            # Let's check `fetch_pmc_fulltext` signature.
            # It was: def fetch_pmc_fulltext(cfg: Dict[str, Any], abstracts_map: Dict[str, Dict[str, Optional[str]]])
            # So we can pass our new abstracts_map!
            pmc_fulltext_map = fetch_pmc_fulltext(cfg, abstracts_map)

    # 5. Normalize
    # We need to pass the accumulated esummary data, but formatted as the original JSON structure expected by normalize
    # normalize_records expects: esummary_json: Dict[str, Any] with "result" key
    # We can reconstruct it.
    combined_esummary = {"result": all_esummary_results}
    # We also need to make sure uids list is present if normalize uses it? 
    # normalize iterates over result.items(), so uids list is not strictly needed but good to have.
    combined_esummary["result"]["uids"] = list(all_esummary_results.keys())
    
    records = normalize_records(combined_esummary, abstracts_map)
    
    # 6. Separate New vs Existing (again, but now with full records)
    # We already identified them, but normalize_records might have filtered some (e.g. non-digit PMIDs).
    # Let's re-classify based on the normalized records.
    
    to_create = []
    to_update_final = []
    
    # Map our update list for quick lookup
    update_map = {u["PMID"]: u["page_id"] for u in update_pmids}
    
    for rec in records:
        pmid = rec["PMID"]
        if pmid in update_map:
            rec["page_id"] = update_map[pmid]
            to_update_final.append(rec)
        elif pmid in new_pmids:
             to_create.append(rec)
        else:
            # Should not happen if logic is correct, but maybe if esummary had extra items
            pass

    # 7. Enrich NEW papers
    num_new_candidates = 0
    if to_create:
        logger.info(f"Running Gemini enrichment on {len(to_create)} new papers...")
        enriched_new = gemini_enrich_records(to_create, abstracts_map, pmc_fulltext_map)
        num_new_candidates = len(enriched_new)
        
        # Apply minimum relevance filter before creating Notion pages
        min_relevance = 75
        high_conf = [rec for rec in enriched_new if rec.get("RelevanceScore", 0) >= min_relevance]
        low_conf = [rec for rec in enriched_new if rec.get("RelevanceScore", 0) < min_relevance]
        
        logger.info(
            f"Gemini enrichment → {len(high_conf)} records with RelevanceScore ≥ {min_relevance}, "
            f"{len(low_conf)} below threshold (kept out of Notion)."
        )
        
        create_res = notion_create_pages(cfg, high_conf)
    else:
        create_res = {"created": 0}

    # 8. Update EXISTING papers
    if to_update_final:
        logger.info(f"Updating {len(to_update_final)} existing papers (LastChecked)...")
        update_res = notion_update_pages(cfg, to_update_final)
    else:
        update_res = {"updated": 0}

    logger.info(
        f"Summary → total_found={count}, new_selected={num_new_candidates}, existing_updated={len(to_update_final)}, "
        f"created={create_res.get('created', 0)}, updated={update_res.get('updated', 0)}"
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run LiteratureSearch Prefect flow")
    parser.add_argument("--query", dest="query_term", type=str, default=None)
    parser.add_argument("--reldays", dest="rel_date_days", type=int, default=365)
    parser.add_argument("--retmax", dest="retmax", type=int, default=220)
    parser.add_argument("--dry-run", dest="dry_run", action="store_true")
    parser.add_argument("--tier", dest="tier", type=int, choices=[1, 2], default=1, help="1 = prostate-focused (default), 2 = broader cancer methods")
    args = parser.parse_args()

    literature_search_flow(
    query_term=args.query_term,
    rel_date_days=args.rel_date_days,
    retmax=args.retmax,
    dry_run=args.dry_run,
    tier=args.tier,
    )

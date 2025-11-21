"""
Prefect version of your n8n `LiteratureSearch` workflow.

High-level steps:
1. Load config (query term, time window, retmax, keys).
2. PubMed eSearch → get PMIDs.
3. Validate results (count / gold checks).
4. If no results → stop.
5. PubMed eSummary + eFetch (abstracts).
6. Normalize into unified article records with DedupeKey.
7. Pull Notion DB pages → build {DedupeKey -> page_id} index.
8. Classify each record as create vs update.
9. Create new Notion pages; update existing ones.

You will need:
- PREFECT 2.x/3.x installed
- `requests` installed
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


def _sanitize_xml_for_gemini(xml_text: str) -> str:
    """
    Remove unnecessary XML elements to reduce token costs and noise.
    Strips AuthorList and ReferenceList which aren't needed for AI analysis.
    """
    try:
        root = ET.fromstring(xml_text)
        # Remove author lists (we already have this from eSummary)
        for author_list in root.findall(".//AuthorList"):
            parent = root.find(".//*[AuthorList]")
            if parent is not None:
                parent.remove(author_list)
        # Remove reference lists (not needed for triage)
        for ref_list in root.findall(".//ReferenceList"):
            parent = root.find(".//*[ReferenceList]")
            if parent is not None:
                parent.remove(ref_list)
        return ET.tostring(root, encoding="unicode")
    except Exception:
        # If sanitization fails, return original
        return xml_text


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
) -> Dict[str, Any]:
    """
    Equivalent to the n8n Config nodes.
    """
    base_query = (
        '("Prostatic Neoplasms"[MeSH Terms] OR prostat*[tiab] OR "prostate cancer"[tiab]) '
        'AND ("spatial transcriptomics"[tw] OR "spatial multiomics"[tw] OR Visium[tw] OR \
        Xenium[tw] OR CosMX[tw] OR GeoMx[tw] OR "Slide-seq"[tw] OR scRNA[tw] OR "single-cell"[tw] \
        OR scATAC[tw] OR ATAC-seq[tw] OR multiome[tw] OR spatial spatial-ATAC OR pseudotime[tw])'
    )
    cfg = {
        "QUERY_TERM": query_term or base_query,
        "RETMAX": int(retmax) if retmax is not None else 200,
        "RELDATE_DAYS": int(rel_date_days) if rel_date_days is not None else 365,
        "NCBI_API_KEY": os.environ.get("NCBI_API_KEY", ""),
        "EMAIL": os.environ.get("NCBI_EMAIL", ""),
        "DATETYPE": os.environ.get("NCBI_DATETYPE", "pdat"),
        "HISTORICAL_MEDIAN": 500,
        "GOLD_SET": [
            "41082386",  # Example PMID
            "10.1111/cas.70220",  # Example DOI
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
def pubmed_esummary_history(cfg: Dict[str, Any], esearch_out: Dict[str, Any], total: int) -> Dict[str, Any]:
    logger = get_run_logger()
    webenv = esearch_out.get("webenv")
    query_key = esearch_out.get("query_key")
    if not webenv or not query_key or total == 0:
        logger.info("No history context; skipping esummary history.")
        return {}

    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
    session = _make_session()
    batch = int(cfg.get("EUTILS_BATCH", 200))
    merged: Dict[str, Any] = {"result": {}}

    for start in range(0, total, batch):
        params = {
            "db": "pubmed",
            "retmode": "json",
            "retstart": start,
            "retmax": batch,
            "query_key": query_key,
            "WebEnv": webenv,
            "email": cfg["EMAIL"],
            "api_key": cfg["NCBI_API_KEY"],
            "tool": cfg.get("EUTILS_TOOL", "prefect-litsearch"),
        }
        logger.info(f"ESummary batch start={start} size={batch}")
        resp = session.get(base_url, params=params, timeout=60)
        resp.raise_for_status()
        js = resp.json()
        res = js.get("result", {})
        for k, v in res.items():
            if k == "uids":
                continue
            merged.setdefault("result", {})[k] = v
        time.sleep(0.34)

    # add uids array for completeness
    uids = [k for k in merged["result"].keys() if k.isdigit()]
    merged["result"]["uids"] = uids
    return merged


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
            "retmax": batch,
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
                "GEO_List": geo_list,
                "SRA_Project": sra_project,
                "MeshHeadingList": mesh_heading_list,
                "MeSH_Terms": mesh_terms,
                "Major_MeSH": major_mesh,
                "RawXML": article_xml,
            }
        time.sleep(0.34)

    return efetch_map


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
            is_review = any(
                ("review" in pt.lower()) or ("meta-analysis" in pt.lower())
                for pt in pub_types
            )
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
                "IsReview": "Yes" if is_review else "No",
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
                    "IsReview": "No",
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
            "DataTypes": {"type": "STRING"}
        },
        "required": ["RelevanceScore", "WhyRelevant", "Methods", "DataTypes"]
    }

    SYSTEM_INSTRUCTION = """You are a PhD-level Bioinformatics curator specializing in Spatial and Single-cell Biology and Prostate Cancer.

Your task: Analyze the provided PubMed XML and extract structured insights.

CONTEXT:
The user creates a database of papers related to:
1. Prostate Cancer (PCa), TME, Lineage Plasticity, NEPC.
2. Multi-omics (scRNA-seq, scATAC-seq, Spatial Transcriptomics/Proteomics).
3. Computational methods for integrating the above.

SCORING RUBRIC (RelevanceScore):
- 90-100: DIRECT HIT. PCa + Spatial/Single-cell + Novel computational method or major biological finding.
- 70-89: RELEVANT. PCa context but standard methods, OR General Spatial/Single-cell method applicable to PCa.
- 40-69: TANGENTIAL. General cancer genomics, bulk sequencing, or clinical reviews without deep molecular focus.
- 0-39: IRRELEVANT.

INSTRUCTIONS:
1. StudySummary: Write for a computational scientist. Focus on the *data generated* and *algorithms used*. Max 3 sentences.
2. DataTypes: Be specific. Use standard terms: "10x Visium", "Xenium", "snATAC-seq", "GeoMx", "H&E". Use comma-separated lowercase.
3. KeyFindings: Extract biologically significant claims regarding tumor heterogeneity or resistance mechanisms.
4. Methods: A concise, semicolon-separated list of major algorithms and technologies (e.g., 'Seurat v5; Cell2Location; ArchR').

OUTPUT:
Return JSON only matching the provided schema. No markdown, no conversation."""

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
        "scrna-seq", "scatac-seq", "spatial transcriptomics", "10x visium",
        "xenium", "cosmx", "geomx", "slide-seq", "h&e", "wgs", "wes",
        "bulk rna-seq", "chip-seq", "atac-seq", "cite-seq", "multiome",
        "cnv", "snrna-seq", "snatac-seq", "merfish", "seqfish"
    }

    enriched: List[Dict[str, Any]] = []
    
    for rec in records:
        pmid = str(rec.get("PMID", "")).strip()
        entry = efetch_data.get(pmid, {})
        xml_text = entry.get("RawXML")
        
        if not pmid or not xml_text:
            enriched.append(rec)
            continue

        try:
            xml_text = _sanitize_xml_for_gemini(xml_text)
            prompt = f"Analyze this PubMed XML for PMID {pmid}:\n\n{xml_text}"
            resp = model.generate_content(prompt)
            data = json.loads(resp.text)

            data_types_raw = data.get("DataTypes", "")
            if data_types_raw:
                raw_types = [t.strip().lower() for t in data_types_raw.replace(";", ",").split(",") if t.strip()]
                normalized_types = []
                for dt in raw_types:
                    matched = False
                    for known in KNOWN_DATA_TYPES:
                        if known in dt or dt in known:
                            normalized_types.append(known)
                            matched = True
                            break
                    if not matched:
                        normalized_types.append(dt)
                data["DataTypes"] = ", ".join(list(dict.fromkeys(normalized_types)))

            rec.update({
                "RelevanceScore": data.get("RelevanceScore", 0),
                "WhyRelevant": data.get("WhyRelevant", ""),
                "StudySummary": data.get("StudySummary", ""),
                "Methods": data.get("Methods", ""),
                "KeyFindings": data.get("KeyFindings", ""),
                "DataTypes": data.get("DataTypes", "")
            })
            
            time.sleep(1) # Rate limiting
            
        except Exception as e:
            logger.warning(f"Gemini enrichment failed for PMID={pmid}: {e}")
            rec["WhyRelevant"] = "Error during AI enrichment"
            
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
    geo_list = rec.get("GEO_List", "")
    sra_project = rec.get("SRA_Project", "")
    mesh_heading_list = rec.get("MeshHeadingList", "")
    mesh_terms = rec.get("MeSH_Terms", "")
    major_mesh = rec.get("Major_MeSH", "")
    study_summary = rec.get("StudySummary", "")
    why_relevant = rec.get("WhyRelevant", "")
    methods = rec.get("Methods", "")
    key_findings = rec.get("KeyFindings", "")
    data_types = rec.get("DataTypes", "")
    relevance_score = rec.get("RelevanceScore", 0)

    mesh_terms_list = [t.strip().replace(",", " -") for t in mesh_terms.split(";") if t.strip()] if mesh_terms else []
    major_mesh_list = [t.strip().replace(",", " -") for t in major_mesh.split(";") if t.strip()] if major_mesh else []
    data_types_list = [t.strip().replace(",", " -") for t in data_types.replace(";", ",").split(",") if t.strip()] if data_types else []

    props: Dict[str, Any] = {
        "Title": {"title": [{"text": {"content": title or "Untitled"}}]},
        "DOI": {"rich_text": ([{"text": {"content": _truncate(doi)}}] if doi else [])},
        "PMID": {"rich_text": ([{"text": {"content": _truncate(pmid)}}] if pmid else [])},
        "URL": {"url": url},
        "Journal": {"rich_text": ([{"text": {"content": _truncate(journal)}}] if journal else [])},
        "Abstract": {"rich_text": ([{"text": {"content": _truncate(abstract)}}] if abstract else [])},
        "Authors": {"rich_text": ([{"text": {"content": _truncate(authors)}}] if authors else [])},
        "GEO_List": {"rich_text": ([{"text": {"content": _truncate(geo_list)}}] if geo_list else [])},
        "SRA_Project": {"rich_text": ([{"text": {"content": _truncate(sra_project)}}] if sra_project else [])},
        "MeshHeadingList": {"rich_text": ([{"text": {"content": _truncate(mesh_heading_list)}}] if mesh_heading_list else [])},
        "MeSH_Terms": {"multi_select": ([{"name": t} for t in mesh_terms_list] if mesh_terms_list else [])},
        "Major_MeSH": {"multi_select": ([{"name": t} for t in major_mesh_list] if major_mesh_list else [])},
        "StudySummary": {"rich_text": ([{"text": {"content": _truncate(study_summary)}}] if study_summary else [])},
        "WhyRelevant": {"rich_text": ([{"text": {"content": _truncate(why_relevant)}}] if why_relevant else [])},
        "Methods": {"rich_text": ([{"text": {"content": _truncate(methods)}}] if methods else [])},
        "KeyFindings": {"rich_text": ([{"text": {"content": _truncate(key_findings)}}] if key_findings else [])},
        "DataTypes": {"multi_select": ([{"name": dt} for dt in data_types_list] if data_types_list else [])},
        "RelevanceScore": {"number": relevance_score},
        "DedupeKey": {"rich_text": [{"text": {"content": _truncate(rec.get("DedupeKey", ""))}}]},
        "LastChecked": {"date": {"start": datetime.utcnow().isoformat()}},
        "IsReview": {"rich_text": ([{"text": {"content": _truncate(rec.get("IsReview", ""))}}] if rec.get("IsReview") else [])},
        "PublicationTypes": {"rich_text": ([{"text": {"content": _truncate(rec.get("PublicationTypes", ""))}}] if rec.get("PublicationTypes") else [])},
    }

    if pubdate:
        props["PubDate"] = {"date": {"start": pubdate.isoformat()}}

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
):
    logger = get_run_logger()
    cfg = get_config(query_term, rel_date_days, retmax, dry_run)

    esearch_out = pubmed_esearch(cfg)
    validation = validate_results(esearch_out, cfg)
    
    count = validation["validation"]["count"]
    total = min(count, cfg["RETMAX"])
    if count == 0:
        logger.info("No results from PubMed; stopping flow.")
        return

    esummary_json = pubmed_esummary_history(cfg, esearch_out, total)
    abstracts_map = pubmed_efetch_abstracts_history(cfg, esearch_out, total)
    records = normalize_records(esummary_json, abstracts_map)

    if not records:
        logger.info("No normalized records; stopping.")
        return

    gold_check = validate_goldset(records, cfg)
    if gold_check.get("goldMissing"):
        logger.warning(f"Gold set items missing: {gold_check['missing']}")

    index = notion_build_index(cfg)
    to_create, to_update = classify_records(records, index)
    
    if not to_create:
        logger.info("No new papers to create. Skipping Gemini enrichment and stopping.")
        return
    
    logger.info(f"Running Gemini enrichment on {len(to_create)} new papers...")
    to_create = gemini_enrich_records(to_create, abstracts_map)
    
    create_res = notion_create_pages(cfg, to_create)
    update_res = notion_update_pages(cfg, to_update)

    logger.info(
        f"Summary → total={count}, normalized={len(records)}, "
        f"to_create={len(to_create)}, to_update={len(to_update)}, "
        f"created={create_res.get('created', 0)}, updated={update_res.get('updated', 0)}"
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run LiteratureSearch Prefect flow")
    parser.add_argument("--query", dest="query_term", type=str, default=None)
    parser.add_argument("--reldays", dest="rel_date_days", type=int, default=None)
    parser.add_argument("--retmax", dest="retmax", type=int, default=None)
    parser.add_argument("--dry-run", dest="dry_run", action="store_true")
    args = parser.parse_args()

    literature_search_flow(
        query_term=args.query_term,
        rel_date_days=args.rel_date_days,
        retmax=args.retmax,
        dry_run=args.dry_run,
    )
### the following is the prompt for the Gemini model used in the workflow ###
'''
You are a biomedical literature-triage model.
Your job is to take raw PubMed XML and produce two things:
	1.	Relevance Score (0–100) for my research focus:
prostate cancer, tumor microenvironment (TME), lineage plasticity, multi-omics (scRNA-seq, scATAC-seq, spatial), immune suppression, CNV, therapy resistance.
	2.	Concise Summary optimized for literature screening.

You may only use information from the XML.
Do not invent details not present in the text.


OUTPUT FORMAT (JSON)

{
  "PMID": "",
  "Title": "",
  "RelevanceScore": 0,
  "WhyRelevant": "",
  "StudySummary": "",
  "Methods": "",
  "KeyFindings": "",
  "DataTypes": ""
}

Field definitions
	•	WhyRelevant: 1–2 sentences explaining why the paper is relevant (or not).
	•	StudySummary: 3–4 sentences max, written for a computational oncology researcher.
	•	Methods: bullet list of major technologies (e.g., scRNA-seq, ST, WGS, CNV calling).
	•	KeyFindings: bullet list of high-level biological insights.
	•	DataTypes: e.g., “scRNA-seq”, “10x Visium ST”, “WGS”, “bulk RNA-seq”, “CNV”, etc.


INSTRUCTIONS
	1.	Parse the raw XML.
	2.	Extract Title, PMID, Abstract, MeSH, DataBank, and any method hints.
	3.	Score relevance strictly by:
	•	✦ hallmark PCa topics (TME, immune suppression, ADT resistance)
	•	✦ multi-omics (scRNA, scATAC, spatial)
	•	✦ lineage plasticity / club-like cells / LP states
	•	✦ CNV, clonal structure
	4.	The RelevanceScore must reflect actual utility for computational biology — not general prostate literature.
	•	90–100 = directly actionable
	•	70–89  = useful background or similar methods
	•	40–69  = tangential
	•	<40     = not relevant
'''
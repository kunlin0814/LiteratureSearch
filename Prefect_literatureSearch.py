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
    NCBI_API_KEY, NCBI_EMAIL, NOTION_TOKEN, NOTION_DB_ID
"""

import os
import time
from datetime import datetime
from typing import List, Dict, Tuple, Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dateutil import parser as date_parser
from prefect import flow, task, get_run_logger
import google.generativeai as genai
# ... other imports
from dotenv import load_dotenv
load_dotenv()  # This loads the variables from .env into os.environ
# Configure Gemini
genai.configure(api_key=os.environ.get("GOOGLE_API_KEY"))

# for k, v in os.environ.items():
#     print(k, v)
# -------------------------
# 1. CONFIG (n8n: Config / Config1)
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


@task
def get_config(
    query_term: Optional[str] = None,
    rel_date_days: Optional[int] = None,
    retmax: Optional[int] = None,
    dry_run: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Equivalent to the n8n Config nodes.
    Adjust QUERY_TERM and defaults as needed.
    """
    base_query = (
        '("Prostatic Neoplasms"[MeSH Terms] OR prostat*[tiab] OR "prostate cancer"[tiab]) '
        'AND ("spatial transcriptomics"[tw] OR "spatial multiomics"[tw] OR Visium[tw] OR \
        Xenium[tw] OR CosMX[tw] OR GeoMx[tw] OR "Slide-seq"[tw] OR scRNA[tw] OR "single-cell"[tw] \
        OR scATAC[tw] OR ATAC-seq[tw] OR multiome[tw] OR CNV[tw] OR pseudotime[tw])'
    )
    cfg = {
        "QUERY_TERM": query_term or base_query,
        "RETMAX": int(retmax) if retmax is not None else 200,
        "RELDATE_DAYS": int(rel_date_days) if rel_date_days is not None else 365,
        # Always use environment variables; no hardcoded defaults
        "NCBI_API_KEY": os.environ.get("NCBI_API_KEY", ""),
        "EMAIL": os.environ.get("NCBI_EMAIL", ""),
        # pdat (publication date) or edat (Entrez date)
        "DATETYPE": os.environ.get("NCBI_DATETYPE", "pdat"),
        # simple QA thresholds (mirrors Validate Results)
        "HISTORICAL_MEDIAN": 500,
        "GOLD_SET": [
            # Known identifiers you expect (PMID or DOI)
            "39550375",  # PMID
            "10.1038/s41467-024-54364-1",  # DOI
        ],
        # Notion
        "NOTION_TOKEN": os.environ.get("NOTION_TOKEN", ""),
        "NOTION_DB_ID": os.environ.get("NOTION_DB_ID", ""),
        # behaviour
        "DRY_RUN": bool(dry_run) if dry_run is not None else False,
        # internal knobs
        "EUTILS_BATCH": 200,
        "EUTILS_TOOL": "prefect-litsearch",
    }
    return cfg


# -------------------------
# 2. PubMed eSearch (n8n: PubMed eSearch / PubMed eSearch1)
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
    return {"count": count, "ids": ids, "webenv": webenv, "query_key": query_key, "raw": data}


# -------------------------
# 3. Validate Results (n8n: Validate Results)
# -------------------------

@task
def validate_results(esearch_out: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Mirrors the logic of the n8n `Validate Results` JS code.
    Very simple: check count thresholds and gold hits.
    """
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

    logger.info(
        f"Validation → count={count}, "
        f"drop_thr={drop_threshold}, jump_thr={jump_threshold}, "
        f"gold_missing={gold_missing}, status={status}"
    )

    return {
        "validation": {
            "count": count,
            "ids": ids,
            "goldMissing": gold_missing,
            "status": status,
        }
    }


# -------------------------
# 4. PubMed eSummary + eFetch (n8n: PubMed eSummary, PubMed eFetch)
# -------------------------

@task(retries=2, retry_delay_seconds=10)
def pubmed_esummary(cfg: Dict[str, Any], ids: List[str]) -> Dict[str, Any]:
    logger = get_run_logger()
    if not ids:
        logger.info("No IDs for eSummary; skipping.")
        return {}

    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
    params = {
        "db": "pubmed",
        "retmode": "json",
        "id": ",".join(ids),
        "email": cfg["EMAIL"],
        "api_key": cfg["NCBI_API_KEY"],
    }

    logger.info(f"ESummary → {len(ids)} IDs")
    resp = _make_session().get(base_url, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


@task(retries=2, retry_delay_seconds=10)
def pubmed_efetch(cfg: Dict[str, Any], ids: List[str]) -> str:
    """
    Return raw XML as string.
    Mirrors PubMed eFetch (Abstracts) node.
    """
    logger = get_run_logger()
    if not ids:
        logger.info("No IDs for eFetch; skipping.")
        return ""

    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    params = {
        "db": "pubmed",
        "retmode": "xml",
        "id": ",".join(ids),
        "email": cfg["EMAIL"],
        "api_key": cfg["NCBI_API_KEY"],
    }

    logger.info(f"EFetch → {len(ids)} IDs")
    resp = _make_session().get(base_url, params=params, timeout=60)
    resp.raise_for_status()
    return resp.text


# Batched via E-Utilities history (recommended for large result sets)
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
                # We'll re-compute uids later if needed
                continue
            merged.setdefault("result", {})[k] = v
        time.sleep(0.34)  # be polite to NCBI

    # add uids array for completeness
    uids = [k for k in merged["result"].keys() if k.isdigit()]
    merged["result"]["uids"] = uids
    return merged


@task(retries=2, retry_delay_seconds=10)
def pubmed_efetch_abstracts_history(cfg: Dict[str, Any], esearch_out: Dict[str, Any], total: int) -> Dict[str, Dict[str, Optional[str]]]:
    """
    Return {pmid -> dict with abstract and additional fields} parsed from efetch XML using history pagination.
    Each entry contains:
        {
            "Abstract": ...,
            "GEO_List": ...,
            "SRA_Project": ...,
            "MeshHeadingList": ...,
            "MeSH_Terms": ...,
            "Major_MeSH": ...,
        }
    """
    import xml.etree.ElementTree as ET

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
            # try to wrap if needed (rare)
            xml_text_wrapped = f"<PubmedArticleSet>{xml_text}</PubmedArticleSet>"
            root = ET.fromstring(xml_text_wrapped)

        for art in root.findall(".//PubmedArticle"):
            pmid_elem = art.find(".//PMID")
            pmid = pmid_elem.text.strip() if pmid_elem is not None else None
            if not pmid:
                continue
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
                        if db_name == "GEO":
                            geo_accessions.update(accs)
                        elif db_name == "SRA":
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
                    # Build entry string
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
            }
        time.sleep(0.34)

    return efetch_map


# -------------------------
# 5. Normalize + Format Date + Parse (n8n: Normalize + Format Date, Normalize + Parse)
# -------------------------

@task
def normalize_records(
    esummary_json: Dict[str, Any],
    efetch_data: Any,
) -> List[Dict[str, Any]]:
    """
    Rough equivalent of the two code nodes:
    - Normalize + Format Date
    - Normalize + Parse

    Output: list of dicts with keys:
        Title, Journal, PubDate, DOI, PMID, URL, Abstract, DedupeKey, Authors, GEO_List, SRA_Project, MeshHeadingList, MeSH_Terms, Major_MeSH
    The Authors field is a string of author names (if available), otherwise None.
    New fields default to empty string when not present.
    """
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
            # Publication Types
            pub_types = []
            for pt in rec.get("pubtype", []):
                if pt:
                    pub_types.append(pt.strip())
            # Determine if review
            is_review = any(
                ("review" in pt.lower()) or ("meta-analysis" in pt.lower())
                for pt in pub_types
            )
            # robust date parsing
            pubdate = None
            if pubdate_raw:
                try:
                    pubdate = date_parser.parse(pubdate_raw, default=datetime(1900, 1, 1)).date()
                except Exception:
                    pubdate = None

            # Authors extraction
            authors_list = rec.get("authors", [])
            author_names = []
            for author in authors_list:
                name = author.get("name")
                if name:
                    author_names.append(name)
                else:
                    # Fallback to lastname + firstname if available
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
                "Abstract": None,  # filled from efetch
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
                # fallback: old format (abstract string)
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

    # Compute DedupeKey (n8n: DOI or PMID)
    out: List[Dict[str, Any]] = []
    for pmid, rec in records.items():
        doi = rec.get("DOI")
        dedupe_key = doi if doi else f"PMID:{pmid}"
        rec["DedupeKey"] = dedupe_key
        out.append(rec)

    logger.info(f"Normalized {len(out)} records.")
    return out


# Additional validation using normalized records for GOLD_SET (DOI or PMID)
@task
def validate_goldset(records: List[Dict[str, Any]], cfg: Dict[str, Any]) -> Dict[str, Any]:
    logger = get_run_logger()
    gold = set(str(x).strip() for x in cfg.get("GOLD_SET", []) if str(x).strip())
    if not gold:
        logger.info("No GOLD_SET provided; skipping gold validation.")
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
# 6. Notion: Get All Pages + Build Index (n8n: Notion: Get All Pages, Build Index)
# -------------------------

@task(retries=2, retry_delay_seconds=10)
def notion_build_index(cfg: Dict[str, Any]) -> Dict[str, str]:
    """
    Fetch all pages from the Notion database and build:
        { DedupeKey -> page_id }
    Mirrors:
      - Notion: Get All Pages
      - Build Index {DedupeKey→page_id}
    """
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
            # ASSUMPTION: property is named "Dedupe Key" and is rich_text
            dedupe_prop = props.get("DedupeKey")
            if dedupe_prop and dedupe_prop.get("rich_text"):
                text = "".join(t["plain_text"] for t in dedupe_prop["rich_text"])
                if text:
                    index[text] = page["id"]

        has_more = data.get("has_more", False)
        payload["start_cursor"] = data.get("next_cursor")

    logger.info(f"Notion index size: {len(index)}")
    return index


# -------------------------
# 7. Classify: create vs update (n8n: Classify: create vs update)
# -------------------------

@task
def classify_records(
    records: List[Dict[str, Any]],
    index: Dict[str, str],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Split into:
        to_create: no existing page
        to_update: existing page (with attached page_id)
    """
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


# -------------------------
# 8. Notion Create / Update (n8n: Notion: Create Page, Notion: Update Page)
# -------------------------

def _notion_page_properties_from_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map normalized record → Notion properties.
    Adjust property names to your DB schema.
    """
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

    # Prepare MeSH terms as lists for Notion multi_select
    # Notion multi_select option names cannot contain commas, so we replace ',' with '·'
    mesh_terms_list = []
    if mesh_terms:
        raw_terms = [t.strip() for t in mesh_terms.split(";") if t.strip()]
        mesh_terms_list = [term.replace(",", " -") for term in raw_terms]

    major_mesh_list = []
    if major_mesh:
        raw_major = [t.strip() for t in major_mesh.split(";") if t.strip()]
        major_mesh_list = [term.replace(",", " -") for term in raw_major]

    props = {
        "Title": {  # <- your real title property name
            "title": [
                {"text": {"content": title or "Untitled"}}
            ]
        },
        "DOI": {
            "rich_text": [
                {"text": {"content": doi}} if doi else {}
            ] if doi else [],
        },
        "PMID": {
            "rich_text": [
                {"text": {"content": pmid}} if pmid else {}
            ] if pmid else [],
        },
        "URL": {
            "url": url,
        },
        "Journal": {
            "rich_text": [
                {"text": {"content": journal}} if journal else {}
            ] if journal else [],
        },
        "Abstract": {
            "rich_text": [
                {"text": {"content": abstract}} if abstract else {}
            ] if abstract else [],
        },
        "Authors": {
            "rich_text": [
                {"text": {"content": authors}} if authors else {}
            ] if authors else [],
        },
        "GEO_List": {
            "rich_text": [
                {"text": {"content": geo_list}} if geo_list else {}
            ] if geo_list else [],
        },
        "SRA_Project": {
            "rich_text": [
                {"text": {"content": sra_project}} if sra_project else {}
            ] if sra_project else [],
        },
        "MeshHeadingList": {
            "rich_text": [
                {"text": {"content": mesh_heading_list}} if mesh_heading_list else {}
            ] if mesh_heading_list else [],
        },
        "MeSH_Terms": {
            "multi_select": [
                {"name": t} for t in mesh_terms_list
            ] if mesh_terms_list else [],
        },
        "Major_MeSH": {
            "multi_select": [
                {"name": t} for t in major_mesh_list
            ] if major_mesh_list else [],
        },
        "DedupeKey": {   # <- match your actual property name
            "rich_text": [
                {"text": {"content": rec.get("DedupeKey", "")}}
            ],
        },
        "LastChecked": {  # <- match your actual property name
            "date": {"start": datetime.utcnow().isoformat()},
        },
        "IsReview": {
            "rich_text": [
                {"text": {"content": rec.get("IsReview", "")}}
            ] if rec.get("IsReview") else [],
        },
        "PublicationTypes": {
            "rich_text": [
                {"text": {"content": rec.get("PublicationTypes", "")}}
            ] if rec.get("PublicationTypes") else [],
        },
    }

    if pubdate:
        props["PubDate"] = {
            "date": {"start": pubdate.isoformat()}
        }

    # Clean empty rich_text props
    for k, v in list(props.items()):
        if "rich_text" in v and not v["rich_text"]:
            props.pop(k)

    return props


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
        payload = {
            "parent": {"database_id": db_id},
            "properties": props,
        }
        resp = session.post(url, headers=headers, json=payload, timeout=30)
        if resp.status_code >= 300:
            logger.warning(f"Create failed for PMID={rec.get('PMID')}: {resp.text}")
        else:
            created += 1
        # gentle rate limit
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
        page_id = rec["page_id"]
        props = _notion_page_properties_from_record(rec)
        url = f"https://api.notion.com/v1/pages/{page_id}"
        payload = {"properties": props}
        resp = session.patch(url, headers=headers, json=payload, timeout=30)
        if resp.status_code >= 300:
            logger.warning(
                f"Update failed for page_id={page_id}, PMID={rec.get('PMID')}: {resp.text}"
            )
        else:
            updated += 1
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "1"))
            time.sleep(retry_after)
        else:
            time.sleep(0.2)
    return {"updated": updated}


# -------------------------
# MAIN FLOW (n8n graph orchestration)
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

    # Pull all results using history
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
    create_res = notion_create_pages(cfg, to_create)
    update_res = notion_update_pages(cfg, to_update)

    # Final summary
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
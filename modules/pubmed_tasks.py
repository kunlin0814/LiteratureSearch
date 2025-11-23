import time
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

from prefect import task, get_run_logger

from .http_utils import make_session
from .data_extraction_utils import (
    extract_geo_sra_from_pubmed_xml,
    extract_mesh_from_pubmed_xml,
)
from .pmc_utils import extract_pmc_sections


@task(retries=2, retry_delay_seconds=10)
def pubmed_esearch(cfg: Dict[str, Any]) -> Dict[str, Any]:
    logger = get_run_logger()
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    session = make_session()
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
    data = resp.json().get("esearchresult", {})
    count = int(data.get("count", 0))
    return {
        "count": count,
        "ids": data.get("idlist", []),
        "webenv": data.get("webenv"),
        "query_key": data.get("querykey"),
    }


@task(retries=2, retry_delay_seconds=10)
def pubmed_esummary_history(
    cfg: Dict[str, Any], esearch_out: Dict[str, Any], batch_size: int, start_offset: int = 0
) -> Dict[str, Any]:
    logger = get_run_logger()
    webenv = esearch_out.get("webenv")
    query_key = esearch_out.get("query_key")
    if not webenv or not query_key:
        logger.info("No history context; skipping esummary.")
        return {}
    session = make_session()
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
    resp = session.get(
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi",
        params=params,
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


@task(retries=2, retry_delay_seconds=10)
def pubmed_efetch_abstracts_by_ids(
    cfg: Dict[str, Any], pmids: List[str]
) -> Dict[str, Dict[str, Optional[str]]]:
    logger = get_run_logger()
    if not pmids:
        return {}
    session = make_session()
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    batch_size = int(cfg.get("EUTILS_BATCH", 200))
    out: Dict[str, Dict[str, Optional[str]]] = {}
    for i in range(0, len(pmids), batch_size):
        batch = pmids[i : i + batch_size]
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
            root = ET.fromstring(f"<PubmedArticleSet>{xml_text}</PubmedArticleSet>")
        for art in root.findall(".//PubmedArticle"):
            pmid_elem = art.find(".//PMID")
            pmid = pmid_elem.text.strip() if pmid_elem is not None and pmid_elem.text else None
            if not pmid:
                continue
            abst_elems = art.findall(".//AbstractText")
            abstract = " ".join((e.text or "").strip() for e in abst_elems if e.text) or None
            pmcid = None
            for aid in art.findall(".//ArticleId"):
                if aid.attrib.get("IdType") == "pmc" and aid.text:
                    pmcid = aid.text.strip()
                    break
            geo_list, sra_project = extract_geo_sra_from_pubmed_xml(art)
            mesh_heading_list, mesh_terms, major_mesh = extract_mesh_from_pubmed_xml(art)
            out[pmid] = {
                "Abstract": abstract,
                "ArticleXML": ET.tostring(art, encoding="unicode"),
                "PMCID": pmcid,
                "GEO_List": geo_list,
                "SRA_Project": sra_project,
                "MeshHeadingList": mesh_heading_list,
                "MeSH_Terms": mesh_terms,
                "Major_MeSH": major_mesh,
            }
        time.sleep(0.5)
    return out


@task(retries=2, retry_delay_seconds=10)
def pubmed_efetch_abstracts_history(
    cfg: Dict[str, Any], esearch_out: Dict[str, Any], total: int
) -> Dict[str, Dict[str, Optional[str]]]:
    logger = get_run_logger()
    webenv = esearch_out.get("webenv")
    query_key = esearch_out.get("query_key")
    if not webenv or not query_key or total == 0:
        logger.info("No history context; skipping efetch history.")
        return {}
    session = make_session()
    batch = int(cfg.get("EUTILS_BATCH", 200))
    out: Dict[str, Dict[str, Optional[str]]] = {}
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
        resp = session.get(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi",
            params=params,
            timeout=60,
        )
        resp.raise_for_status()
        xml_text = resp.text
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            root = ET.fromstring(f"<PubmedArticleSet>{xml_text}</PubmedArticleSet>")
        for art in root.findall(".//PubmedArticle"):
            pmid_elem = art.find(".//PMID")
            pmid = pmid_elem.text.strip() if pmid_elem is not None and pmid_elem.text else None
            if not pmid:
                continue
            abst_elems = art.findall(".//AbstractText")
            abstract = " ".join((e.text or "").strip() for e in abst_elems if e.text) or None
            pmcid = None
            article_id_list = art.find(".//ArticleIdList")
            if article_id_list is not None:
                for aid in article_id_list.findall("ArticleId"):
                    if aid.attrib.get("IdType") == "pmc" and aid.text:
                        pmcid = aid.text.strip()
                        break
            mesh_heading_list, mesh_terms, major_mesh = extract_mesh_from_pubmed_xml(art)
            geo_list, sra_project = extract_geo_sra_from_pubmed_xml(art)
            out[pmid] = {
                "Abstract": abstract,
                "PMCID": pmcid,
                "GEO_List": geo_list,
                "SRA_Project": sra_project,
                "MeshHeadingList": mesh_heading_list,
                "MeSH_Terms": mesh_terms,
                "Major_MeSH": major_mesh,
                "RawXML": ET.tostring(art, encoding="unicode"),
            }
        time.sleep(0.34)
    return out


@task(retries=2, retry_delay_seconds=10)
def fetch_pmc_fulltext(
    cfg: Dict[str, Any], efetch_map: Dict[str, Dict[str, Any]]
) -> Dict[str, Dict[str, Any]]:
    logger = get_run_logger()
    pmcid_to_pmid = {}
    for pmid, data in efetch_map.items():
        pmcid = data.get("PMCID")
        if pmcid:
            pmcid_to_pmid[pmcid] = pmid
    if not pmcid_to_pmid:
        logger.info("No PMCIDs found in this batch.")
        return {}
    session = make_session()
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    pmcids = list(pmcid_to_pmid.keys())
    batch_size = 50
    results: Dict[str, Dict[str, Any]] = {}
    for i in range(0, len(pmcids), batch_size):
        batch = pmcids[i : i + batch_size]
        params = {
            "db": "pmc",
            "retmode": "xml",
            "id": ",".join([b.replace("PMC", "") for b in batch]),
            "email": cfg["EMAIL"],
            "api_key": cfg["NCBI_API_KEY"],
        }
        try:
            resp = session.get(base_url, params=params, timeout=120)
            resp.raise_for_status()
            root = ET.fromstring(resp.text)
            for article in root.findall(".//article"):
                pmcid = None
                for aid in article.findall(".//article-id"):
                    if aid.attrib.get("pub-id-type") in ("pmc", "pmcid") and aid.text:
                        pmcid = aid.text.strip()
                        if not pmcid.startswith("PMC"):
                            pmcid = "PMC" + pmcid
                        break
                if not pmcid or pmcid not in pmcid_to_pmid:
                    continue
                pmid = pmcid_to_pmid[pmcid]
                xml_str = ET.tostring(article, encoding="unicode")
                extracted = extract_pmc_sections(xml_str)
                results[pmid] = {"full_text": extracted, "pmcid": pmcid, "used": True}
            time.sleep(0.5)
        except Exception as e:
            logger.error(f"Error fetching PMC batch {i}: {e}")
    logger.info(f"Successfully extracted full text for {len(results)} articles.")
    return results

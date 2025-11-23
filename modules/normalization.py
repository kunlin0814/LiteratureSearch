from typing import Any, Dict, List
from datetime import datetime
from dateutil import parser as date_parser
from prefect import task, get_run_logger


@task
def normalize_records(esummary_json: Dict[str, Any], efetch_data: Any) -> List[Dict[str, Any]]:
    logger = get_run_logger()
    records: Dict[str, Dict[str, Any]] = {}

    # eSummary data
    if esummary_json:
        result = esummary_json.get("result", {})
        for pmid, rec in result.items():
            if not str(pmid).isdigit():
                continue
            title = rec.get("title")
            journal = rec.get("fulljournalname") or rec.get("source")
            pubdate_raw = rec.get("pubdate") or rec.get("sortpubdate")
            doi = None
            for id_obj in rec.get("articleids", []):
                if id_obj.get("idtype") == "doi":
                    doi = id_obj.get("value")
                    break
            pub_types = [pt.strip() for pt in rec.get("pubtype", []) if pt]
            pubdate = None
            if pubdate_raw:
                try:
                    pubdate = date_parser.parse(
                        pubdate_raw, default=datetime(1900, 1, 1)
                    ).date()
                except (ValueError, OverflowError, TypeError):
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
            records[str(pmid)] = {
                "PMID": str(pmid),
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

    # eFetch (abstracts + extras)
    if isinstance(efetch_data, dict) and efetch_data:
        for pmid, entry in efetch_data.items():
            pmid_str = str(pmid)
            if isinstance(entry, dict):
                abstract = entry.get("Abstract")
                geo_list = entry.get("GEO_List", "")
                sra_project = entry.get("SRA_Project", "")
                mesh_heading_list = entry.get("MeshHeadingList", "")
                mesh_terms = entry.get("MeSH_Terms", "")
                major_mesh = entry.get("Major_MeSH", "")
            else:
                abstract = entry
                geo_list = mesh_heading_list = mesh_terms = major_mesh = sra_project = ""
            if pmid_str not in records:
                records[pmid_str] = {
                    "PMID": pmid_str,
                    "Title": None,
                    "Journal": None,
                    "PubDate": None,
                    "PubDateParsed": None,
                    "DOI": None,
                    "URL": f"https://pubmed.ncbi.nlm.nih.gov/{pmid_str}/",
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
                r = records[pmid_str]
                r["Abstract"] = abstract
                r["GEO_List"] = geo_list
                r["SRA_Project"] = sra_project
                r["MeshHeadingList"] = mesh_heading_list
                r["MeSH_Terms"] = mesh_terms
                r["Major_MeSH"] = major_mesh

    out: List[Dict[str, Any]] = []
    for pmid, rec in records.items():
        dedupe_key = rec.get("DOI") or f"PMID:{pmid}"
        rec["DedupeKey"] = dedupe_key
        out.append(rec)
    logger.info("Normalized %s records.", len(out))
    return out

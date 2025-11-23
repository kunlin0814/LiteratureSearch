"""
Notion API utilities for literature search pipeline.

This module handles conversion of internal record dictionaries to Notion API
property format, including conditional field inclusion for AI-generated content.
"""

from datetime import datetime
from typing import Dict, Any, Optional


def truncate_for_notion(text: Optional[str], limit: int = 2000) -> str:
    """
    Safely truncate text to avoid Notion API 400 errors.
    
    Args:
        text: Text to truncate
        limit: Maximum character limit (default 2000)
        
    Returns:
        Truncated text or empty string
    """
    if not text:
        return ""
    return text[:limit] if len(text) > limit else text


def build_notion_page_properties(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert internal record to Notion API page properties format.
    
    Conditionally includes AI-generated fields only when present in the record.
    This prevents overwriting existing Notion data with defaults during updates.
    
    Args:
        record: Internal record dictionary with paper metadata and AI analysis
        
    Returns:
        Dictionary formatted for Notion API page creation/update
    """
    title = record.get("Title") or record.get("PMID")
    doi = record.get("DOI")
    pmid = record.get("PMID")
    url = record.get("URL")
    journal = record.get("Journal")
    pubdate = record.get("PubDateParsed")
    authors = record.get("Authors")
    abstract = record.get("Abstract")
    mesh_heading_list = record.get("MeshHeadingList", "")
    mesh_terms = record.get("MeSH_Terms", "")
    major_mesh = record.get("Major_MeSH", "")

    mesh_terms_list = [t.strip().replace(",", " -") for t in mesh_terms.split(";") if t.strip()] if mesh_terms else []
    major_mesh_list = [t.strip().replace(",", " -") for t in major_mesh.split(";") if t.strip()] if major_mesh else []

    # Base properties (always included)
    props: Dict[str, Any] = {
        "Title": {"title": [{"text": {"content": title or "Untitled"}}]},
        "DOI": {"rich_text": ([{"text": {"content": truncate_for_notion(doi)}}] if doi else [])},
        "PMID": {"rich_text": ([{"text": {"content": truncate_for_notion(pmid)}}] if pmid else [])},
        "URL": {"url": url},
        "Journal": {"rich_text": ([{"text": {"content": truncate_for_notion(journal)}}] if journal else [])},
        "Abstract": {"rich_text": ([{"text": {"content": truncate_for_notion(abstract)}}] if abstract else [])},
        "Authors": {"rich_text": ([{"text": {"content": truncate_for_notion(authors)}}] if authors else [])},
        "MeshHeadingList": {"rich_text": ([{"text": {"content": truncate_for_notion(mesh_heading_list)}}] if mesh_heading_list else [])},
        "MeSH_Terms": {"multi_select": ([{"name": t} for t in mesh_terms_list] if mesh_terms_list else [])},
        "Major_MeSH": {"multi_select": ([{"name": t} for t in major_mesh_list] if major_mesh_list else [])},
        "DedupeKey": {"rich_text": [{"text": {"content": truncate_for_notion(record.get("DedupeKey", ""))}}]},
        "LastChecked": {"date": {"start": datetime.utcnow().isoformat()}},
        "PublicationTypes": {"rich_text": ([{"text": {"content": truncate_for_notion(record.get("PublicationTypes", ""))}}] if record.get("PublicationTypes") else [])},
    }

    if pubdate:
        props["PubDate"] = {"date": {"start": pubdate.isoformat()}}

    # AI-generated fields - only include if present in record
    # These are only set by gemini_enrich_records, so they won't exist for records that skip enrichment
    
    if "RelevanceScore" in record:
        props["RelevanceScore"] = {"number": record["RelevanceScore"]}
    
    if "PipelineConfidence" in record:
        props["PipelineConfidence"] = {"multi_select": [{"name": record["PipelineConfidence"]}]}
    
    if "FullTextUsed" in record:
        props["FullTextUsed"] = {"checkbox": bool(record["FullTextUsed"])}
    
    if "StudySummary" in record and record["StudySummary"]:
        props["StudySummary"] = {"rich_text": [{"text": {"content": truncate_for_notion(record["StudySummary"])}}]}
    
    if "WhyRelevant" in record and record["WhyRelevant"]:
        props["WhyRelevant"] = {"rich_text": [{"text": {"content": truncate_for_notion(record["WhyRelevant"])}}]}
    
    if "Methods" in record and record["Methods"]:
        props["Methods"] = {"rich_text": [{"text": {"content": truncate_for_notion(record["Methods"])}}]}
    
    if "KeyFindings" in record and record["KeyFindings"]:
        props["KeyFindings"] = {"rich_text": [{"text": {"content": truncate_for_notion(record["KeyFindings"])}}]}
    
    if "DataTypes" in record and record["DataTypes"]:
        data_types_list = [t.strip().replace(",", " -") for t in record["DataTypes"].replace(";", ",").split(",") if t.strip()]
        if data_types_list:
            props["DataTypes"] = {"multi_select": [{"name": dt} for dt in data_types_list]}
    
    if "GEO_List" in record and record["GEO_List"]:
        props["GEO_List"] = {"rich_text": [{"text": {"content": truncate_for_notion(record["GEO_List"])}}]}
    
    if "SRA_Project" in record and record["SRA_Project"]:
        props["SRA_Project"] = {"rich_text": [{"text": {"content": truncate_for_notion(record["SRA_Project"])}}]}

    # Remove empty rich_text blocks
    for k in list(props.keys()):
        v = props[k]
        if isinstance(v, dict) and "rich_text" in v and not v["rich_text"]:
            props.pop(k)

    return props

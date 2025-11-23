import os
from typing import Any, Dict, Optional
from dotenv import load_dotenv

load_dotenv()

def get_config(
    query_term: Optional[str] = None,
    rel_date_days: Optional[int] = None,
    retmax: Optional[int] = None,
    dry_run: Optional[bool] = None,
    tier: Optional[int] = 1,
) -> Dict[str, Any]:
    """Resolve runtime configuration (tiered or explicit query + env vars)."""
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

    resolved_query = query_term or (tier2_query if tier == 2 else tier1_query)

    return {
        "QUERY_TERM": resolved_query,
        "RETMAX": int(retmax) if retmax is not None else 200,
        "RELDATE_DAYS": int(rel_date_days) if rel_date_days is not None else 365,
        "NCBI_API_KEY": os.environ.get("NCBI_API_KEY", ""),
        "EMAIL": os.environ.get("NCBI_EMAIL", ""),
        "DATETYPE": os.environ.get("NCBI_DATETYPE", "pdat"),
        "HISTORICAL_MEDIAN": 500,
        "GOLD_SET": ["36750562", "10.1038/s41467-023-36325-2"],
        "NOTION_TOKEN": os.environ.get("NOTION_TOKEN", ""),
        "NOTION_DB_ID": os.environ.get("NOTION_DB_ID", ""),
        "DRY_RUN": bool(dry_run) if dry_run is not None else False,
        "EUTILS_BATCH": 200,
        "EUTILS_TOOL": "prefect-litsearch",
    }

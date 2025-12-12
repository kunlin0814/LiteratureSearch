"""Prefect LiteratureSearch flow orchestrator (modular version).

This file now only coordinates tasks imported from modules:
- config.get_config
- pubmed_tasks.*
- validation_tasks.*
- normalization.normalize_records
- enrichment.gemini_enrich_records
- notion_tasks.*
"""

from typing import Optional
from prefect import flow, get_run_logger

from modules.config import get_config
from modules.pubmed_tasks import (
    pubmed_esearch,
    pubmed_esummary_history,
    pubmed_efetch_abstracts_by_ids,
    fetch_pmc_fulltext,
)
from modules.validation_tasks import validate_results
from modules.normalization import normalize_records
from modules.enrichment import gemini_enrich_records
from modules.notion_tasks import (
    notion_build_index,
    notion_create_pages,
    notion_update_pages,
)


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
    
    # 3. Smart Pagination Loop
    # Goal: Find `cfg["RETMAX"]` *new* papers.
    
    target_new_count = cfg["RETMAX"]
    new_pmids = []
    update_pmids = []
    all_esummary_results = {} # Accumulate esummary data for normalization
    
    current_retstart = 0
    current_fetch_size = target_new_count
    max_pages = 30
    page_count = 0
    
    while len(new_pmids) < target_new_count and current_retstart < count and page_count < max_pages:
        page_count += 1
        logger.info(f"--- Smart Search Page {page_count} (Start={current_retstart}, Fetch={current_fetch_size}) ---")
        
        # Fetch batch of metadata (eSummary)
        # Note: We use the history server, so we just need to advance retstart
        # We need to ensure we don't go past total count
        if current_retstart >= count:
            logger.info("Reached end of search results.")
            break
            
        # Clamp fetch size
        this_batch_size = min(current_fetch_size, count - current_retstart)
        
        # Get eSummary for this batch
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
        
        logger.info(f"Batch result: {len(batch_new)} new, {len(batch_update)} existing. Total new so far: {len(new_pmids)}")
        
        current_retstart += this_batch_size
        # Increase fetch size for subsequent pages to scan faster
        current_fetch_size = 50
        
    # Trim to target if we over-fetched
    new_pmids = new_pmids[:target_new_count]
    logger.info(f"Final Selection: {len(new_pmids)} new papers to process.")

    if not new_pmids and not update_pmids:
        logger.info("No papers found (new or existing). Stopping.")
        return

    # 4. Fetch Abstracts & Full Text for NEW papers only
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
            pmc_fulltext_map = fetch_pmc_fulltext(cfg, abstracts_map)

    # 5. Normalize
    combined_esummary = {"result": all_esummary_results}
    combined_esummary["result"]["uids"] = list(all_esummary_results.keys())
    
    records = normalize_records(combined_esummary, abstracts_map)
    
    # 6. Separate New vs Existing (again, but now with full records)
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
            pass

    # 7. Enrich NEW papers
    num_new_candidates = 0
    if to_create:
        logger.info(f"Running Gemini enrichment on {len(to_create)} new papers...")
        from google.api_core.exceptions import ResourceExhausted
        
        try:
            enriched_new = gemini_enrich_records(to_create, abstracts_map, pmc_fulltext_map)
            num_new_candidates = len(enriched_new)
            
            # CHANGED: Create ALL pages, even low relevance, to prevent infinite loop of re-processing.
            logger.info(f"Gemini enrichment → {len(enriched_new)} records processed. Creating all in Notion.")
            create_res = notion_create_pages(cfg, enriched_new)
            
        except ResourceExhausted:
            logger.critical("Gemini quota exceeded during enrichment. Stopping flow immediately to avoid saving partial/error data.")
            return
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
    parser.add_argument("--retmax", dest="retmax", type=int, default=100)
    parser.add_argument("--dry-run", dest="dry_run", action="store_true")
    parser.add_argument(
        "--tier",
        dest="tier",
        type=int,
        choices=[1, 2],
        default=1,
        help="1 = prostate-focused (default), 2 = broader cancer methods",
    )
    args = parser.parse_args()
    literature_search_flow(
        query_term=args.query_term,
        rel_date_days=args.rel_date_days,
        retmax=args.retmax,
        dry_run=args.dry_run,
        tier=args.tier,
    )

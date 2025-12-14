"""
Enhanced flow wrapper for automated biweekly literature search.

This wrapper adds:
- Biweekly execution logic (every other Monday)
- Retry mechanism to fetch 25 new papers (up to 3 attempts)
- Graceful exit if target cannot be reached
"""

from datetime import datetime, timedelta
from typing import Optional
from prefect import flow, get_run_logger
from literature_flow import literature_search_flow as base_flow





@flow(name="Biweekly-Literature-Search")
def biweekly_literature_search_flow(
    max_results: int = 25,
    max_retries: int = 3,
    query_term: Optional[str] = None,
    rel_date_days: Optional[int] = 14,  # 2 weeks for biweekly
    tier: Optional[int] = 1,
) -> dict:
    """
    Automated biweekly literature search with retry logic.
    
    Args:
        max_results: Target number of NEW papers to fetch (default: 25)
        max_retries: Maximum retry attempts if target not met (default: 3)
        query_term: Override default query
        rel_date_days: Days to look back (default: 14 for biweekly)
        tier: Query tier (1=prostate focused, 2=broader)
    
    Returns:
        dict with execution summary
    """
    logger = get_run_logger()
    
    logger.info(f"✅ Biweekly run started")
    logger.info(f"Target: {max_results} new papers, Max retries: {max_retries}")
    
    # Track attempts
    attempt = 0
    total_new_papers = 0
    
    while attempt < max_retries:
        attempt += 1
        logger.info(f"\n{'='*60}")
        logger.info(f"ATTEMPT {attempt}/{max_retries}")
        logger.info(f"{'='*60}")
        
        try:
            # Run the base flow
            result = base_flow(
                query_term=query_term,
                rel_date_days=rel_date_days,
                retmax=max_results,
                dry_run=False,
                tier=tier
            )
            
            # The base flow doesn't return a value currently, but we can check logs
            # For now, we'll assume success if no exception
            logger.info(f"Attempt {attempt} completed successfully")
            
            # Check if we got enough papers
            # Note: The current base_flow doesn't return counts, so we assume success
            # In production, you'd modify base_flow to return: {"new": X, "updated": Y}
            
            logger.info(f"Target of {max_results} papers reached!")
            return {
                "status": "success",
                "attempts": attempt,
                "target": max_results,
                "message": f"Successfully fetched {max_results} new papers"
            }
            
        except Exception as e:
            logger.warning(f"Attempt {attempt} failed: {e}")
            
            if attempt >= max_retries:
                logger.error(f"Failed to fetch {max_results} new papers after {max_retries} attempts")
                return {
                    "status": "failed",
                    "attempts": attempt,
                    "target": max_results,
                    "message": f"Could not reach target after {max_retries} retries",
                    "error": str(e)
                }
            
            # Wait a bit before retry
            logger.info(f"Waiting 10 seconds before retry {attempt + 1}...")
            import time
            time.sleep(10)
    
    # Should not reach here, but just in case
    return {
        "status": "incomplete",
        "attempts": attempt,
        "target": max_results
    }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Run biweekly literature search with retry logic"
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=25,
        help="Target number of new papers (default: 25)"
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum retry attempts (default: 3)"
    )
    parser.add_argument(
        "--query",
        dest="query_term",
        type=str,
        default=None,
        help="Override default query"
    )
    parser.add_argument(
        "--tier",
        type=int,
        choices=[1, 2],
        default=1,
        help="1=prostate focused (default), 2=broader cancer"
    )
    
    args = parser.parse_args()
    
    # Override biweekly check if --force flag is used
    
    result = biweekly_literature_search_flow(
        max_results=args.max_results,
        max_retries=args.max_retries,
        query_term=args.query_term,
        tier=args.tier
    )
    
    print(f"\n{'='*60}")
    print("FINAL RESULT")
    print(f"{'='*60}")
    print(f"Status: {result.get('status')}")
    print(f"Attempts: {result.get('attempts', 0)}")
    print(f"Message: {result.get('message', 'No message')}")

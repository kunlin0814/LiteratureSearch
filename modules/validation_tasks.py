from typing import Any, Dict, List
from prefect import task, get_run_logger


@task
def validate_results(esearch_out: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
    logger = get_run_logger()
    count = esearch_out.get("count", 0)
    ids = esearch_out.get("ids", [])
    historical_median = cfg["HISTORICAL_MEDIAN"]
    drop_threshold = historical_median * 0.5
    jump_threshold = historical_median * 2
    gold_set = set(cfg.get("GOLD_SET", []))
    found_gold = any(i in gold_set for i in ids)
    gold_missing = bool(gold_set) and not found_gold
    status = "OK"
    if count == 0 or count < drop_threshold or count > jump_threshold or gold_missing:
        status = "ALERT"
    logger.info(
        "Validation → count=%s, gold_missing=%s, status=%s",
        count,
        gold_missing,
        status,
    )
    return {"validation": {"count": count, "ids": ids, "goldMissing": gold_missing, "status": status}}


@task
def validate_goldset(records: List[Dict[str, Any]], cfg: Dict[str, Any]) -> Dict[str, Any]:
    logger = get_run_logger()
    gold = set(str(x).strip() for x in cfg.get("GOLD_SET", []) if str(x).strip())
    if not gold:
        return {"goldMissing": False, "missing": []}
    seen = set()
    for r in records:
        pmid = str(r.get("PMID", "")).strip()
        doi = (r.get("DOI") or "").strip()
        if pmid:
            seen.add(pmid)
        if doi:
            seen.add(doi)
    missing = sorted(list(gold - seen))
    gold_missing = len(missing) > 0
    logger.info("Gold validation → missing=%s", len(missing))
    return {"goldMissing": gold_missing, "missing": missing}

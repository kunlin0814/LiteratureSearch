import time
from typing import Any, Dict, List, Tuple

from prefect import task, get_run_logger

from .http_utils import make_session
from .notion_utils import build_notion_page_properties


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
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    session = make_session()
    index: Dict[str, str] = {}
    payload: Dict[str, Any] = {}
    has_more = True
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
    logger.info("Notion index size: %s", len(index))
    return index


@task
def classify_records(
    records: List[Dict[str, Any]], index: Dict[str, str]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    to_create: List[Dict[str, Any]] = []
    to_update: List[Dict[str, Any]] = []
    for rec in records:
        key = rec.get("DedupeKey")
        page_id = index.get(key)
        if page_id:
            r = dict(rec)
            r["page_id"] = page_id
            to_update.append(r)
        else:
            to_create.append(rec)
    return to_create, to_update


@task(retries=2, retry_delay_seconds=10)
def notion_create_pages(cfg: Dict[str, Any], records: List[Dict[str, Any]]) -> Dict[str, int]:
    logger = get_run_logger()
    if cfg["DRY_RUN"]:
        logger.info("[DRY_RUN] Would create %s Notion pages.", len(records))
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
    session = make_session()
    url = "https://api.notion.com/v1/pages"
    created = 0
    for rec in records:
        props = build_notion_page_properties(rec)
        payload = {"parent": {"database_id": db_id}, "properties": props}
        resp = session.post(url, headers=headers, json=payload, timeout=30)
        if resp.status_code >= 300:
            logger.warning(
                "Create failed for PMID=%s: %s", rec.get("PMID"), resp.text[:200]
            )
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
        logger.info("[DRY_RUN] Would update %s Notion pages.", len(records))
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
    session = make_session()
    updated = 0
    for rec in records:
        page_id = rec.get("page_id")
        if not page_id:
            continue
        props = build_notion_page_properties(rec)
        url = f"https://api.notion.com/v1/pages/{page_id}"
        payload = {"properties": props}
        resp = session.patch(url, headers=headers, json=payload, timeout=30)
        if resp.status_code >= 300:
            logger.warning(
                "Update failed for page_id=%s PMID=%s: %s",
                page_id,
                rec.get("PMID"),
                resp.text[:200],
            )
        else:
            updated += 1
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "1"))
            time.sleep(retry_after)
        else:
            time.sleep(0.2)
    return {"updated": updated}

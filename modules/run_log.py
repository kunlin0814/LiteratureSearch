"""
Utility for appending structured run metadata to a CSV log.

This keeps a lightweight audit trail of which queries were executed and
how many records were created/updated in Notion.
"""

import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, Any


def append_run_log(cfg: Dict[str, Any], stats: Dict[str, Any]) -> None:
    """Append a single run entry to the configured log file."""
    log_path = Path(cfg.get("RUN_LOG_PATH", "run_history.csv")).expanduser()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "timestamp_utc",
        "tier",
        "query_term",
        "rel_date_days",
        "retmax",
        "dry_run",
        "ai_provider",
        "total_found",
        "new_selected",
        "existing_updates",
        "created",
        "updated",
        "notes",
    ]

    row = {
        "timestamp_utc": datetime.utcnow().isoformat(),
        "tier": stats.get("tier"),
        "query_term": cfg.get("QUERY_TERM", "").strip().replace("\n", " "),
        "rel_date_days": cfg.get("RELDATE_DAYS"),
        "retmax": cfg.get("RETMAX"),
        "dry_run": cfg.get("DRY_RUN"),
        "ai_provider": cfg.get("AI_PROVIDER"),
        "total_found": stats.get("total_found"),
        "new_selected": stats.get("new_selected"),
        "existing_updates": stats.get("existing_updates"),
        "created": stats.get("created"),
        "updated": stats.get("updated"),
        "notes": stats.get("notes", ""),
    }

    write_header = not log_path.exists()

    with log_path.open("a", newline="", encoding="utf-8") as logfile:
        writer = csv.DictWriter(logfile, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow(row)

import os
import time
import json
from typing import Any, Dict, List

import google.generativeai as genai
from prefect import task, get_run_logger

genai.configure(api_key=os.environ.get("GOOGLE_API_KEY"))


@task(retries=3, retry_delay_seconds=5)
def gemini_enrich_records(
    records: List[Dict[str, Any]],
    efetch_data: Dict[str, Dict[str, Any]],
    pmc_fulltext_map: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    logger = get_run_logger()
    if not os.environ.get("GOOGLE_API_KEY"):
        logger.warning("GOOGLE_API_KEY not set; skipping Gemini enrichment.")
        return records

    response_schema = {
        "type": "OBJECT",
        "properties": {
            "RelevanceScore": {"type": "INTEGER"},
            "WhyRelevant": {"type": "STRING"},
            "StudySummary": {"type": "STRING"},
            "Methods": {"type": "STRING"},
            "KeyFindings": {"type": "STRING"},
            "DataTypes": {"type": "STRING"},
        },
        "required": [
            "RelevanceScore",
            "WhyRelevant",
            "StudySummary",
            "Methods",
            "KeyFindings",
            "DataTypes",
        ],
    }

    SYSTEM_INSTRUCTION = (
        "You are a PhD-level bioinformatics curator specializing in cancer biology, spatial and single-cell technologies, and computational methods. "
        "You will be given the text of a scientific paper (Abstract, Methods, Results, and Data/Code Availability). Extract structured fields."
    )

    try:
        model = genai.GenerativeModel(
            model_name="gemini-2.5-flash",
            system_instruction=SYSTEM_INSTRUCTION,
            generation_config={
                "temperature": 0.1,
                "response_mime_type": "application/json",
                "response_schema": response_schema,
            },
        )
    except Exception as e:  # noqa: BLE001
        logger.error("Model init failed: %s", e)
        return records

    KNOWN_DATA_TYPES = {
        "scrna-seq",
        "scatac-seq",
        "scdna",
        "spatial transcriptomics",
        "10x visium",
        "xenium",
        "cosmx",
        "geomx",
        "slide-seq",
        "h&e",
        "wgs",
        "wes",
        "bulk rna-seq",
        "chip-seq",
        "atac-seq",
        "cite-seq",
        "multiome",
        "cnv",
        "snrna-seq",
        "snatac-seq",
        "merfish",
        "seqfish",
    }

    enriched: List[Dict[str, Any]] = []
    for rec in records:
        pmid = str(rec.get("PMID", "")).strip()
        full_text_entry = pmc_fulltext_map.get(pmid)
        full_text_used = False
        if full_text_entry and full_text_entry.get("full_text"):
            text_to_analyze = (
                "Analysis based on Full Text (Abstract + Methods + Results + Data/Code Availability):\n\n"
                + full_text_entry["full_text"]
            )
            full_text_used = True
        else:
            abstract_text = efetch_data.get(pmid, {}).get("Abstract", "")
            if not abstract_text:
                text_to_analyze = "Title: " + str(rec.get("Title", "")) + "\n\n(No Abstract Available)"
            else:
                text_to_analyze = f"Analysis based on Abstract:\n\n{abstract_text}"
        try:
            resp = model.generate_content(f"Analyze this text for PMID {pmid}:\n\n{text_to_analyze}")
            parsed = json.loads(resp.text or "{}")
        except Exception as e:  # noqa: BLE001
            logger.warning("Gemini enrichment failed for PMID=%s: %s", pmid, e)
            rec.setdefault("RelevanceScore", 0)
            rec.setdefault("WhyRelevant", f"Error during AI enrichment: {e}")
            rec.setdefault("StudySummary", "")
            rec.setdefault("Methods", "")
            rec.setdefault("KeyFindings", "")
            rec.setdefault("DataTypes", "")
            rec.setdefault("PipelineConfidence", "Low")
            rec.setdefault("FullTextUsed", False)
            enriched.append(rec)
            continue

        parsed.setdefault("RelevanceScore", 0)
        parsed.setdefault("WhyRelevant", "Analysis failed or returned empty.")
        parsed.setdefault("StudySummary", "")
        parsed.setdefault("Methods", "")
        parsed.setdefault("KeyFindings", "")
        parsed.setdefault("DataTypes", "")

        raw_types = [
            t.strip().lower()
            for t in parsed.get("DataTypes", "").replace(";", ",").split(",")
            if t.strip()
        ]
        normalized_types: List[str] = []
        for dt in raw_types:
            matched = False
            for known in KNOWN_DATA_TYPES:
                if known in dt:
                    normalized_types.append(known)
                    matched = True
                    break
            if not matched:
                normalized_types.append(dt)
        parsed["DataTypes"] = ", ".join(dict.fromkeys(normalized_types))

        relevance_score = parsed.get("RelevanceScore", 0)
        why_relevant = parsed.get("WhyRelevant", "")
        if relevance_score == 0 and (
            "relevant" in why_relevant.lower() and "no abstract" not in why_relevant.lower()
        ):
            relevance_score = 50
            parsed["RelevanceScore"] = 50

        methods_str = parsed.get("Methods", "").lower()
        key_findings_str = parsed.get("KeyFindings", "").lower()
        confidence = "Low"
        if full_text_used:
            confidence = "High"
        else:
            if relevance_score >= 80:
                confidence = "Medium"
            else:
                strong_keywords = [
                    "spatial",
                    "visium",
                    "xenium",
                    "cosmx",
                    "scrna",
                    "snrna",
                    "multiome",
                ]
                if any(k in methods_str for k in strong_keywords) or any(
                    k in key_findings_str for k in strong_keywords
                ):
                    confidence = "Medium"

        rec.update(
            {
                "RelevanceScore": relevance_score,
                "WhyRelevant": parsed.get("WhyRelevant", ""),
                "StudySummary": parsed.get("StudySummary", ""),
                "Methods": parsed.get("Methods", ""),
                "KeyFindings": parsed.get("KeyFindings", ""),
                "DataTypes": parsed.get("DataTypes", ""),
                "PipelineConfidence": confidence,
                "FullTextUsed": full_text_used,
            }
        )
        enriched.append(rec)
        time.sleep(1)

    return enriched

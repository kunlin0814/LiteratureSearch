import os
import time
import json
import re
from typing import Any, Dict, List

import google.generativeai as genai
from prefect import task, get_run_logger

genai.configure(api_key=os.environ.get("GOOGLE_API_KEY"))


def _extract_json_text(resp: Any) -> str:
    """Pull best-effort JSON text out of a Gemini response."""
    raw = (getattr(resp, "text", None) or "").strip()
    if not raw and getattr(resp, "candidates", None):
        # Fall back to concatenating candidate part texts.
        for cand in resp.candidates:
            parts = getattr(cand, "content", None)
            if not parts or not getattr(parts, "parts", None):
                continue
            texts = [p.text for p in parts.parts if getattr(p, "text", None)]
            if texts:
                raw = "".join(texts).strip()
                break
    # Strip Markdown fences if present
    if raw.startswith("```json"):
        raw = raw[len("```json"):].strip()
    elif raw.startswith("```"):
        raw = raw[len("```"):].strip()
    if raw.endswith("```"):
        raw = raw[:-len("```")].strip()
    return raw


def _load_response_json(raw: str) -> Dict[str, Any]:
    """Attempt to parse JSON even if Gemini wraps it in prose/code fences.

    If we have to repair obviously truncated JSON (unterminated string / braces),
    we add a special marker key ``"__TRUNCATED__"`` so callers can downgrade
    confidence or tag the record.
    """
    if not raw:
        return {}
    
    # Try direct parsing first
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    # Try to salvage the first complete JSON object via regex (best-effort)
    try:
        match = re.search(r"\{.*?\}", raw, re.DOTALL)
        if match:
            return json.loads(match.group(0))
    except json.JSONDecodeError:
        pass

    # If that fails, try to find and extract just the JSON structure
    # Look for balanced braces
    brace_count = 0
    start_idx = raw.find('{')
    if start_idx != -1:
        for i in range(start_idx, len(raw)):
            if raw[i] == '{':
                brace_count += 1
            elif raw[i] == '}':
                brace_count -= 1
                if brace_count == 0:
                    try:
                        return json.loads(raw[start_idx:i+1])
                    except json.JSONDecodeError:
                        pass
                    break
    
    # Attempt to repair truncated JSON
    # This is a best-effort heuristic for common truncation patterns
    try:
        # If it looks like a truncated string inside a JSON
        # e.g. {"key": "valu
        # We can try to close it.
        repaired = raw.strip()
        # If it doesn't end with '}', try to close the last open string and object
        if not repaired.endswith('}'):
            # Count quotes to see if we are inside a string
            # This is a naive check, doesn't handle escaped quotes perfectly but might suffice
            quote_count = repaired.count('"')
            if quote_count % 2 == 1:
                repaired += '"'
            
            # Count braces to see how many to close
            open_braces = repaired.count('{')
            close_braces = repaired.count('}')
            repaired += '}' * (open_braces - close_braces)

            obj = json.loads(repaired)
            # Mark that we had to repair a truncation so callers can react.
            if isinstance(obj, dict):
                obj["__TRUNCATED__"] = True
            return obj
    except (json.JSONDecodeError, Exception):
        pass

    # Last resort: return empty dict and log the error
    # We re-raise to let the caller handle logging/fallback if they want, 
    # or we can just raise ValueError as before.
    # The original code raised ValueError with the original error.
    # We will try to provide a helpful message.
    raise ValueError(f"Could not parse JSON from response. Raw (first 500 chars): {raw[:500]}")


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
    "You are a PhD-level bioinformatics curator specializing in cancer biology, "
    "prostate cancer, spatial transcriptomics, single-cell genomics, and multi-omics methods. "
    "Given paper text, return ONLY a JSON object matching the provided schema.\n\n"
    "RelevanceScore rules:\n"
    "- 0 = Not relevant (neither cancer nor spatial/single-cell/multi-omics).\n"
    "- 30–60 = Weak: generic cancer OR generic omics method.\n"
    "- 70–84 = Cancer-focused but limited spatial/single-cell/multi-omics.\n"
    "- 85–94 = Prostate cancer + at least one key technology (scRNA/snrna, scATAC/snatac, multiome, Visium/Xenium/CosMx/GeoMx).\n"
    "- 95–100 = Prostate cancer + both single-cell/multiome AND spatial technology.\n"
    "- For non-prostate cancers, assign ≥75 only if ≥3 relevant technologies are clearly used.\n\n"
    "WhyRelevant: 1 sentence explaining the score.\n"
    "StudySummary: 2–3 sentences (aim, system/cohort, main result).\n"
    "Methods: Experimental platforms + computational tools if stated.\n"
    "KeyFindings: Concise bullet-like points in a single string separated by ';'.\n"
    "DataTypes: Comma-separated assays; use controlled vocabulary when possible; empty string if not reported.\n\n"
    "Missing info → empty string. No fabrication. Output compact JSON only."
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
        "multi-omics",
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
            user_prompt = (
            f"You will be given text associated with a scientific paper for PMID {pmid}.\n"
            "Carefully read it and then fill the JSON fields exactly as specified in your system instructions.\n"
            "Return ONLY the JSON object and nothing else.\n\n"
            "TEXT_START\n"
            f"{text_to_analyze}\n"
            "TEXT_END"
            )
            resp = model.generate_content(user_prompt)
            raw_json = _extract_json_text(resp)
            parsed = _load_response_json(raw_json)
        except Exception as e:  # noqa: BLE001
            logger.warning("Gemini enrichment failed for PMID=%s: %s", pmid, e)
            try:
                if 'raw_json' in locals() and raw_json:
                    logger.debug("Gemini raw response (truncated) for PMID=%s: %s", pmid, raw_json[:500])
            except Exception:  # noqa: BLE001
                pass
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

        # If truncated-repair was needed, mark the title so it is easy to spot.
        if parsed.pop("__TRUNCATED__", False):
            title = str(rec.get("Title", ""))
            if not title.startswith("trct-title:"):
                rec["Title"] = f"trct-title: {title}" if title else "trct-title:"

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
                    "multi-omics"
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

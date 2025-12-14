import os
import time
import json
import re
from typing import Any, Dict, List
from google.api_core.exceptions import ResourceExhausted

import google.generativeai as genai
from prefect import task, get_run_logger

# Module-level API client initialization (reused across all calls)
genai.configure(api_key=os.environ.get("GOOGLE_API_KEY"))

# Initialize OpenAI client if available (lazy import to avoid hard dependency)
try:
    from openai import OpenAI
    _OPENAI_CLIENT = OpenAI(api_key=os.environ.get("OPENAI_API_KEY")) if os.environ.get("OPENAI_API_KEY") else None
except ImportError:
    _OPENAI_CLIENT = None


# Common prompt template used by both providers
SYSTEM_INSTRUCTION = (
    "You are a PhD-level bioinformatics curator specializing in cancer biology, "
    "prostate cancer, spatial transcriptomics, single-cell genomics, and multi-omics methods. "
    "Given paper text, return ONLY a JSON object matching the provided schema.\\n\\n"
    "RelevanceScore rules:\\n"
    "- 0 = Not relevant (neither cancer nor spatial/single-cell/multi-omics).\\n"
    "- 30–60 = Weak: generic cancer OR generic omics method.\\n"
    "- 70–84 = Cancer-focused but limited spatial/single-cell/multi-omics.\\n"
    "- 85–94 = Prostate cancer + at least one key technology (scRNA/snrna, scATAC/snatac, multiome, Visium/Xenium/CosMx/GeoMx).\\n"
    "- 95–100 = Prostate cancer + both single-cell/multiome AND spatial technology.\\n"
    "- For non-prostate cancers, assign ≥75 only if ≥3 relevant technologies are clearly used.\\n\\n"
    "WhyRelevant: 1 sentence explaining the score.\\n"
    "StudySummary: 2–3 sentences (aim, system/cohort, main result).\\n"
    "Methods: Experimental platforms + computational tools if stated.\\n"
    "KeyFindings: Concise bullet-like points in a single string separated by ';'.\\n"
    "DataTypes: Comma-separated assays; use controlled vocabulary when possible; empty string if not reported.\\n"
    "Group: The 'Principal Investigator' or 'Lab Name'. Logic:\\n"
    "  1. If full text is provided, look for the 'Corresponding Author' or 'Correspondence to' section.\\n"
    "  2. If valid corresponding author found, use their Name (e.g. 'John Doe') or Lab Name (e.g. 'Doe Lab').\\n"
    "  3. If NO full text or NO corresponding author found, strictly use the LAST author from the provided Author list.\\n\\n"
    "Missing info → empty string. No fabrication. Output compact JSON only."
)


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


def _call_gemini_api(user_prompt: str, logger) -> Dict[str, Any]:
    """Call Gemini API and return parsed JSON response."""
    response_schema = {
        "type": "OBJECT",
        "properties": {
            "RelevanceScore": {"type": "INTEGER"},
            "WhyRelevant": {"type": "STRING"},
            "StudySummary": {"type": "STRING"},
            "Methods": {"type": "STRING"},
            "KeyFindings": {"type": "STRING"},
            "DataTypes": {"type": "STRING"},
            "Group": {"type": "STRING"},
        },
        "required": [
            "RelevanceScore",
            "WhyRelevant",
            "StudySummary",
            "Methods",
            "KeyFindings",
            "DataTypes",
            "Group",
        ],
    }
    
    model = genai.GenerativeModel(
        model_name="gemini-2.5-flash",
        system_instruction=SYSTEM_INSTRUCTION,
        generation_config={
            "temperature": 0.1,
            "response_mime_type": "application/json",
            "response_schema": response_schema,
        },
    )
    
    resp = model.generate_content(user_prompt)
    raw_json = _extract_json_text(resp)
    
    # Estimate output tokens
    estimated_output_tokens = len(raw_json) // 4
    
    return _load_response_json(raw_json), estimated_output_tokens


def _call_openai_api(user_prompt: str, logger, model_name: str = "gpt-5-nano") -> Dict[str, Any]:
    """Call OpenAI Chat Completions API with strict schema enforcement."""
    # Use module-level client (reused across all calls)
    if _OPENAI_CLIENT is None:
        raise ValueError("OpenAI client not initialized. Check OPENAI_API_KEY environment variable.")
    
    # Define strict JSON schema matching Gemini's response_schema
    json_schema = {
        "name": "paper_enrichment_response",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "RelevanceScore": {
                    "type": "integer",
                    "description": "Relevance score from 0-100"
                },
                "WhyRelevant": {
                    "type": "string",
                    "description": "1 sentence explaining the score"
                },
                "StudySummary": {
                    "type": "string",
                    "description": "2-3 sentences about aim, system/cohort, main result"
                },
                "Methods": {
                    "type": "string",
                    "description": "Experimental platforms and computational tools"
                },
                "KeyFindings": {
                    "type": "string",
                    "description": "Concise bullet-like points separated by ;"
                },
                "DataTypes": {
                    "type": "string",
                    "description": "Comma-separated assays"
                },
                "Group": {
                    "type": "string",
                    "description": "Principal Investigator or Lab Name"
                }
            },
            "required": [
                "RelevanceScore",
                "WhyRelevant",
                "StudySummary",
                "Methods",
                "KeyFindings",
                "DataTypes",
                "Group"
            ],
            "additionalProperties": False
        }
    }
    
    # Build API parameters
    params: Dict[str, Any] = {
        "model": model_name,
        "messages": [
            {"role": "system", "content": SYSTEM_INSTRUCTION},
            {"role": "user", "content": user_prompt}
        ],
        "response_format": {
            "type": "json_schema",
            "json_schema": json_schema
        }
    }
    
    # Only non-GPT-5 models support custom temperature
    if not model_name.startswith("gpt-5"):
        params["temperature"] = 0.1
    
    response = _OPENAI_CLIENT.chat.completions.create(**params)
    
    raw_json = response.choices[0].message.content
    output_tokens = response.usage.completion_tokens
    
    return json.loads(raw_json), output_tokens


@task(retries=2, retry_delay_seconds=30)
def ai_enrich_records(
    records: List[Dict[str, Any]],
    efetch_data: Dict[str, Dict[str, Any]],
    pmc_fulltext_map: Dict[str, Dict[str, Any]],
    cfg: Dict[str, Any] = None,
) -> List[Dict[str, Any]]:
    logger = get_run_logger()
    
    # Get provider from config (default to gemini for backward compatibility)
    if cfg is None:
        from modules.config import get_config
        cfg = get_config()
    
    provider = cfg.get("AI_PROVIDER", "gemini").lower()
    
    # Check API keys
    if provider == "gemini":
        if not os.environ.get("GOOGLE_API_KEY"):
            logger.warning("GOOGLE_API_KEY not set; skipping enrichment.")
            return records
        logger.info("Using Gemini API for enrichment")
    elif provider == "openai":
        if not os.environ.get("OPENAI_API_KEY"):
            logger.warning("OPENAI_API_KEY not set; skipping enrichment.")
            return records
        
        # Configuration for Nano-First Strategy
        DEFAULT_MODEL = "gpt-5-nano"
        ESCALATION_MODEL = "gpt-5-mini"
        logger.info(f"Using OpenAI API (Default: {DEFAULT_MODEL} -> Escalate: {ESCALATION_MODEL})")
    else:
        logger.error(f"Unknown AI_PROVIDER: {provider}. Use 'gemini' or 'openai'.")
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
    
    # Token tracking for TPM monitoring
    total_input_tokens = 0
    total_output_tokens = 0
    start_time = time.time()
    
    for rec in records:
        pmid = str(rec.get("PMID", "")).strip()
        full_text_entry = pmc_fulltext_map.get(pmid)
        full_text_used = False
        if full_text_entry and full_text_entry.get("full_text"):
            text_to_analyze = (
                "Analysis based on Full Text (Abstract + Methods + Results + Data/Code Availability):\\n\\n"
                + full_text_entry["full_text"]
            )
            full_text_used = True
        else:
            abstract_text = efetch_data.get(pmid, {}).get("Abstract", "")
            if not abstract_text:
                text_to_analyze = "Title: " + str(rec.get("Title", "")) + "\\n\\n(No Abstract Available)"
            else:
                text_to_analyze = f"Analysis based on Abstract:\\n\\n{abstract_text}"

        # Add Authors to the prompt context
        authors_str = rec.get("Authors", "") or "No authors listed"
        
        user_prompt = (
        f"You will be given text associated with a scientific paper for PMID {pmid}.\\n"
        "Carefully read it and then fill the JSON fields exactly as specified in your system instructions.\\n"
        f"The authors listed for this paper are: {authors_str}\\n"
        "Carefully read the text and then fill the JSON fields exactly as specified in your system instructions.\\n"
        "Return ONLY the JSON object and nothing else.\\n\\n"
        "TEXT_START\\n"
        f"{text_to_analyze}\\n"
        "TEXT_END"
        )
        
        # Estimate token usage (rough approximation: 1 token ≈ 4 characters)
        estimated_input_tokens = len(user_prompt) // 4
        total_input_tokens += estimated_input_tokens
        elapsed_minutes = (time.time() - start_time) / 60.0 or 0.01
        tokens_per_minute = total_input_tokens / elapsed_minutes
        
        logger.info(
            f"PMID {pmid}: ~{estimated_input_tokens:,} input tokens | "
            f"Cumulative: {total_input_tokens:,} tokens in {elapsed_minutes:.2f} min "
            f"(~{tokens_per_minute:,.0f} TPM)"
        )
        
        try:
            # Route to the appropriate provider with Escalation Logic
            if provider == "gemini":
                parsed, output_tokens = _call_gemini_api(user_prompt, logger)
            else:  # openai
                # Try 1: Default Model (Nano)
                try:
                    parsed, output_tokens = _call_openai_api(user_prompt, logger, model_name=DEFAULT_MODEL)
                    
                    # Escalation Check
                    rel_score = parsed.get("RelevanceScore", 0)
                    needs_escalation = False
                    
                    # Trigger 1: Ambiguous Score (70-80)
                    if 70 <= rel_score <= 80:
                        needs_escalation = True
                        logger.warning(f"PMID {pmid}: Ambiguous score ({rel_score}) with {DEFAULT_MODEL}. Escalating...")
                        
                    # Trigger 2: Parsing Failure or Empty (Handled by exception/defaults usually, but check parsed dict)
                    if not parsed or parsed.get("WhyRelevant") == "Analysis failed or returned empty.":
                        needs_escalation = True
                        
                    if needs_escalation:
                         # Try 2: Escalation Model (Mini)
                         logger.info(f"Escalating PMID {pmid} to {ESCALATION_MODEL} for better reasoning...")
                         parsed, output_tokens = _call_openai_api(user_prompt, logger, model_name=ESCALATION_MODEL)
                         
                except Exception as e:
                    # If Nano fails completely (e.g. JSON error), try Escalation Model
                    logger.warning(f"PMID {pmid}: Failed with {DEFAULT_MODEL} ({e}). Escalating to {ESCALATION_MODEL}...")
                    parsed, output_tokens = _call_openai_api(user_prompt, logger, model_name=ESCALATION_MODEL)

            total_output_tokens += output_tokens
            
        except ResourceExhausted as e:
            # Extract quota error details if available
            error_details = str(e)
            logger.error(
                f"{provider.upper()} QUOTA EXCEEDED for PMID={pmid}\n"
                f"Error Details: {error_details}\n"
                f"Current Usage: {total_input_tokens:,} input tokens + {total_output_tokens:,} output tokens\n"
                f"Time Elapsed: {elapsed_minutes:.2f} minutes ({tokens_per_minute:,.0f} tokens/min)\n"
                f"Likely Cause: {'TPM limit (1M tokens/min)' if tokens_per_minute > 900000 else 'Daily quota or other limit'}\n"
                f"STOPPING pipeline to prevent Notion database corruption."
            )
            raise  # Re-raise to stop the flow
            
        # NOTE: We deliberately removed the generic 'except Exception' block here.
        # We WANT the pipeline to crash if there's a JSON parsing error or API error,
        # so that we don't write bad/empty data to Notion.

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
        parsed.setdefault("Group", "")

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
        
        # Check if escalaction improved things or if we still have low confidence
        if provider == "openai" and relevance_score <= 85 and relevance_score >= 70:
             confidence = "Medium-Ambiguous" # Mark as ambiguous but potentially handled by escalation

        rec.update(
            {
                "RelevanceScore": relevance_score,
                "WhyRelevant": parsed.get("WhyRelevant", ""),
                "StudySummary": parsed.get("StudySummary", ""),
                "Methods": parsed.get("Methods", ""),
                "KeyFindings": parsed.get("KeyFindings", ""),
                "DataTypes": parsed.get("DataTypes", ""),
                "Group": parsed.get("Group", ""),
                "PipelineConfidence": confidence,
                "FullTextUsed": full_text_used,
            }
        )
        enriched.append(rec)
        time.sleep(0.3)

    return enriched

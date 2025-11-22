# Literature Search & Triage Pipeline

Automated PubMed → Gemini → Notion workflow for prostate spatial-omics literature.

![Python](https://img.shields.io/badge/python-3.9%2B-blue) ![Prefect](https://img.shields.io/badge/prefect-3.x-orange) ![License](https://img.shields.io/badge/license-MIT-green)

---

## Overview

This pipeline helps computational oncology researchers keep a structured, deduplicated Notion database of recent prostate cancer studies using spatial transcriptomics, single‑cell, ATAC, and multiomic technologies.

It:
1. Queries PubMed with a focused prostate + spatial / single‑cell / multiome search.
2. Fetches abstracts, MeSH terms, GEO/SRA accessions, publication metadata.
3. Enriches new papers only using Gemini (native JSON schema) to extract relevance score & structured summaries.
4. Normalizes and writes to Notion (safe truncation, multi‑select sanitation, dedupe by DOI/PMID).
5. Performs gold‑set validation for drift detection.

Extended, more detailed documentation lives in `DETAILS.md`.

---

## Key Features (Condensed)
- Native JSON AI enrichment (no post‑hoc parsing failures).
- Deterministic low‑temperature relevance scoring (0–100 rubric).
- Cost control: enrich only new records; XML sanitization reduces tokens.
- Safe Notion sync: 2000‑char truncation, retry on 429, DOI/PMID dedupe.
- Controlled vocabulary for `DataTypes` to prevent tag drift.
- Gold‑set presence check (early warning of query drift).

---
## Before You Run
```bash
# Ensure .env support
pip install python-dotenv
```

This pipeline relies on `python-dotenv` to load environment variables from the `.env` file. Make sure it is installed.
```
Create a `.env` file in the project root with the following variables:
```
NCBI_API_KEY=your_ncbi_api_key
NCBI_EMAIL=your.email@institution.edu
GOOGLE_API_KEY=your_gemini_api_key
NOTION_TOKEN=secret_notion_token
NOTION_DB_ID=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```
---
## Quick Start

```bash
git clone https://github.com/yourusername/API_WF.git
cd API_WF
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env  # (Optional helper; create if not present)
```

Run:
```bash
python Prefect_literatureSearch.py
```

Custom query / window:
```bash
python Prefect_literatureSearch.py \
  --query '("Prostatic Neoplasms"[MeSH] OR prostat*[tiab]) AND ("spatial transcriptomics"[tw] OR Visium[tw])' \
  --reldays 180 \
  --retmax 100
```

Dry run (no Notion writes):
```bash
python Prefect_literatureSearch.py --dry-run
```

---

## CLI Flags
| Flag | Purpose | Default |
|------|---------|---------|
| `--query` | Override built-in PubMed query | internal prostate spatial query |
| `--reldays` | Look-back window (days) | 1095 |
| `--retmax` | Max results to process | 200 |
| `--dry-run` | Skip Notion create/update writes | False |

---

## Workflow
```
PubMed eSearch → Validate → eSummary/eFetch → Normalize
  ↓
Build Notion Index → Classify (new vs existing)
  ↓
Gemini Enrichment (new only) → Create Pages
Existing pages → Update Pages
```

---

## AI Enrichment Summary
Fields: `RelevanceScore`, `WhyRelevant`, `StudySummary`, `Methods`, `KeyFindings`, `DataTypes`.

Rubric (compact):
- 90–100 Direct prostate + advanced spatial/single‑cell or pivotal method.
- 70–89 Strongly relevant (transferable spatial/single‑cell or useful prostate biology).
- 40–69 Tangential / mainly methodological.
- 0–39 Irrelevant to cancer molecular/spatial context.

Temperature = 0.1 for consistency; schema enforced to avoid key drift.

---

## Minimal Notion Schema
Required properties (case‑sensitive) for core functionality:
- `Title` (Title)
- `DedupeKey` (Text)
- `PMID`, `DOI`, `URL`, `Journal` (Text/URL)
- `Abstract` (Text)
- `StudySummary`, `WhyRelevant`, `Methods`, `KeyFindings` (Text)
- `DataTypes` (Multi-select)
- `RelevanceScore` (Number)
- `LastChecked` (Date)

Optional but supported: `Authors`, `GEO_List`, `SRA_Project`, `MeSH_Terms`, `Major_MeSH`, `IsReview`, `PublicationTypes`, `MeshHeadingList`.

Safeguards: all rich_text trimmed to 2000 chars; commas in multi‑select tokens replaced with ` -` to satisfy Notion validation.

---

## Safeguards & Cost Controls
| Aspect | Mechanism |
|--------|-----------|
| API retries | Requests session with backoff (429/5xx) |
| Token usage | XML sanitization (remove AuthorList / ReferenceList) |
| Duplicate pages | `DedupeKey` (DOI else `PMID:{id}`) |
| Notion limits | Truncation + respect `Retry-After` on 429 |
| Gemini spend | Enrich only new papers |
| Query drift | Gold‑set validation |

Typical academic run remains within free tiers (NCBI, Gemini, Notion).

---

## Roadmap (High-Level)
- Optional broader cancer “methods” secondary query tier.
- Crossref / preprint ingestion.
- MeSH topic clustering & trend visualization.
- Citation network / influence scoring.
- Prefect deployment examples (Cloud + Blocks).

---

## Contributing
PRs welcome for: new DataTypes vocabulary items, enrichment logic refinements, validation heuristics, performance improvements. Please open an issue before large architectural changes.

---

## License
MIT License. See `LICENSE`.

---

## Attribution
If this pipeline speeds up your research, a star or mention is appreciated. Extended technical details, rationale, and design notes: see `DETAILS.md`.

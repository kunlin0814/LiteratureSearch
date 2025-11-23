# Literature Search & Triage Pipeline

Automated PubMed → Gemini → Notion workflow for cancer spatial/single-cell literature triage.

![Python](https://img.shields.io/badge/python-3.9%2B-blue) ![Prefect](https://img.shields.io/badge/prefect-3.x-orange) ![License](https://img.shields.io/badge/license-MIT-green)

---

## Overview

Production-ready pipeline for computational oncology researchers to maintain a structured Notion database of spatial transcriptomics and single‑cell studies, with AI-powered relevance scoring and metadata enrichment.

**What it does:**
1. **Two-tier PubMed queries** – Tier 1 (prostate-focused) or Tier 2 (pan-cancer methods discovery).
2. **Rich metadata extraction** – Abstracts, MeSH terms, GEO/SRA accessions, PMC full-text (when available).
3. **Gemini AI enrichment** – Native JSON mode extracts `RelevanceScore`, `StudySummary`, `Methods`, `KeyFindings`, `DataTypes`.
4. **Cost-optimized** – Only enriches new papers; XML sanitization reduces tokens.
5. **Notion sync** – Safe truncation, multi-select normalization, DOI/PMID deduplication.
6. **Quality checks** – Gold-set validation detects query drift.

**Modular architecture:** Utility modules (`pmc_utils`, `data_extraction_utils`, `notion_utils`) separate concerns for maintainability.

Extended technical documentation: `DETAILS.md`

---

## Key Features

**Discovery Strategy**
- **Tier 1** (default): Prostate cancer + spatial/single-cell/multiome – high precision.
- **Tier 2**: Pan-cancer spatial/multiome studies – captures transferable methods & tools.
- Custom query override supported via `--query`.

**AI Enrichment**
- Gemini 2.5 Flash with enforced JSON schema (no parsing failures).
- Temperature 0.1 for deterministic scoring.
- Extracts: `RelevanceScore` (0–100), `WhyRelevant`, `StudySummary`, `Methods`, `KeyFindings`, `DataTypes`.
- Controlled vocabulary prevents tag drift.

**Data Extraction**
- GEO/SRA accessions from DataBankList + ReferenceList (regex fallback).
- MeSH terms with major/minor distinction and qualifiers.
- PMC full-text when PMCID available (Abstract + Methods + Results + Data Availability).

**Notion Integration**
- 2000-char truncation safeguard (no API 400 errors).
- Conditional field inclusion (only writes AI fields when present).
- Multi-select comma sanitization (` -` replacement).
- DOI/PMID deduplication keys.

**Cost & Safety**
- Only enriches new records (skip existing Notion entries).
- XML sanitization (strips AuthorList/ReferenceList, ~40% token savings).
- Retry with exponential backoff (429/5xx handling).
- Gold-set validation for query drift detection.

---
## Before You Run

Create a `.env` file in the project root (requires `python-dotenv` - already in `requirements.txt`):
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
```

Create `.env` as shown above, then run:
```bash
# Tier 1 (prostate-focused, default)
python Prefect_literatureSearch.py

# Tier 2 (broader cancer methods)
python Prefect_literatureSearch.py --tier 2

# Custom query
python Prefect_literatureSearch.py \
  --query '("Prostatic Neoplasms"[MeSH]) AND ("spatial transcriptomics"[tw])' \
  --reldays 180 \
  --retmax 100

# Dry run (no Notion writes)
python Prefect_literatureSearch.py --dry-run --tier 1
```

---

## CLI Flags
| Flag | Purpose | Default |
|------|---------|---------|
| `--tier` | Query tier: 1=prostate-focused, 2=pan-cancer methods | 1 |
| `--query` | Override built-in tier query | None (uses tier) |
| `--reldays` | Look-back window (days) | 365 |
| `--retmax` | Max results to process | 220 |
| `--dry-run` | Skip Notion create/update writes | False |

---

## Workflow
```
PubMed eSearch (tier query or custom) → Validate (count + gold-set)
  ↓
eSummary/eFetch (metadata + abstracts + MeSH + GEO/SRA)
  ↓
PMC Full-Text (when PMCID available) → Extract sections
  ↓
Normalize Records (DedupeKey = DOI or PMID:{id})
  ↓
Build Notion Index → Classify (new vs existing)
  ↓
Gemini Enrichment (new records only) → Structured JSON extraction
  ↓
Create/Update Notion Pages → Done
```

**Key Tasks:**
- `pubmed_esearch`: History-mode retrieval (WebEnv/QueryKey).
- `pubmed_efetch_abstracts_history`: Batch XML fetch with utility function extraction.
- `fetch_pmc_fulltext`: Opportunistic full-text retrieval for PMCID-tagged articles.
- `gemini_enrich_records`: Schema-enforced AI analysis (low temp, native JSON).
- `notion_create_pages` / `notion_update_pages`: Safe property mapping with truncation.

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

**Required properties** (case-sensitive):

| Property | Type | Purpose |
|----------|------|---------|
| `Title` | Title | Paper title (auto) |
| `DedupeKey` | Text | DOI or `PMID:{id}` |
| `PMID` | Text | PubMed ID |
| `DOI` | Text | Digital Object Identifier |
| `URL` | URL | PubMed link |
| `Journal` | Text | Full journal name |
| `PubDate` | Date | Publication date |
| `Abstract` | Text | Truncated to 2000 chars |
| `StudySummary` | Text | AI-generated (3 sentences) |
| `WhyRelevant` | Text | AI justification |
| `Methods` | Text | Semicolon-separated tools |
| `KeyFindings` | Text | Biological insights |
| `DataTypes` | Multi-select | Normalized data types |
| `RelevanceScore` | Number | 0-100 AI score |
| `LastChecked` | Date | Sync timestamp |

**Optional but supported:**
`Authors`, `GEO_List`, `SRA_Project`, `MeSH_Terms`, `Major_MeSH`, `MeshHeadingList`, `PublicationTypes`, `PipelineConfidence`, `FullTextUsed`.

**Safeguards:**
- All rich_text fields truncated to 2000 chars.
- Multi-select commas replaced with ` -` (Notion validation compliance).
- Conditional AI field inclusion (only writes when enrichment occurred).

---

## Safeguards & Cost Controls

| Aspect | Implementation |
|--------|----------------|
| **API Retries** | Requests session: exponential backoff for 429/5xx/connection errors |
| **Token Usage** | XML sanitization (strips AuthorList/ReferenceList, ~40% savings) |
| **Duplicate Pages** | DOI-first deduplication; fallback to `PMID:{id}` |
| **Notion Limits** | 2000-char truncation; respects `Retry-After` on 429 |
| **Gemini Spend** | Only enriches new papers; skips existing Notion entries |
| **Query Drift** | Gold-set validation (alerts if landmark papers missing) |
| **PMC Full-Text** | Opportunistic (only when PMCID present); extracts key sections only |

**Typical run cost:** $0 (all services free-tier: NCBI 10 req/sec, Gemini 1500 RPD, Notion unlimited reads).

---

## Roadmap
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

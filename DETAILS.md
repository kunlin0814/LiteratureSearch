# Literature Search & Triage Pipeline – Technical Details

**Automated PubMed → Gemini → Notion pipeline for prostate cancer spatial biology and cancer single-cell / spatial-omics research.**

![Python](https://img.shields.io/badge/python-3.9%2B-blue)
![Prefect](https://img.shields.io/badge/prefect-3.x-orange)
![License](https://img.shields.io/badge/license-MIT-green)

This document describes the internals of the pipeline.  
For a short “how to use it” overview, see `README.md`.

---

## 🎯 High-Level Overview

This pipeline automates discovery, enrichment, and organization of literature for:

- **Prostate cancer (PCa) spatial biology and multi-omics**, and  
- **Cross-cancer single-cell / spatial methods** that are transferable to PCa.

It:

1. **Searches PubMed** using tiered queries targeting spatial transcriptomics, single-cell, and PCa/cancer biology.  
2. **Enriches metadata** (GEO/SRA accessions, MeSH, abstracts) via NCBI E-Utilities.  
3. Uses **Google Gemini 2.5 Flash (native JSON mode)** to score relevance (0–100) and extract structured summaries.  
4. **Syncs to Notion** with deduplication and cost-aware AI enrichment.

---

## 🔭 Search Strategy – Tiered Queries

The pipeline supports a **two-tier search strategy** controlled by CLI:

- `--tier 1` → **Tier 1 (High Precision)**  
- `--tier 2` → **Tier 2 (Broader Methods)**  

You can still override the query entirely with `--query`, in which case `--tier` is ignored.

### Tier 1 – Prostate-Focused, High Precision

Tier 1 is designed to pull:

- Prostate cancer / prostatic neoplasms  
- Spatial transcriptomics platforms (Visium, Xenium, CosMX, GeoMx, MERFISH, Slide-seq, etc.)  
- Single-cell / single-nucleus (scRNA-seq, snRNA-seq, scATAC-seq, snATAC-seq)  
- Multiome / joint RNA+ATAC  
- Pseudotime, trajectory, RNA velocity, lineage / plasticity analyses  

This tier is used for **direct PCa spatial/single-cell literature scanning**.

### Tier 2 – Broader Cancer Methods / Technology Proxy

Tier 2 broadens the cancer context to:

- “Neoplasms” MeSH + general cancer terms (cancer, carcinoma, tumor, malignancy)  
- Same advanced technologies and methods (spatial, single-cell, multiome, trajectory, etc.)  

This tier is meant to discover:

- Strong methods, pipelines, and analysis frameworks  
- High-value spatial/single-cell studies in other cancers (breast, lung, colon, etc.)  
- Computational work directly reusable in PCa projects

Gemini’s scoring rubric explicitly distinguishes:

- **PCa + tech**  
- **Tech + similar cancer**  
- **Method-only relevance**  

so Tier 2 output can be filtered downstream.

---

## 🏗️ Pipeline Architecture

```text
PubMed eSearch
  → Validate (counts + gold-set)
  → eSummary/eFetch (history-based)
  → Normalize records (metadata + XML)
        ↓
   Build Notion index (DedupeKey→page)
        ↓
   Classify records (create vs update)
        ↓
   New? → YES → Gemini enrichment → Create pages
        → NO  → Update pages only
```

All steps are orchestrated with **Prefect 3** tasks and flow:

- `literature_search_flow` – top-level flow  
- `get_config` – config + environment loading  
- `pubmed_esearch`, `pubmed_esummary_history`, `pubmed_efetch_abstracts_history`  
- `normalize_records`, `validate_results`, `validate_goldset`  
- `notion_build_index`, `classify_records`  
- `gemini_enrich_records`  
- `notion_create_pages`, `notion_update_pages`

---

## 📚 PubMed Integration

### E-Utilities Usage

The pipeline uses three main endpoints:

1. **`esearch.fcgi`**
   - Parameters:
     - `db=pubmed`
     - `term=QUERY_TERM`
     - `reldate=RELDATE_DAYS`
     - `retmax=RETMAX`
     - `usehistory=y`
     - `retmode=json`
     - `api_key=NCBI_API_KEY` (optional but recommended)
   - Outputs:
     - `count`: total hits
     - `idlist`: first page PMIDs
     - `WebEnv`, `QueryKey`: for history-based retrieval

2. **`esummary.fcgi` (history mode)**
   - Uses `WebEnv` + `QueryKey` instead of explicit ID lists.
   - Retrieved in batches (`EUTILS_BATCH`, e.g. 200).
   - Produces:
     - Title, journal, authors, publication types, DOIs
     - Pub dates, article types, etc.

3. **`efetch.fcgi` (history mode)**
   - Also uses `WebEnv` + `QueryKey`.
   - Returns XML for each PMID.
   - Parsed for:
     - Abstract text
     - MeSH headings + major MeSH
     - `DataBankList` (GEO / SRA accessions)
     - Additional flags used downstream

### Validation

Two main validations:

1. **Result sanity (`validate_results`)**
   - Checks:
     - `count == 0`
     - Large deviations vs a configured median (`HISTORICAL_MEDIAN`, if set)
   - Logs warnings rather than hard-failing.

2. **Gold-set check (`validate_goldset`)**
   - Maintains a small list of **must-appear** PMIDs/DOIs (landmark papers).
   - If any are missing from the retrieved records:
     - Logs an alert to reconsider query or investigate indexing.

---

## 🧪 Record Normalization

`normalize_records` merges eSummary and eFetch outputs into a single dict per article:

Typical fields:

- `PMID`
- `Title`
- `Journal`
- `PubDateRaw` + parsed `PubDateParsed`
- `DOI`, `URL` (PubMed)
- `Abstract`
- `Authors`
- `PublicationTypes`
- `IsReview`
- `MeshHeadingList` (with qualifiers)
- `MeSH_Terms` (descriptor names)
- `Major_MeSH`
- `GEO_List` (list of GEO accessions)
- `SRA_Project` (SRA accessions)
- `RawXML` (sanitized XML for Gemini)

A **`DedupeKey`** is assigned:

- If DOI exists: `DedupeKey = DOI`  
- Otherwise: `DedupeKey = "PMID:{PMID}"`  

`DedupeKey` is the primary key for Notion sync and for distinguishing new vs existing records.

---

## 🤖 Gemini Enrichment (Native JSON Mode)

### System Instruction (Conceptual Summary)

The Gemini system prompt is designed to:

- Understand the **two-tier query design** (Tier 1 PCa, Tier 2 broader cancers).  
- Evaluate relevance along both axes:
  - Tumor type (prostate vs other cancer vs non-cancer)
  - Technology / method (spatial, single-cell, multiome, trajectory, CNV, etc.)  
- Produce **strict JSON** matching this schema:

```json
{
  "RelevanceScore": 0,
  "WhyRelevant": "",
  "StudySummary": "",
  "Methods": "",
  "KeyFindings": "",
  "DataTypes": ""
}
```

- Focus on:
  - Data generated (modalities, platforms, scale)
  - Analytical / computational methods (Seurat, Scanpy, ArchR, etc.)
  - Biologic insights relevant to heterogeneity, TME, lineage plasticity, and resistance.

### RelevanceScore Rubric (0–100)

**90–100: Direct Hit**

- Prostate cancer or closely related GU tumors  
- AND advanced spatial/single-cell/multiome data **or** a clearly applicable computational method  
- Directly useful for PCa spatial/single-cell projects.

**70–89: Strongly Relevant**

- Any cancer type with high-quality spatial/single-cell/multiome work, clearly useful for method transfer,  
  **OR**  
- PCa with good but more standard omics, still informative for heterogeneity/TME/plasticity/resistance.

**40–69: Method-Level / Tangential**

- Strong methods or pipelines usable in spatial/single-cell analysis, but the biological context is distant,  
  **OR**  
- Cancer papers with only a modest single-cell/spatial angle.

**0–39: Irrelevant**

- No meaningful cancer focus and no relevant omics method.  
- Mostly clinical, social, device-oriented, or epidemiologic without computational genomics.

Gemini runs with **low temperature (≈0.1)** and enforced `response_schema` to keep results deterministic and machine-safe.

### Extracted Fields

- `StudySummary` – For a computational scientist; max 3 sentences; focus on data + methods.  
- `WhyRelevant` – 1–2 sentences explicitly linking tumor context + technology/method relevance.  
- `Methods` – Semicolon-separated list of experimental platforms and software/algorithms.  
- `KeyFindings` – Concise list-style text emphasizing heterogeneity, TME, plasticity, or resistance.  
- `DataTypes` – Comma-separated list of standardized data types.

---

## 🧬 DataTypes Controlled Vocabulary

`DataTypes` from Gemini are normalized to a controlled lowercase vocabulary. Current core set:

```text
scrna-seq, snrna-seq, scatac-seq, snatac-seq,
spatial transcriptomics, 10x visium, xenium, cosmx, geomx, slide-seq, merfish, seqfish,
multiome, cite-seq,
wgs, wes, atac-seq, chip-seq, bulk rna-seq,
cnv, h&e, scdna
```

Logic:

- Free-form Gemini output is split by commas.  
- Tokens are trimmed and mapped to known forms when possible.  
- Unknown tokens are preserved and appended; you can periodically review them to expand the vocabulary.

---

## 🧼 XML Sanitization

Before sending to Gemini:

- The PubMed XML is parsed.
- **`<AuthorList>`** and **`<ReferenceList>`** are removed.
- The goal is to:
  - Reduce token usage by ~30–40%.  
  - Focus the model on abstract, MeSH, databanks, and key metadata.  
  - Keep API cost manageable when scaling.

If XML parsing fails, the original XML string is used as a fallback rather than crashing the run.

---

## 🗄️ Notion Database Setup

### Required Properties

Your Notion database should have at least these properties (case-sensitive names):

| Property Name    | Type         | Notes                                      |
|------------------|--------------|--------------------------------------------|
| `Title`          | Title        | Paper title                                |
| `DedupeKey`      | Text         | DOI or `PMID:{id}`                         |
| `PMID`           | Text         | PubMed ID                                  |
| `DOI`            | Text         | DOI                                        |
| `URL`            | URL          | PubMed link                                |
| `Journal`        | Text         | Journal name                               |
| `PubDate`        | Date         | Publication date                           |
| `Abstract`       | Text         | Truncated to 2000 chars                    |
| `Authors`        | Text         | Comma-separated authors                    |
| `GEO_List`       | Text         | GEO accessions                             |
| `SRA_Project`    | Text         | SRA accessions                             |
| `MeshHeadingList`| Text         | Full MeSH with qualifiers                  |
| `MeSH_Terms`     | Multi-select | Descriptor names only                      |
| `Major_MeSH`     | Multi-select | Major topic descriptors                    |
| `StudySummary`   | Text         | AI-generated summary                       |
| `WhyRelevant`    | Text         | AI justification                           |
| `Methods`        | Text         | Semicolon-separated tools/methods          |
| `KeyFindings`    | Text         | Biological insights                        |
| `DataTypes`      | Multi-select | Normalized data types                      |
| `RelevanceScore` | Number       | 0–100 AI score                             |
| `IsReview`       | Text         | `"Yes"` / `"No"`                           |
| `PublicationTypes`| Text        | Semicolon-separated PubMed types           |
| `LastChecked`    | Date         | Last sync timestamp                        |

You can add more fields if you want (e.g. impact factor, notes, tags); the pipeline will ignore unknown properties.

### Truncation Safeguard

Notion enforces a **2000-character limit** for a single rich_text block in the API.

To avoid 400 errors:

- All long text fields (Abstract, StudySummary, KeyFindings, etc.) are passed through a `_truncate()` helper.
- This trims content to a safe length and appends an ellipsis.

### Multi-Select Normalization

- Commas inside source terms are replaced with `" -"` before sending to Notion multi-select.
- This avoids splitting a single MeSH term into multiple invalid tags.
- `DataTypes` are always mapped to the controlled vocabulary to keep your tag space manageable.

---

## ⚙️ Configuration Reference

Key configuration variables (from code + `.env` + CLI):

| Key            | Purpose                          | Source                |
|----------------|----------------------------------|-----------------------|
| `QUERY_TERM`   | PubMed search term               | CLI or tier default   |
| `RETMAX`       | Max records to process           | CLI / code            |
| `RELDATE_DAYS` | Look-back window in days         | CLI / code            |
| `NCBI_API_KEY` | Higher E-Utilities rate limits   | `.env`                |
| `NCBI_EMAIL`   | Contact email for NCBI           | `.env`                |
| `NCBI_DATETYPE`| Date type (e.g. `pdat`)          | `.env` (optional)     |
| `NOTION_TOKEN` | Notion API auth token            | `.env`                |
| `NOTION_DB_ID` | Target Notion database ID        | `.env`                |
| `GOOGLE_API_KEY`| Gemini API key                  | `.env`                |
| `DRY_RUN`      | Skip Notion writes/enrichment    | CLI flag              |
| `GOLD_SET`     | Landmark PMIDs/DOIs              | Code / config         |
| `EUTILS_BATCH` | Batch size for history fetching  | Code constant         |
| `TIER`         | 1 or 2 (query tier)              | CLI flag              |

---

## 🗓️ Scheduling & Automation

### Simple Cron

Example weekly cron job:

```bash
0 6 * * 1 cd /path/to/API_WF && /path/to/venv/bin/python Prefect_literatureSearch.py --tier 1 >> run.log 2>&1
```

You can run Tier 2 on a separate schedule if desired.

## 🚨 Error Handling & Retries

- **Requests session**:
  - Automatic retries with backoff for HTTP 429 and 5xx from NCBI/Notion.
- **Notion 429**:
  - Respects `Retry-After` header when present.
- **Gemini errors**:
  - Per-article failures are caught and logged.
  - The record is still created/updated in Notion without enrichment rather than being dropped.
- Prefect logging can be increased via:


---

## 💰 API Cost Estimates (Free Tier)

Approximate usage under typical academic settings:

| Service               | Free Tier                         | Pipeline Usage            | Estimated Cost |
|-----------------------|-----------------------------------|---------------------------|----------------|
| **NCBI E-Utilities**  | 10 req/sec with key              | ~400 requests/run         | Free           |
| **Gemini 2.5 Flash**  | ~15 RPM, ~1500 RPD (varies)      | ~1 request/new paper      | Free (for <1500 new papers/day) |
| **Notion API**        | Reads free; ~3 writes/sec        | O(100–300) writes/run     | Free           |

For normal PCa/spatial search frequency (weekly/biweekly), this is effectively **$0/month**.

---

## 🧭 Gold-Set Strategy

Maintaining a gold set is strongly recommended:

- A small list of PMIDs/DOIs representing:
  - Foundational spatial/single-cell PCa papers  
  - Key method papers you never want to miss  

If a gold article disappears from the hit set:

- Treat it as a **signal** that:
  - Your query may have drifted, or
  - NCBI indexing/search behavior changed.

Revisit:
- The query,
- Filters (`reldays`, `datetype`),
- And potentially your tier design.

---

## 🐛 Troubleshooting

### Common Issues

**`GOOGLE_API_KEY not set`**

- Check `.env` is loaded and `python-dotenv` is installed.
- Confirm the key is for the correct project in Google AI Studio.

**`400 Bad Request` from Notion**

- Usually due to overlength rich_text.
- Ensure you are using the version of the code that truncates long text.

**`429 Too Many Requests` from Gemini**

- You are hitting the per-minute limit.
- Ensure you are not running multiple instances.
- If needed, increase `time.sleep()` in the Gemini loop.

**No enrichment happening**

- Likely all records are classified as `to_update` (existing in Notion).
- Try:
  - Narrowing the time window,
  - Or checking the Notion DB and `DedupeKey` logic.

---

## 🔄 Future Extensions

Possible next steps (if you feel like extending it):

- Crossref / preprint integration (bioRxiv, medRxiv mapping).  
- Citation network tracking (e.g. store citing/cited DOIs).  
- Weekly email/Slack/Teams digest of new high-score papers.  
- More granular tiering (e.g. Tier 3: methods-only, no cancer restriction).

---

## 📚 References

- [NCBI E-Utilities Documentation](https://www.ncbi.nlm.nih.gov/books/NBK25501/)  
- [Gemini API Documentation](https://ai.google.dev/docs)  
- [Notion API Reference](https://developers.notion.com/reference/intro)  
- [Prefect Documentation](https://docs.prefect.io/)

---

If this pipeline saves you time or shapes your own workflow, documenting the changes in a fork or opening a PR will help the next person trying to solve the same problem.
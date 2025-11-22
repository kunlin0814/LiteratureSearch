# Literature Search & Triage Pipeline

**Automated PubMed literature screening with AI-powered relevance scoring for Prostate Cancer spatial biology research.**

![Python](https://img.shields.io/badge/python-3.9%2B-blue)
![Prefect](https://img.shields.io/badge/prefect-3.x-orange)
![License](https://img.shields.io/badge/license-MIT-green)

---

## 🎯 Overview

This pipeline automates the discovery, enrichment, and organization of scientific literature for **Prostate Cancer (PCa) spatial biology and multi-omics research**. It:

1. **Searches PubMed** using sophisticated queries targeting spatial transcriptomics, single-cell sequencing, and PCa biology.
2. **Enriches metadata** (GEO/SRA accessions, MeSH terms, abstracts) via NCBI E-Utilities.
3. **AI-powered triage**: Uses Google Gemini 2.5 Flash with **native JSON mode** to score relevance (0–100) and extract structured insights.
4. **Syncs to Notion**: Creates/updates a research database with deduplication, avoiding redundant API calls and costs.

---

## ✨ Key Features

### 🔬 Smart Literature Discovery
- **Two-tier query system**:
  - **Tier 1**: Prostate-specific spatial/single-cell/multiome studies
  - **Tier 2**: Broader cancer spatial biology (method development papers)
- **History-based batching**: Efficiently retrieves 200+ results via NCBI's WebEnv/QueryKey
- **Gold-set validation**: Alerts if known landmark papers are missing

### 🤖 AI-Powered Enrichment (Native JSON Mode)
- **Structured extraction** via Gemini 2.5 Flash with enforced schema:
  - `RelevanceScore` (0–100): PCa + spatial/single-cell relevance
  - `WhyRelevant`: 1-2 sentence justification
  - `StudySummary`: Data + algorithms (3 sentences max)
  - `Methods`: Semicolon-separated tool list (e.g., "Seurat v5; Cell2Location; ArchR")
  - `KeyFindings`: Tumor heterogeneity/resistance insights
  - `DataTypes`: Normalized ontology (e.g., "10x visium, scrna-seq, h&e")
- **Token optimization**: XML sanitization removes author/reference lists (~40% reduction)
- **Cost control**: Only enriches **new** papers (skips existing Notion entries)

### 📊 Production-Ready Data Pipeline
- **Notion 2000-char truncation**: Prevents API 400 errors on long abstracts
- **Multi-select normalization**: Comma-replacement for MeSH/DataTypes to avoid tag fragmentation
- **Retry logic**: Exponential backoff for PubMed (429/5xx) and Notion (429)
- **Deduplication**: DOI or PMID-based keys prevent duplicate pages

---

## 🚀 Installation

### Prerequisites
- **Python 3.9+**
- API Keys (all free tier available):
  - [NCBI API Key](https://www.ncbi.nlm.nih.gov/account/settings/)
  - [Notion Integration Token](https://www.notion.so/my-integrations)
  - [Google AI Studio API Key](https://aistudio.google.com/app/apikey)

### Setup

```bash
# Clone repository
git clone https://github.com/yourusername/API_WF.git
cd API_WF

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Environment Configuration

Create a `.env` file in the project root:

```bash
# NCBI E-Utilities
NCBI_API_KEY=your_ncbi_api_key_here
NCBI_EMAIL=your.email@institution.edu

# Google Gemini
GOOGLE_API_KEY=your_google_ai_api_key_here

# Notion
NOTION_TOKEN=secret_xxxxxxxxxxxxxxxxxxxxx
NOTION_DB_ID=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

**⚠️ Security**: Never commit `.env` to version control. Add it to `.gitignore`.

---

## 📖 Usage

### Quick Start

```bash
# Run with default settings (Tier 1 query, last 365 days, max 200 results)
python Prefect_literatureSearch.py

# Custom query and time window
python Prefect_literatureSearch.py \
  --query '("Prostatic Neoplasms"[MeSH] OR prostat*[tiab]) AND ("spatial transcriptomics"[tw] OR Visium[tw])' \
  --reldays 60 \
  --retmax 200

# Use Tier 2 query (broader cancer methods)
python Prefect_literatureSearch.py --tier 2 --reldays 180

# Dry run (test without writing to Notion)
python Prefect_literatureSearch.py --dry-run
```

### Command-Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--query` | Custom PubMed query string | Tier 1 query |
| `--tier` | Query tier (1=PCa-focused, 2=broader cancer) | 1 |
| `--reldays` | Days to look back (publication date) | 1095 (3 years) |
| `--retmax` | Maximum results to retrieve | 200 |
| `--dry-run` | Skip Notion writes (validation mode) | False |

---

## 🏗️ Pipeline Architecture

```
PubMed eSearch → Validate → eSummary/eFetch History → Normalize Records
     ↓
Build Notion Index → Classify (create vs update)
     ↓
New Papers? → YES → Gemini Enrichment → Create Notion Pages
           → NO  → Update Notion Pages → Done
```

### Key Components

1. **PubMed E-Utilities Integration**
   - `esearch`: History-based pagination with WebEnv/QueryKey
   - `esummary`: Batch metadata retrieval (title, journal, authors, pub types)
   - `efetch`: Full XML including abstracts, MeSH terms, GEO/SRA accessions

2. **Gemini AI Enrichment** (`gemini_enrich_records`)
   - Native JSON mode with enforced `response_schema`
   - Rate limiting (1 req/sec) to stay within free tier (15 RPM)
   - XML sanitization + DataTypes normalization

3. **Notion Database Sync**
   - Properties: Title, DOI, PMID, URL, Abstract, Authors, Journal, PubDate, GEO_List, SRA_Project, MeSH_Terms, Major_MeSH, StudySummary, WhyRelevant, Methods, KeyFindings, DataTypes, RelevanceScore, IsReview, PublicationTypes
   - Deduplication via `DedupeKey` (DOI or PMID)
   - Text truncation for 2000-char API limit

---

## 🔧 Notion Database Setup

### Required Properties

Your Notion database must have these properties (case-sensitive):

| Property Name | Type | Notes |
|--------------|------|-------|
| `Title` | Title | Auto-populated |
| `DedupeKey` | Text | DOI or PMID:{id} |
| `PMID` | Text | PubMed ID |
| `DOI` | Text | Digital Object Identifier |
| `URL` | URL | Link to PubMed |
| `Journal` | Text | Full journal name |
| `PubDate` | Date | Publication date |
| `Abstract` | Text | Truncated to 2000 chars |
| `Authors` | Text | Comma-separated list |
| `GEO_List` | Text | GEO accession numbers |
| `SRA_Project` | Text | SRA accession numbers |
| `MeshHeadingList` | Text | Full MeSH with qualifiers |
| `MeSH_Terms` | Multi-select | Descriptor names only |
| `Major_MeSH` | Multi-select | Major topic terms |
| `StudySummary` | Text | AI-generated (3 sentences) |
| `WhyRelevant` | Text | AI justification |
| `Methods` | Text | Semicolon-separated tools |
| `KeyFindings` | Text | Biological insights |
| `DataTypes` | Multi-select | Normalized data types |
| `RelevanceScore` | Number | 0-100 AI score |
| `IsReview` | Text | "Yes" or "No" |
| `PublicationTypes` | Text | Semicolon-separated |
| `LastChecked` | Date | Sync timestamp |

---

## 💰 API Cost Estimates (Free Tier)

| Service | Free Tier | Pipeline Usage | Estimated Cost |
|---------|-----------|----------------|----------------|
| **NCBI E-Utilities** | 10 req/sec with key | ~400 requests/run | **Free** |
| **Google Gemini 2.5 Flash** | 15 RPM, 1500 RPD | 1 req/paper, rate-limited | **Free** (up to 1500 papers/day) |
| **Notion API** | Unlimited reads, 3 req/sec writes | ~200 writes/run | **Free** |

**Total Cost**: $0/month for typical academic use (<1500 new papers/day).

### 📜 License

This project is licensed under the **MIT License**. See [LICENSE](LICENSE) for details.

### 🚨 Usage Guidelines

- **Rate Limiting**: The pipeline includes automatic rate limiting. Do not modify delays.
- **Attribution**: If you publish research using this pipeline, consider citing the repository.
- **Ethical Use**: Do not use for bulk commercial scraping or violate terms of service.

---

## 🐛 Troubleshooting

### Common Issues

**Error: `GOOGLE_API_KEY not set`**
- Solution: Add API key to `.env` file and ensure `python-dotenv` is installed.

**Error: `400 Bad Request` from Notion**
- Cause: Text field exceeds 2000 characters.
- Solution: Pipeline includes `_truncate()` for all rich_text properties.

**Error: `429 Too Many Requests` from Gemini**
- Cause: Exceeded 15 RPM limit.
- Solution: Pipeline includes `time.sleep(1)` rate limiting. Verify not running multiple instances.

**No enrichment happening**
- Check: Pipeline skips enrichment if all papers already exist in Notion (cost optimization).

### Debug Mode

Enable Prefect logging:

```bash
export PREFECT_LOGGING_LEVEL=DEBUG
python Prefect_literatureSearch.py
```

---

## 🤝 Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Enhancement Ideas

- [ ] Add support for Crossref API (preprints)
- [ ] Implement citation network analysis
- [ ] Export to BibTeX/Zotero
- [ ] Add email digest summaries
- [ ] Support for additional Notion properties (impact factor, citation count)

---

## 📚 References

- [PubMed E-Utilities Documentation](https://www.ncbi.nlm.nih.gov/books/NBK25501/)
- [Gemini API Documentation](https://ai.google.dev/docs)
- [Notion API Reference](https://developers.notion.com/reference/intro)
- [Prefect Documentation](https://docs.prefect.io/)

---

**⭐ If this pipeline helps your research, please star the repository!**

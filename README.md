# LiteratureSearch Prefect Flow

Automates a PubMed literature sweep and syncs results into a Notion database.

## Setup

1. Create and activate a Python 3.10+ environment.
2. Install dependencies:

```
pip install -r requirements.txt
```

3. Set environment variables:

- `NCBI_API_KEY` (optional but recommended)
- `NCBI_EMAIL` (your contact email for NCBI tool param)
- `NOTION_TOKEN` (integration secret)
- `NOTION_DB_ID` (target database ID)

## Run

Default query and settings:

```
python Prefect_literatureSearch.py
```

Override parameters:

```
python Prefect_literatureSearch.py \
  --query 'prostate cancer AND spatial' \
  --reldays 365 \
  --retmax 200 \
  --dry-run
```

The flow will:
- eSearch with history → get `WebEnv` + `QueryKey`
- Batch eSummary/eFetch with retries/backoff
- Normalize records and compute `DedupeKey`
- Build Notion index and create/update pages
- Emit a final summary

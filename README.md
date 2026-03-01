# diagnostic-python-freelance

A small portfolio repository of “freelance-style” Python tasks: clear inputs/outputs, deterministic processing, and a machine-readable report.

**Status**
- ✅ Task 1 — CSV cleaning + deduplication
- ✅ Task 2 — API fetch + normalization + report
- ⏳ Task 3 — planned (packaging / delivery)

---

## Requirements

- Python **3.11+**

Dependencies:
- Install base dependencies:
  ```bash
  pip install -r requirements.txt
  ```
- **Task 2 requires `requests`**:
  ```bash
  pip install requests
  ```

> Tip: It’s better to add `requests` to `requirements.txt` so a clean install works in one command.

---

## Setup

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

pip install -r requirements.txt
pip install requests
```

---

## Repository structure

```text
.
├── task1_cleaning/
│   ├── data/
│   │   └── input.csv
│   ├── out/
│   │   ├── clean.csv
│   │   └── report.json
│   └── main.py
├── task2_api/
│   ├── out/
│   │   ├── output.csv / output.json
│   │   └── report.json
│   └── main.py
├── requirements.txt
└── README.md
```

---

## Task 1 — CSV Cleaning & Deduplication

### What it does
Reads an input CSV, normalizes fields, removes fully-empty rows, deduplicates records by `phone` and/or `email`, and writes:
- cleaned CSV
- JSON report with metrics

### Input
Default: `task1_cleaning/data/input.csv`

Expected columns:
- `lead_id`, `name`, `phone`, `email`, `created_at`, `amount`

### Output
Defaults:
- cleaned CSV: `task1_cleaning/out/clean.csv`
- report JSON: `task1_cleaning/out/report.json`

### Run
From repository root:

```bash
python task1_cleaning/main.py
```

Optional CLI arguments:

```bash
python task1_cleaning/main.py \
  --input task1_cleaning/data/input.csv \
  --output task1_cleaning/out/clean.csv \
  --report task1_cleaning/out/report.json
```

### Report fields (high level)
- `rows_in`, `rows_out`
- `dropped_empty_rows`
- `invalid_phones`, `invalid_emails`, `invalid_dates`, `invalid_amounts`
- `duplicates_removed`

---

## Task 2 — API Fetcher → Normalizer → Report

### What it does
Fetches data from a public API, enriches posts with user info and comment counts, then writes:
- `output.csv` (or `output.json`)
- `report.json` with endpoint status and row counts

### Data sources (endpoints)
- Posts: `https://jsonplaceholder.typicode.com/posts`
- Users: `https://jsonplaceholder.typicode.com/users`
- Comments: `https://jsonplaceholder.typicode.com/comments`

### Enriched output schema
- `post_id`
- `title`
- `user_id`
- `user_name`
- `user_email`
- `comments_count`

### Run
From repository root:

```bash
python task2_api/main.py
```

Optional CLI arguments:

```bash
python task2_api/main.py \
  --out_dir task2_api/out \
  --format csv \
  --timeout 10 \
  --retries 3 \
  --sleep 1
```

### Report fields (high level)
- `started_at`, `finished_at`, `duration_sec`
- `endpoints`: list of `{url, ok, status_code, retries_used, error}`
- `rows`: `{posts, users, comments, posts_enriched}`
- `warnings`: notes about partial output (if any endpoint fails)

---

## Notes

- Output files are written to `task*/out/`.

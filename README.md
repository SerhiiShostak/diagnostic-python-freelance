# diagnostic-python-freelance

A small set of Python “freelance-style” mini-projects: clear input/output, deterministic behavior, and measurable reports.

- ✅ **Task 1** is finished.
- ⏳ **Task 2** and **Task 3** will be added to this same repository.

---

## Requirements

- Python **3.11+**
- Dependencies from `requirements.txt` (includes `dateparser`)

---

## Setup

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

pip install -r requirements.txt
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
├── requirements.txt
└── README.md
```

---

## Task 1 — CSV Cleaning (CSV → cleaned CSV + JSON report)

### What it does

Reads an input CSV, normalizes/validates fields, removes empty rows, deduplicates records, and writes:

- `clean.csv` — normalized data
- `report.json` — processing summary & metrics

### Input schema

`task1_cleaning/data/input.csv` must contain the following columns:

- `lead_id`
- `name`
- `phone`
- `email`
- `created_at`
- `amount`

### Key rules (high level)

- **name**: trimmed, repeated spaces collapsed
- **phone**: normalized to `+380XXXXXXXXX` (Ukraine format) or empty if invalid
- **email**: trimmed + lowercased + basic validity check; empty if invalid
- **created_at**: parsed from multiple formats → `YYYY-MM-DD`; empty if invalid
- **amount**: normalized to a decimal number with 2 digits (`1200.50`); empty if invalid

### Deduplication rule

Records are considered duplicates if they share **either**:
- normalized **phone** (non-empty), **or**
- normalized **email** (non-empty)

For each duplicate group, the script keeps **one** record:
1. records with a non-empty date are preferred;
2. among dated records, the **earliest** date wins;
3. ties are resolved deterministically by original order.

---

## Run Task 1

Run from the repository root:

```bash
python task1_cleaning/main.py
```

Default paths:
- input: `task1_cleaning/data/input.csv`
- output: `task1_cleaning/out/clean.csv`
- report: `task1_cleaning/out/report.json`

### Custom paths (CLI)

```bash
python task1_cleaning/main.py \
  --input task1_cleaning/data/input.csv \
  --output task1_cleaning/out/clean.csv \
  --report task1_cleaning/out/report.json
```

---

## Report format (`report.json`)

The report contains:

- `rows_in`
- `rows_out`
- `dropped_empty_rows`
- `invalid_phones`
- `invalid_emails`
- `invalid_dates`
- `invalid_amounts`
- `duplicates_removed`

---

## Task 2 (planned)

To be added later.

---

## Task 3 (planned)

To be added later.

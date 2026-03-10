# PE Firm Employee Tracker

Scrapes team pages from 40+ private equity firms weekly and produces a change report
(promotions, role changes, and leavers) by comparing two consecutive snapshots.

---

## Setup

```bash
# 1. Create a virtual environment
python -m venv .venv

# 2. Activate it
.venv\Scripts\activate          # Windows
source .venv/bin/activate       # macOS / Linux

# 3. Install dependencies
pip install -r requirements.txt

# 4. Install Playwright browsers (first time only)
playwright install chromium
```

---

## Weekly workflow

### Step 1 — Scrape

```bash
python main.py
```

Runs all scrapers and saves a timestamped snapshot:

```
employees_YYYYMMDD_HHMMSS.xlsx
```

### Step 2 — Compare

```bash
python compare.py
```

Auto-detects the two most recent `employees_*.xlsx` files and produces a report:

```
report_YYYYMMDD_HHMMSS.xlsx
```

Or pass files explicitly:

```bash
python compare.py employees_week1.xlsx employees_week2.xlsx
```

---

## Output files

### `employees_YYYYMMDD_HHMMSS.xlsx`

One row per employee with columns:

| Column | Description |
|---|---|
| `firm_name` | PE firm |
| `person_name` | Full name |
| `person_position` | Job title |
| `team` | Business unit / department |
| `location` | Office location |
| `date_scraped` | Date the data was collected |

### `report_YYYYMMDD_HHMMSS.xlsx`

**Sheet: Current Employees**

All employees from the current week's snapshot. The `change` column indicates:

| Value | Meaning | Highlight |
|---|---|---|
| *(blank)* | No change | — |
| `Promotion` | Title changed since last week | Yellow cell on position |
| `New Hire` | Not present last week | Light blue row |

A `previous_role` column shows the prior title for promoted employees.

**Sheet: Leavers**

Employees present last week but absent this week.

| Column | Description |
|---|---|
| `firm_name` | PE firm |
| `person_name` | Full name |
| `last_known_position` | Most recent title |
| `last_seen_date` | Date of last appearance in data |

---

## Firms covered

40 firms are scraped across two modes:

- **Custom scrapers** — firms with non-standard pages (API-based, paginated, tab-based, etc.)
- **Generic DOM scraper** — firms with standard card-based team pages

A small number of firms are skipped due to Cloudflare protection or no public team page
(see the "Skipped" section printed at the start of each run).

---

## Scheduling (optional)

To run the full pipeline automatically each week, create a scheduled task that runs:

```
.venv\Scripts\python.exe main.py && .venv\Scripts\python.exe compare.py
```

On Windows, use **Task Scheduler** and point it at a `.bat` file in this folder.

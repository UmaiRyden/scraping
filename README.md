# PE Firm Employee Tracker

Scrapes team pages from 40+ private equity firms weekly and produces a change report
(promotions, role changes, and leavers) by comparing two consecutive snapshots.

---

## How it works (automated)

The scraper runs **automatically every Monday at 06:00 UTC** via GitHub Actions —
no manual effort required.

Each week it:
1. Scrapes all firm team pages (`main.py`)
2. Compares this week vs last week (`compare.py`)
3. Commits the results to the `data/` folder in this repo

To download the latest report, go to the **`data/` folder** in the repo and download
the most recent `report_YYYYMMDD.xlsx` file.

To trigger a run manually (outside the Monday schedule):
1. Go to the **Actions** tab in GitHub
2. Click **Weekly PE Firm Scrape** in the left sidebar
3. Click **Run workflow → Run workflow**

Results will appear in `data/` within 60–90 minutes.

---

## Output files (in `data/`)

### `employees_YYYYMMDD_HHMMSS.xlsx` — Weekly snapshot

One row per employee:

| Column | Description |
|---|---|
| `firm_name` | PE firm |
| `person_name` | Full name |
| `person_position` | Job title |
| `team` | Business unit / department |
| `location` | Office location |
| `date_scraped` | Date the data was collected |

### `report_YYYYMMDD_HHMMSS.xlsx` — Weekly change report

**Sheet: Current Employees**

All employees from the current week. The `change` column indicates:

| Value | Meaning | Highlight |
|---|---|---|
| *(blank)* | No change | — |
| `Promotion` | Title changed since last week | Yellow cell on position |
| `New Hire` | Not present last week | Light blue row |

The `previous_role` column shows the prior title for employees who were promoted.

**Sheet: Leavers**

Employees present last week but absent this week:

| Column | Description |
|---|---|
| `firm_name` | PE firm |
| `person_name` | Full name |
| `last_known_position` | Most recent title |
| `last_seen_date` | Date of last appearance in data |

---

## Firms covered

40+ firms scraped across two modes:

- **Custom scrapers** — firms with non-standard pages (API-based, paginated, tab-based, etc.)
- **Generic DOM scraper** — firms with standard card-based team pages

A small number of firms are skipped due to Cloudflare protection or no public team page.
These are listed in the "Skipped" section at the start of each run log (visible in GitHub Actions).

---

## Local setup (optional)

Only needed if you want to run the scripts on your own machine.

```bash
# 1. Create a virtual environment
python -m venv .venv

# 2. Activate it
.venv\Scripts\activate          # Windows
source .venv/bin/activate       # macOS / Linux

# 3. Install dependencies
pip install -r requirements.txt

# 4. Install Playwright browser (first time only)
playwright install chromium
```

### Run scraper manually

```bash
python main.py
```

Saves `employees_YYYYMMDD_HHMMSS.xlsx` in the current directory.

### Run comparison manually

```bash
python compare.py
```

Auto-detects the two most recent `employees_*.xlsx` files and saves
`report_YYYYMMDD_HHMMSS.xlsx`. Or pass files explicitly:

```bash
python compare.py employees_week1.xlsx employees_week2.xlsx
```

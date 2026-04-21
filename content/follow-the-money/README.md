# Blood Money — NYC DSA Healthcare Working Group

A campaign finance transparency tool tracking donations from the healthcare lobby to New York State legislators.

**Live site:** [healthcare.socialists.nyc/follow-the-money](https://healthcare.socialists.nyc/follow-the-money) *(coming soon)*

Built by the [NYC DSA Healthcare Working Group](https://healthcare.socialists.nyc).

---

## What This Is

New York State lawmakers accept millions of dollars from health insurers, hospital lobbyists, and pharmaceutical companies — the same industries blocking universal healthcare. This tool makes that money visible.

Data source: [New York State Board of Elections Campaign Finance Disclosures](https://publicreporting.elections.ny.gov)

---

## Repo Structure

```
nyc-dsa-healthcare/
│
├── follow-the-money/           # The public-facing website
│   ├── index.html              # Main tool
│   ├── methodology.html        # Methodology + why this matters
│   └── data/
│       ├── legislators.json              # Feeds the web tool (generated)
│       └── legislators_contributions.csv # Downloadable by users (generated)
│
├── pipeline/                   # Data processing — runs locally, not deployed
│   ├── fetch_data.py           # Downloads raw NYSBOE data automatically
│   ├── process_data.py         # Processes raw data → legislators.json + CSV
│   ├── legislator_reference.csv  # Maintained crosswalk: names, parties, districts
│   └── raw/                    # NYSBOE bulk downloads land here (gitignored)
│
├── .gitignore
└── README.md
```

---

## Quarterly Update Workflow

Data is published by NYSBOE on a quarterly schedule (January, March/April, July, November). Each quarter:

**Step 1 — Download fresh data**
```bash
cd pipeline
python3 fetch_data.py --latest
```

Or for a specific year and period:
```bash
python3 fetch_data.py --year 2026 --period "July Periodic"
```

This downloads two CSVs into `pipeline/raw/`:
- `contributions_2026_July_Periodic.csv` — every itemized contribution
- `filers_2026_July_Periodic.csv` — the candidate/committee registry

**Step 2 — Run the pipeline**
```bash
python3 process_data.py \
  --contributions raw/contributions_2026_July_Periodic.csv \
  --filers raw/filers_2026_July_Periodic.csv \
  --period "Q3 2026"
```

This outputs:
- `follow-the-money/data/legislators.json` — feeds the web tool
- `follow-the-money/data/legislators_contributions.csv` — downloadable dataset

**Step 3 — Review the output**

The script prints a match report. If any candidate names scored below 85% confidence, add them to `NAME_OVERRIDES` in `process_data.py`:

```python
NAME_OVERRIDES = {
    "Christopher J. Ryan":    "Chris Ryan",
    "Jeremy Akbar Cooney":    "Jeremy Cooney",
    # add new mismatches here as they come up
}
```

**Step 4 — Push to GitHub**
```bash
git add follow-the-money/data/
git commit -m "Q3 2026 data update"
git push
```

GitHub Pages deploys automatically. The site is live within ~60 seconds.

---

## Pipeline Details

### `fetch_data.py`

Automates the NYSBOE bulk download. NYSBOE does not have a public API — this script reverse-engineers their download form and hits the `DownloadZipFile` endpoint directly with the right parameters.

```
Arguments:
  --latest          Auto-detects the most recent filing period
  --year YEAR       Specify a year (e.g. 2026)
  --period PERIOD   Specify a filing period (e.g. "January Periodic")
  --list            List all available years and filing periods
```

### `process_data.py`

Reads the raw NYSBOE CSVs and produces the tool's data files.

Key steps:
1. Loads contributions + filer files
2. Filters to non-individual monetary contributions only
3. Normalizes contributor names (collapses capitalization variants via `CANONICAL` dict)
4. Matches contributors to healthcare keyword list
5. Applies exclusions list (`EXCLUDED_CONTRIBUTORS`)
6. Fuzzy-matches NYSBOE candidate names to `legislator_reference.csv` for clean display names, party labels, district labels, and contact URLs
7. Classifies each donor by type (Insurer / Care Provider / Lobbying / Trade Org / Pharma / Healthcare Industry)
8. Outputs `legislators.json` and `legislators_contributions.csv`

**To add a new contributor to the exclusions list** (e.g. a false positive):
```python
EXCLUDED_CONTRIBUTORS = {
    "Ace American Insurance Co",
    "Matt Nimey Insurance",
    # add here
}
```

**To add a new canonical name normalization** (e.g. a contributor that files under multiple name variants):
```python
CANONICAL = {
    "hanys pac":   "HANYS PAC",
    "hanyspac":    "HANYS PAC",
    # add here
}
```

### `legislator_reference.csv`

A manually maintained crosswalk file mapping legislator names to clean display names, party affiliation, district labels, and contact URLs.

Columns: `filer_id, display_name, chamber, district_label, party, photo_url, contact_url`

- `filer_id` — intentionally blank; matching is done by name, not ID
- `display_name` — the clean name shown in the tool
- `chamber` — `senate` or `assembly`
- `district_label` — e.g. `Senate District 33 — The Bronx / Norwood`
- `party` — `D`, `R`, or `I`
- `photo_url` — optional; official legislature headshot URL
- `contact_url` — links to the legislator's official page

Update this file when:
- New legislators are elected (elections, special elections)
- A legislator changes party
- A seat becomes vacant

---

## Requirements

```bash
pip install pandas requests
```

Python 3.9+. No other dependencies for the pipeline. The web tool is pure HTML/CSS/JS with no build step.

---

## Deployment

The site is hosted on GitHub Pages from the `follow-the-money/` folder. DNS is managed separately via a CNAME record at `healthcare.socialists.nyc` pointing to the GitHub Pages URL.

To enable GitHub Pages:
1. Go to repo Settings → Pages
2. Source: Deploy from branch → `main`
3. Folder: `/follow-the-money`

---

## Contact

NYC DSA Healthcare Working Group — [healthcare.socialists.nyc](https://healthcare.socialists.nyc)

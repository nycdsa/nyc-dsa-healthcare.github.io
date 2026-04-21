"""
fetch_data.py
─────────────────────────────────────────────────────────────────────────────
Automated downloader for NYSBOE campaign finance bulk data.

Downloads two files per run:
  1. Disclosure Report (contributions) — the transaction-level data
  2. Filer Data — the candidate/committee registry

Both are saved to pipeline/raw/ and named with the year + filing period
so you always know what you have and files never overwrite each other.

USAGE
─────
  # Download a specific year + filing period:
  python3 fetch_data.py --year 2026 --period "January Periodic"

  # Download the most recent filing period automatically:
  python3 fetch_data.py --latest

  # List all available years and filing periods:
  python3 fetch_data.py --list

FILING PERIODS (Disclosure Report)
───────────────────────────────────
  January Periodic    — filed in January (covers prior year activity)
  March Periodic      — filed in March
  July Periodic       — filed in July
  General             — post-general election
  Primary             — post-primary election
  Special             — post-special election
  Off Cycle           — miscellaneous

QUARTERLY UPDATE WORKFLOW
─────────────────────────
  1. python3 fetch_data.py --latest
  2. python3 process_data.py
  3. git add follow-the-money/data/ && git commit -m "Q[N] [YEAR] data update" && git push
"""

import argparse
import os
import sys
import time
import zipfile
from datetime import datetime
from pathlib import Path

import requests

# ─── Configuration ──────────────────────────────────────────────────────────

BASE_URL   = "https://publicreporting.elections.ny.gov"
DOWNLOAD   = f"{BASE_URL}/DownloadCampaignFinanceData/DownloadZipFile"
RAW_DIR    = Path(__file__).parent / "raw"

# Year label → NYSBOE internal year code
# Add new years here as they become available each cycle
YEAR_CODES = {
    "2026": "47",
    "2025": "46",
    "2024": "45",
    "2023": "44",
    "2022": "41",
    "2021": "40",
    "2020": "39",
    "2019": "38",
    "2018": "37",
}

# Filing period order by calendar (used for --latest logic)
PERIOD_ORDER = [
    "January Periodic",
    "March Periodic",
    "Primary",
    "July Periodic",
    "General",
    "Special",
    "Off Cycle",
]

# ─── Helpers ────────────────────────────────────────────────────────────────

def sanitize(s):
    """Make a string safe for use in a filename."""
    return s.replace(" ", "_").replace("/", "-")


def download_file(params, label, dest_path):
    """Download a zip from NYSBOE and extract CSVs to dest_path."""
    print(f"  Downloading {label}...")
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; NYC-DSA-HCWG-DataPipeline/1.0)",
        "Referer": f"{BASE_URL}/DownloadCampaignFinanceData/DownloadCampaignFinanceData",
    }

    try:
        resp = requests.get(DOWNLOAD, params=params, headers=headers,
                            timeout=120, stream=True)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"  ✗ Request failed: {e}")
        return None

    content_type = resp.headers.get("Content-Type", "")
    if "zip" not in content_type and "octet-stream" not in content_type:
        print(f"  ✗ Unexpected content type: {content_type}")
        print(f"    (The server may have returned an error page instead of a zip)")
        return None

    zip_path = dest_path.with_suffix(".zip")
    total = 0
    with open(zip_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=65536):
            f.write(chunk)
            total += len(chunk)

    print(f"    Downloaded {total / 1024 / 1024:.1f} MB")

    # Extract zip
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            names = z.namelist()
            z.extractall(dest_path.parent)
            print(f"    Extracted: {', '.join(names)}")

            # Rename extracted CSV to our standard name
            for name in names:
                extracted = dest_path.parent / name
                if extracted.exists() and extracted != dest_path:
                    extracted.rename(dest_path)
                    break

        zip_path.unlink()  # Remove zip after extraction
        return dest_path

    except zipfile.BadZipFile:
        print(f"  ✗ Downloaded file is not a valid zip")
        zip_path.unlink(missing_ok=True)
        return None


def get_latest_period():
    """Determine the most recently available filing period based on today's date."""
    month = datetime.now().month
    year  = datetime.now().year

    # NYSBOE filing deadlines (approximate):
    # Jan 15 → January Periodic available
    # Mar 15 → March Periodic available
    # Jul 15 → July Periodic available
    # Nov 30 → General available
    if month >= 12 or month == 1:
        return str(year if month == 1 else year), "January Periodic"
    elif month in (2, 3, 4):
        return str(year), "January Periodic"
    elif month in (5, 6):
        return str(year), "March Periodic"
    elif month in (7, 8, 9):
        return str(year), "July Periodic"
    elif month in (10, 11):
        return str(year), "July Periodic"
    else:
        return str(year), "January Periodic"


# ─── Main logic ─────────────────────────────────────────────────────────────

def cmd_list():
    print("\nAvailable years:")
    for year in sorted(YEAR_CODES.keys(), reverse=True):
        print(f"  {year}  (code: {YEAR_CODES[year]})")
    print("\nAvailable filing periods (Disclosure Report):")
    for p in PERIOD_ORDER:
        print(f"  {p}")
    print("\nExample usage:")
    print('  python3 fetch_data.py --year 2026 --period "January Periodic"')
    print('  python3 fetch_data.py --latest')


def cmd_download(year_str, period):
    if year_str not in YEAR_CODES:
        print(f"✗ Unknown year '{year_str}'. Run --list to see available years.")
        sys.exit(1)

    year_code = YEAR_CODES[year_str]
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    safe_period = sanitize(period)
    print(f"\n{'─'*60}")
    print(f"  Year:   {year_str}")
    print(f"  Period: {period}")
    print(f"  Output: {RAW_DIR}/")
    print(f"{'─'*60}\n")

    # ── 1. Download Disclosure Report (contributions) ──────────────────────
    contrib_dest = RAW_DIR / f"contributions_{year_str}_{safe_period}.csv"
    contrib_params = {
        "lstDateType":   "2",          # Disclosure Report
        "lstUCYearDCF":  year_code,
        "lstFilingDesc": period,
    }
    contrib_result = download_file(contrib_params, "Contributions", contrib_dest)
    time.sleep(2)  # Be polite between requests

    # ── 2. Download Filer Data (candidate/committee registry) ──────────────
    filer_dest = RAW_DIR / f"filers_{year_str}_{safe_period}.csv"
    filer_params = {
        "lstDateType":   "1",          # Filer Data
        "lstUCYearDCF":  year_code,
        "lstFilingDesc": period,
    }
    filer_result = download_file(filer_params, "Filer Data", filer_dest)

    # ── Summary ─────────────────────────────────────────────────────────────
    print(f"\n{'─'*60}")
    if contrib_result and filer_result:
        print(f"✓ Both files downloaded successfully.\n")
        print(f"  Contributions: {contrib_result.name}")
        print(f"  Filer Data:    {filer_result.name}")
        print(f"\nNext step:")
        print(f"  python3 process_data.py \\")
        print(f"    --contributions raw/{contrib_result.name} \\")
        print(f"    --filers raw/{filer_result.name}")
    else:
        print("✗ One or more downloads failed. Check output above.")
        sys.exit(1)
    print(f"{'─'*60}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Download NYSBOE campaign finance data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--list",   action="store_true",
                       help="List available years and filing periods")
    group.add_argument("--latest", action="store_true",
                       help="Download the most recent filing period automatically")
    group.add_argument("--year",   type=str,
                       help="Year to download (e.g. 2026)")

    parser.add_argument("--period", type=str,
                        help='Filing period (e.g. "January Periodic"). Required with --year.')

    args = parser.parse_args()

    if args.list:
        cmd_list()

    elif args.latest:
        year_str, period = get_latest_period()
        print(f"Auto-detected latest period: {year_str} — {period}")
        cmd_download(year_str, period)

    elif args.year:
        if not args.period:
            print('✗ --period is required when using --year.')
            print('  Example: --year 2026 --period "January Periodic"')
            sys.exit(1)
        cmd_download(args.year, args.period)


if __name__ == "__main__":
    main()

"""
NYC DSA Healthcare Working Group — Campaign Finance Data Pipeline
=================================================================
Processes raw NYSBOE campaign finance bulk downloads into legislators.json
for the Follow the Money web tool.

USAGE:
    # Explicit file paths (recommended — output of fetch_data.py):
    python3 process_data.py \\
        --contributions raw/contributions_2026_January_Periodic.csv \\
        --filers raw/filers_2026_January_Periodic.csv

    # Or just edit the CONTRIBUTIONS_FILE / FILERS_FILE constants below
    # and run with no arguments:
    python3 process_data.py

INPUT FILES (place in pipeline/raw/):
    - contributions_YYYY_<period>.csv    NYSBOE bulk contributions download
    - filers_YYYY_<period>.csv           NYSBOE public campaign finance candidate list
    - legislator_reference.csv           Manually maintained name/party/district crosswalk

OUTPUT:
    - follow-the-money/data/legislators.json
    - follow-the-money/data/legislators_contributions.csv

NOTES:
    - The contributions file has a trailing comma, so 22 fields for 21 named columns.
      This is handled automatically.
    - Amount field comes in as "$34.00" format — stripped and cast to float.
    - Recipient ID is extracted from the Recipient field after the "#" character.
    - Candidate Name from the filers file is used as the canonical display name,
      rolling up multiple committee IDs per candidate correctly.
    - Healthcare industry classification uses keyword matching on Contributor Name.
    - If no legislator_reference.csv exists yet, the script still runs but
      district labels and photo/contact URLs will be blank.
"""

import argparse
import pandas as pd
import json
import os
import re
from datetime import datetime

# ─────────────────────────────────────────────
# CONFIGURATION — defaults, overridable via CLI
# ─────────────────────────────────────────────

CONTRIBUTIONS_FILE  = "pipeline/raw/contributions_YYYY_QQ.csv"
FILERS_FILE         = "pipeline/raw/filers_YYYY_QQ.csv"
REFERENCE_FILE      = "pipeline/legislator_reference.csv"
OUTPUT_FILE         = "follow-the-money/data/legislators.json"
LAST_UPDATED        = "Q2 2026"   # update each quarter

# ─────────────────────────────────────────────
# EXPLICIT EXCLUSIONS
# Contributor names that match keywords but are NOT healthcare industry
# Add to this list as needed each quarter
# ─────────────────────────────────────────────

EXCLUDED_CONTRIBUTORS = [
    "Ace American Insurance Co",        # Property/casualty insurer, not health
    "Matt Nimey Insurance",             # Local insurance broker, not health industry
    "NYS Veterinary Medical Society",   # Veterinary, not human healthcare
    "NYC Hospitality Alliance PAC",     # Restaurant/hospitality, matched on 'hospital'
]

# Normalize to lowercase for case-insensitive matching
EXCLUDED_CONTRIBUTORS_LOWER = [e.lower() for e in EXCLUDED_CONTRIBUTORS]
# Match against Contributor Name field
# ─────────────────────────────────────────────

HEALTHCARE_KEYWORDS = [
    # Broad industry terms
    "insurance",
    "health plan",
    "hospital",
    "pharmaceutical",
    "pharmacy",
    "healthcare",
    "health care",
    "medical",
    "managed care",
    "pbm",
    "benefit manager",
    "health system",
    "physician",
    "chiropractic",
    "podiatry",
    "nurse practitioner",
    "anesthesiology",
    "anesthesiologist",
    "radiology",
    "radiologist",
    "nursing",

    # Major insurers
    "unitedhealth",
    "united health",
    "aetna",
    "cigna",
    "anthem",
    "elevance",
    "bluecross",
    "blue cross",
    "emblem",
    "excellus",
    "humana",
    "molina",
    "centene",
    "cdphp",
    "medamerica",
    "metropolitan life",
    "metlife",
    "empire blue",
    "mvp health",
    "univera",
    "fidelis",
    "healthfirst",
    "metroplus",

    # Care providers / hospitals / health systems
    "northwell",
    "montefiore",
    "mount sinai",
    "nyu langone",
    "presbyterian",
    "memorial sloan",
    "maimonides",
    "kaleida",
    "suny downstate",
    "albany med",
    "garden of eden hfa",
    "davita",

    # Pharmacy / PBM
    "cvs",
    "walgreens",
    "rite aid",
    "express scripts",
    "mckesson",
    "amerisourcebergen",
    "cardinal health",

    # Major pharma
    "pfizer",
    "merck",
    "abbvie",
    "astrazeneca",
    "johnson & johnson",
    "sanofi",
    "lilly",
    "teva",
    "novartis",
    "bristol myers",
    "genentech",
    "amgen",
    "gilead",

    # Lobbying / trade orgs
    "phrma",
    "hanys",
    "nysana",
    "caipa",
    "uhap",
    "nyspt",
    "mlmic",
    "ny anesthesiologists",
    "nys radiologists",
    "nurse practitioners of nys",
    "hospital association",
    "medical society",
    "dental society",
    "life insurance council",
    "insurance association",
    "insurance council",
    "health insurance",
    "america's health insurance",
    "ahip",
]


# ─────────────────────────────────────────────
# COLUMN DEFINITIONS
# ─────────────────────────────────────────────

CONTRIB_COLS = [
    "Contribution Date", "Amount", "Contributor Name", "Detail Original Name",
    "Contributor Address", "Contributor City", "Contributor State", "Contributor Zip",
    "Contributor Country", "Transaction Type", "Contributor Type", "Transfer Type",
    "Recipient", "Disclosure Report", "Committee Type", "Filer Type", "Filer County",
    "Filer Municipality", "Filer Office", "Filer District", "Claimed For Match", "_extra"
]

FILER_COLS = [
    "Election Year", "Election Type", "Filer ID", "Committee Name", "Candidate Name",
    "Office", "District", "Registration Date", "Certified Date", "Competing Form Date",
    "Termination Date", "Cycle Start", "Cycle End", "_extra"
]


# ─────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────

def parse_amount(val):
    """Convert '$1,234.56' to float 1234.56"""
    if pd.isna(val):
        return 0.0
    return float(re.sub(r'[^\d.]', '', str(val)))


def is_healthcare(contributor_name):
    """Return True if contributor name matches any healthcare keyword
    and is not on the explicit exclusions list"""
    if pd.isna(contributor_name):
        return False
    name = str(contributor_name).lower()
    if name in EXCLUDED_CONTRIBUTORS_LOWER:
        return False
    return any(kw in name for kw in HEALTHCARE_KEYWORDS)


def extract_recipient_id(recipient_str):
    """Extract numeric ID from 'Committee Name - ID# 123456'"""
    if pd.isna(recipient_str):
        return None
    match = re.search(r'#\s*(\d+)', str(recipient_str))
    return int(match.group(1)) if match else None


def classify_donor_type(contributor_name, contributor_type):
    """Classify donor into a readable category for the tool"""
    name = str(contributor_name).lower() if not pd.isna(contributor_name) else ""
    if any(kw in name for kw in ["insurance", "health plan", "aetna", "cigna",
                                   "anthem", "bluecross", "blue cross", "emblem",
                                   "excellus", "humana", "molina", "centene",
                                   "united health", "unitedhealth", "empire blue",
                                   "mvp health", "univera", "fidelis", "healthfirst",
                                   "metroplus", "medamerica", "metropolitan life",
                                   "metlife", "cdphp", "elevance"]):
        return "Insurer"
    if any(kw in name for kw in ["hospital", "health system", "northwell",
                                   "montefiore", "mount sinai", "langone",
                                   "presbyterian", "memorial sloan", "maimonides",
                                   "kaleida", "medical center", "garden of eden hfa",
                                   "davita"]):
        return "Care Provider / Hospital / Health System"
    if any(kw in name for kw in ["hanys", "hospital association", "medical society",
                                   "phrma", "ahip", "insurance association",
                                   "insurance council", "nysana", "caipa", "uhap",
                                   "nyspt", "mlmic", "ny anesthesiologists",
                                   "nys radiologists", "nurse practitioners of nys"]):
        return "Lobbying / Trade Org"
    if any(kw in name for kw in ["pfizer", "merck", "abbvie", "astrazeneca",
                                   "johnson & johnson", "sanofi", "lilly", "teva",
                                   "novartis", "bristol myers", "genentech",
                                   "amgen", "gilead", "pharmaceutical"]):
        return "Pharma"
    if any(kw in name for kw in ["cvs", "walgreens", "rite aid", "express scripts",
                                   "mckesson", "amerisourcebergen", "pharmacy", "pbm",
                                   "benefit manager"]):
        return "Pharmacy / PBM"
    if "union" in str(contributor_type).lower():
        return "Union / Healthcare"
    return "Healthcare Industry"


def normalize_contributor_name(name):
    """Normalize contributor names to collapse capitalization variants and
    common abbreviation differences into a single canonical form."""
    if pd.isna(name):
        return name

    # Strip whitespace and convert to title case as baseline
    n = str(name).strip().title()

    # ── Manual canonical mappings ──
    # Maps any variant (lowercased for matching) to the canonical display name.
    # Add new entries here whenever you spot duplicates in the donor output.
    CANONICAL = {
        # HANYS variants
        "hanys pac":                                "HANYS PAC",
        "hanys":                                    "HANYS PAC",
        "hospital association of new york pac":     "HANYS PAC",
        "hospital association of new york state":   "HANYS PAC",

        # Anthem variants
        "anthem health pac":                        "Anthem Health PAC",
        "anthem health p.a.c":                      "Anthem Health PAC",

        # NYS Radiologists variants
        "nys radiologists pac":                     "NYS Radiologists PAC",

        # Anesthesiology variants — keep NYSANA separate from general anesthesiologists
        "ny anesthesiologists pac":                 "NY Anesthesiologists PAC",
        "nys anesthesiologists pac":                "NY Anesthesiologists PAC",
        "new york anesthesiologists political act":  "NY Anesthesiologists PAC",
        "new york anesthesiologists political action committee": "NY Anesthesiologists PAC",
        "ny anesthesiologists":                     "NY Anesthesiologists PAC",

        # Medamerica variants
        "medamerica insurance co":                  "Medamerica Insurance",
        "medamerica insurance company of florida":  "Medamerica Insurance",

        # MLMIC variants
        "mlmicpac":                                 "MLMIC PAC",
        "mlmic pac":                                "MLMIC PAC",
    }

    lookup = n.lower().strip()
    return CANONICAL.get(lookup, n)




def run():
    print(f"[1/6] Loading files...")

    # ── Load contributions ──
    contribs = pd.read_csv(
        CONTRIBUTIONS_FILE,
        names=CONTRIB_COLS,
        skiprows=1,
        dtype=str
    )
    print(f"      Contributions: {len(contribs):,} rows loaded")

    # ── Load filers ──
    filers = pd.read_csv(
        FILERS_FILE,
        names=FILER_COLS,
        skiprows=1,
        dtype=str
    )
    print(f"      Filers: {len(filers):,} rows loaded")

    # ── Load reference (optional) ──
    ref_exists = os.path.exists(REFERENCE_FILE)
    if ref_exists:
        ref = pd.read_csv(REFERENCE_FILE, dtype=str)
        print(f"      Reference: {len(ref):,} rows loaded")
    else:
        print(f"      Reference file not found — district labels will be auto-generated")
        ref = None

    # ─────────────────────────────
    print(f"\n[2/6] Cleaning and preparing data...")

    # Parse amounts
    contribs["Amount_Clean"] = contribs["Amount"].apply(parse_amount)

    # Extract Recipient ID
    contribs["Recipient ID"] = contribs["Recipient"].apply(extract_recipient_id)

    # Parse contribution dates
    contribs["Contribution Date"] = pd.to_datetime(
        contribs["Contribution Date"], errors="coerce"
    )

    # Clean filer IDs
    filers["Filer ID"] = pd.to_numeric(filers["Filer ID"], errors="coerce")
    filers["District"] = pd.to_numeric(filers["District"], errors="coerce")

    # Keep only monetary contributions (exclude in-kind, transfers)
    monetary_types = [
        "A - Monetary Contributions Received From Ind. & Part.",
        "B - Monetary Contributions Received From Corporation",
        "C - Monetary Contributions Received From All Other"
    ]
    contribs = contribs[contribs["Transaction Type"].isin(monetary_types)].copy()
    print(f"      Monetary contributions: {len(contribs):,} rows")

    # Exclude individual contributors — we only care about orgs, PACs, companies
    contribs = contribs[contribs["Contributor Type"] != "Individual"].copy()
    print(f"      After excluding individuals: {len(contribs):,} rows")

    # Normalize contributor names to collapse capitalization and known variants
    contribs["Contributor Name"] = contribs["Contributor Name"].apply(normalize_contributor_name)
    print(f"      Contributor names normalized")

    # ─────────────────────────────
    print(f"\n[3/6] Filtering healthcare industry donations...")

    contribs["is_healthcare"] = contribs["Contributor Name"].apply(is_healthcare)
    hc = contribs[contribs["is_healthcare"]].copy()
    print(f"      Healthcare donations: {len(hc):,} rows")
    print(f"      Healthcare total: ${hc['Amount_Clean'].sum():,.0f}")

    # ─────────────────────────────
    print(f"\n[4/6] Merging with filer/candidate data...")

    # Build a clean candidate lookup: Filer ID → Candidate Name, Office, District
    # De-duplicate: one candidate can have multiple filer IDs across election years
    # Keep most recent election year per Filer ID
    filers_sorted = filers.sort_values("Election Year", ascending=False)
    filer_lookup = filers_sorted.drop_duplicates(subset=["Filer ID"])[
        ["Filer ID", "Candidate Name", "Office", "District"]
    ].copy()

    # Merge healthcare donations to candidate names
    hc = hc.merge(
        filer_lookup,
        left_on="Recipient ID",
        right_on="Filer ID",
        how="left"
    )

    # Drop rows with no candidate match
    unmatched = hc["Candidate Name"].isna().sum()
    if unmatched > 0:
        print(f"      WARNING: {unmatched} rows could not be matched to a candidate")
    hc = hc.dropna(subset=["Candidate Name"]).copy()

    # Determine chamber from Office field
    hc["chamber"] = hc["Office"].apply(
        lambda x: "senate" if "Senator" in str(x) else "assembly"
    )

    # ─────────────────────────────
    print(f"\n[5/6] Aggregating totals and building top donors...")

    # Total healthcare donations per candidate (rolled up by Candidate Name)
    totals = (hc.groupby("Candidate Name")["Amount_Clean"]
                .sum()
                .reset_index()
                .rename(columns={"Amount_Clean": "total_healthcare"}))

    # Top 5 donors per candidate (by Contributor Name, rolled up)
    donor_totals = (hc.groupby(["Candidate Name", "Contributor Name"])
                      .agg(
                          amount=("Amount_Clean", "sum"),
                          contributor_type=("Contributor Type", "first")
                      )
                      .reset_index()
                      .sort_values("amount", ascending=False))

    # Build top 5 per candidate
    top_donors_map = {}
    for cand, group in donor_totals.groupby("Candidate Name"):
        top5 = group.head(5)
        top_donors_map[cand] = [
            {
                "name": row["Contributor Name"],
                "type": classify_donor_type(row["Contributor Name"], row["contributor_type"]),
                "amount": int(row["amount"])
            }
            for _, row in top5.iterrows()
        ]

    # Get one row per candidate for chamber + district info
    cand_meta = (hc.groupby("Candidate Name")
                   .agg(
                       chamber=("chamber", "first"),
                       district=("District", "first"),
                       filer_id=("Filer ID", "first")
                   )
                   .reset_index())

    # Merge totals with meta
    results = totals.merge(cand_meta, on="Candidate Name")

    # ─────────────────────────────
    # Apply reference file overrides via fuzzy name matching
    if ref_exists:
        import difflib

        ref_names = ref["display_name"].dropna().tolist()

        def fuzzy_match_name(candidate_name):
            """Match a NYSBOE Candidate Name to the closest display_name in the reference.
            Returns (matched_display_name, score) or (None, 0) if no good match found."""
            if pd.isna(candidate_name):
                return None, 0

            # Normalize both for comparison: lowercase, strip punctuation/suffixes
            def normalize(n):
                n = str(n).lower().strip()
                # Remove common suffixes that vary between sources
                for suffix in [" jr.", " jr", " sr.", " sr", " iii", " ii", " iv",
                                " esq", " esq.", " phd", " md", " jd"]:
                    n = n.replace(suffix, "")
                # Remove punctuation
                for ch in [".", ",", "-", "'"]:
                    n = n.replace(ch, "")
                return n.strip()

            norm_candidate = normalize(candidate_name)
            norm_refs = {normalize(r): r for r in ref_names}

            # Try exact match first (after normalization)
            if norm_candidate in norm_refs:
                return norm_refs[norm_candidate], 100

            # Fuzzy match using difflib SequenceMatcher
            matches = difflib.get_close_matches(
                norm_candidate,
                norm_refs.keys(),
                n=1,
                cutoff=0.75
            )

            if matches:
                best_norm = matches[0]
                score = difflib.SequenceMatcher(
                    None, norm_candidate, best_norm
                ).ratio() * 100
                return norm_refs[best_norm], round(score, 1)

            return None, 0

        print(f"\n      Fuzzy-matching {results['Candidate Name'].nunique()} candidates to reference...")

        match_results = []
        for cand in results["Candidate Name"]:
            matched_name, score = fuzzy_match_name(cand)
            match_results.append({"Candidate Name": cand, "_ref_name": matched_name, "_match_score": score})

        match_df = pd.DataFrame(match_results)

        # Report matches
        good = match_df[match_df["_match_score"] >= 85]
        poor = match_df[match_df["_match_score"] < 85]
        print(f"      Matched: {len(good)} candidates (score ≥ 85)")
        if len(poor) > 0:
            print(f"      Low-confidence / unmatched ({len(poor)}):")
            for _, r in poor.iterrows():
                print(f"        '{r['Candidate Name']}' → '{r['_ref_name']}' (score: {r['_match_score']})")

        # Merge match results onto results df
        results = results.merge(match_df, on="Candidate Name", how="left")

        # Now pull reference columns using the matched display_name
        ref_lookup = ref.drop_duplicates(subset=["display_name"]).set_index("display_name")

        # Manual overrides for names that don't fuzzy-match well
        # Key = NYSBOE Candidate Name (exact), Value = display_name in reference file
        NAME_OVERRIDES = {
            "Christopher J. Ryan":  "Chris Ryan",
            "Jeremy Akbar Cooney":  "Jeremy Cooney",
            "Jose Gustavo Rivera":  "Gustavo Rivera",
            "Kristen S Gonzalez":   "Kristen Gonzalez",
            "Aber Abdelkareem Kawas": "Aber Kawas",
        }

        def resolve_ref_name(row):
            """Return the reference display_name to use for this candidate."""
            cand = row["Candidate Name"]
            if cand in NAME_OVERRIDES:
                return NAME_OVERRIDES[cand]
            ref_name = row.get("_ref_name")
            if ref_name and row.get("_match_score", 0) >= 85:
                return ref_name
            return None

        def get_ref_field(ref_name, field):
            if ref_name and ref_name in ref_lookup.index:
                if field == "display_name":
                    return ref_name  # it IS the index
                val = ref_lookup.at[ref_name, field]
                return val if pd.notna(val) and str(val).strip() not in ["", "nan"] else None
            return None

        results["display_name"] = results.apply(
            lambda r: get_ref_field(resolve_ref_name(r), "display_name") or r["Candidate Name"], axis=1
        )
        results["district_label"] = results.apply(
            lambda r: get_ref_field(resolve_ref_name(r), "district_label") or
                      f"{'Senate' if r['chamber'] == 'senate' else 'Assembly'} "
                      f"District {int(r['district']) if pd.notna(r['district']) else '—'}",
            axis=1
        )
        results["party"]       = results.apply(lambda r: get_ref_field(resolve_ref_name(r), "party"), axis=1)
        results["photo_url"]   = results.apply(lambda r: get_ref_field(resolve_ref_name(r), "photo_url"), axis=1)
        results["contact_url"] = results.apply(lambda r: get_ref_field(resolve_ref_name(r), "contact_url"), axis=1)

    else:
        results["display_name"] = results["Candidate Name"]
        results["district_label"] = results.apply(
            lambda r: f"{'Senate' if r['chamber'] == 'senate' else 'Assembly'} "
                      f"District {int(r['district']) if pd.notna(r['district']) else '—'}",
            axis=1
        )
        results["party"] = None
        results["photo_url"] = None
        results["contact_url"] = None

    # Sort by total healthcare donations descending
    results = results.sort_values("total_healthcare", ascending=False).reset_index(drop=True)

    # ─────────────────────────────
    print(f"\n[6/6] Building output JSON...")

    legislators = []
    for i, row in results.iterrows():
        cand_name = row["Candidate Name"]
        legislators.append({
            "id": i + 1,
            "name": row["display_name"] if pd.notna(row.get("display_name")) else cand_name,
            "chamber": row["chamber"],
            "district": row["district_label"] if pd.notna(row.get("district_label")) else "",
            "party": row["party"] if pd.notna(row.get("party")) else "",
            "photo_url": row["photo_url"] if pd.notna(row.get("photo_url")) else None,
            "contact_url": row["contact_url"] if pd.notna(row.get("contact_url")) else None,
            "total_healthcare": int(row["total_healthcare"]),
            "top_donors": top_donors_map.get(cand_name, [])
        })

    output = {
        "meta": {
            "last_updated": LAST_UPDATED,
            "total_donated": int(hc["Amount_Clean"].sum()),
            "unique_donors": int(hc["Contributor Name"].nunique()),
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M")
        },
        "legislators": legislators
    }

    # Write output
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    # ─────────────────────────────
    # Build downloadable CSV — one row per contribution, cleaned up
    # ─────────────────────────────

    # Contributor descriptions — researched from public sources.
    # Key = normalized contributor name (as output after normalize_contributor_name)
    # Value = one-sentence description
    CONTRIBUTOR_DESCRIPTIONS = {
        "Anthem Health PAC":
            "The political action committee of Anthem Health (now Elevance Health), one of the largest for-profit health insurance companies in the United States.",
        "Caipa Pac":
            "The political action committee of CAIPA (Coalition of Asian-American IPA), a New York independent practice association serving over 500,000 patients in the Asian community through a network of 1,800+ private practice providers.",
        "Cdphp State Pac":
            "The political action committee of CDPHP (Capital District Physicians' Health Plan), a physician-founded not-for-profit health insurer serving over 400,000 members across upstate New York.",
        "Committee For Medical Eye Care":
            "A political action committee that advocates on behalf of ophthalmologists and the medical eye care industry in New York State.",
        "Davita":
            "DaVita Inc. is a Fortune 500 for-profit company and one of the largest providers of kidney dialysis services in the United States, operating over 2,600 outpatient centers.",
        "Garden Of Eden Hfa":
            "Garden of Eden HFA is a Brooklyn-based licensed assisted living and home care facility providing long-term care services to seniors in New York.",
        "Greater New York Nursing Home":
            "An organization representing nursing home operators in the Greater New York area that advocates for the long-term care industry's interests in state policy.",
        "HANYS PAC":
            "The political action committee of HANYS (Healthcare Association of New York State), the statewide trade association representing hospitals, health systems, and continuing care providers.",
        "MLMIC PAC":
            "The political action committee of MLMIC Insurance Company (Medical Liability Mutual Insurance Company), New York's largest medical malpractice insurer, now a Berkshire Hathaway company.",
        "Medamerica Insurance":
            "Medamerica Insurance Company is a long-term care insurance provider that markets and underwrites policies for individuals and employer groups.",
        "Metlife Pac":
            "The political action committee of MetLife Inc., one of the world's largest insurance companies offering life, dental, disability, and other coverage products.",
        "Metropolitan Life Company":
            "Metropolitan Life Insurance Company (MetLife) is one of the largest global providers of insurance, annuities, and employee benefit programs, headquartered in New York City.",
        "Montefiore Pac Inc.":
            "The political action committee of Montefiore Health System, a major academic health system based in the Bronx operating 14 hospitals and over 300 outpatient sites across the New York metro area.",
        "NY Anesthesiologists PAC":
            "The political action committee of the New York State Society of Anesthesiologists (NYSSA), representing over 4,000 anesthesiologists who advocate on scope-of-practice, reimbursement, and healthcare policy issues in New York.",
        "NYS Radiologists PAC":
            "The political action committee of the New York State Radiological Society (NYSRS), the state chapter of the American College of Radiology, representing radiologists and advocating for imaging services and reimbursement policy.",
        "New York Chiropractic Political Action C":
            "The political action committee of the New York Chiropractic Council, the professional association representing licensed chiropractors and advocating for chiropractic scope of practice in New York State.",
        "Nurse Practitioners Of Nys Pac":
            "The political action committee of The Nurse Practitioner Association New York State (NPA), formed to support candidates who champion nurse practitioner practice and advance NP visibility in the state legislature.",
        "Ny Podiatry Pac":
            "The political action committee representing podiatrists in New York State, advocating for podiatric medicine, reimbursement policy, and scope of practice legislation.",
        "Nysana-Crna Pac":
            "The political action committee of the New York State Association of Nurse Anesthetists (NYSANA), representing over 1,900 Certified Registered Nurse Anesthetists (CRNAs) and advocating for CRNA practice authority in New York.",
        "Nyspt Political Action Committee":
            "The political action committee of the New York Physical Therapy Association (NYSPT), representing physical therapists and advocating for PT practice, reimbursement, and access to care in New York State.",
        "The Life Insurance Council Of New York P":
            "The political action committee of the Life Insurance Council of New York (LICONY), a trade association representing life and health insurers doing business in New York State.",
        "Uhap Pac":
            "The political action committee of the Upstate Hospital Association of Providers (UHAP), an organization that unites Upstate New York hospitals and health systems around shared legislative priorities.",
    }

    hc_csv = hc.copy()

    # Add donor type classification
    hc_csv["Donor Industry Type"] = hc_csv.apply(
        lambda r: classify_donor_type(r["Contributor Name"], r["Contributor Type"]), axis=1
    )

    # Add contributor description
    hc_csv["Contributor Description"] = hc_csv["Contributor Name"].map(
        CONTRIBUTOR_DESCRIPTIONS
    ).fillna("No description available.")

    # Build clean export columns
    export = pd.DataFrame({
        "Contribution Date":       hc_csv["Contribution Date"].dt.strftime("%Y-%m-%d"),
        "Amount":                  hc_csv["Amount_Clean"],
        "Contributor Name":        hc_csv["Contributor Name"],
        "Donor Industry Type":     hc_csv["Donor Industry Type"],
        "Contributor Description": hc_csv["Contributor Description"],
        "Recipient Name":          hc_csv["Candidate Name"],
        "Recipient Filer ID":      hc_csv["Filer ID"].astype("Int64"),
    })

    csv_path = OUTPUT_FILE.replace(".json", "_contributions.csv")
    export.sort_values(["Recipient Name", "Contribution Date"]).to_csv(csv_path, index=False)

    print(f"  Downloadable CSV written to: {csv_path}")
    print(f"  CSV rows: {len(export):,}")

    print(f"\n✓ Done!")
    print(f"  Legislators processed: {len(legislators)}")
    print(f"  Total healthcare donations: ${output['meta']['total_donated']:,}")
    print(f"  Unique donor organizations: {output['meta']['unique_donors']}")
    print(f"  Output written to: {OUTPUT_FILE}")
    print(f"\n  Top 5 recipients:")
    for leg in legislators[:5]:
        print(f"    {leg['name']}: ${leg['total_healthcare']:,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process NYSBOE data for the Blood Money tool")
    parser.add_argument("--contributions", type=str,
                        help="Path to contributions CSV (overrides CONTRIBUTIONS_FILE constant)")
    parser.add_argument("--filers", type=str,
                        help="Path to filers CSV (overrides FILERS_FILE constant)")
    parser.add_argument("--reference", type=str,
                        help="Path to legislator_reference.csv (overrides REFERENCE_FILE constant)")
    parser.add_argument("--output", type=str,
                        help="Path for output JSON (overrides OUTPUT_FILE constant)")
    parser.add_argument("--period", type=str,
                        help='Filing period label for LAST_UPDATED (e.g. "Q1 2026")')
    args = parser.parse_args()

    if args.contributions:
        CONTRIBUTIONS_FILE = args.contributions
    if args.filers:
        FILERS_FILE = args.filers
    if args.reference:
        REFERENCE_FILE = args.reference
    if args.output:
        OUTPUT_FILE = args.output
    if args.period:
        LAST_UPDATED = args.period

    run()

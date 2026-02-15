# vayo - Current State

**Last updated:** 2026-02-14
**Sessions:** ~6+
**Readiness:** 35%

## Goal
Build a "Zillow meets Bloomberg Terminal" apartment finder that uses public data transparency to surface hidden gems and predict availability before listings go live.

## Status
Data consolidation complete. Clean database built. Scoring prototype exists. Ready to iterate on the product.

## What We Have

### Database: `vayo_clean.db` (8.4 GB) — THE canonical database
All tables keyed on BBL integer with proper indexes. 14 tables, ~49M records.

| Table | Rows | Notes |
|-------|------|-------|
| buildings | 767,302 | All NYC residential from PLUTO (foundation) |
| acris_transactions | 1,222,855 | Proper BBL + condo mapping + buyer/seller names |
| complaints | 25,038,632 | HPD complaints via BIN→BBL |
| service_requests_311 | 15,132,059 | Detailed 311 with resolution + lat/lon |
| complaints_311 | 3,299,201 | Compact 311 (from nyc_311_complete) |
| ecb_violations | 836,477 | ECB violations via BIN→BBL |
| building_contacts | 713,578 | Managing agents, owners, officers |
| hpd_litigation | 204,005 | HPD litigation cases |
| dob_permits | 950,978 | DOB permits via boro+block+lot |
| dob_complaints | 1,438,476 | DOB complaints via BIN→BBL |
| marshal_evictions | 38,979 | Address-matched to PLUTO BBLs |
| rent_stabilized | 40,132 | With 421-a/J-51 flags |
| certificates_of_occupancy | 88,437 | Legal occupancy changes |
| bin_map | 194,209 | BIN→BBL cross-reference |

### Database: `all_nyc_units.db` (785 MB) — unit-level dataset
- 3,893,293 total units across 767,302 buildings
- 912,653 real discovered units (23.4%), 2,980,640 placeholders
- Still needs BIN-only HPD units added (see unfinished work below)

### Apartment Finder Prototype
- `scripts/apartment_finder.py` — Scores buildings on two axes:
  - **Gem Score (0-100):** Size, maintenance, noise, character, rent stabilization
  - **Availability Score (0-100):** ACRIS turnover, estate/trust signals, lis pendens, mortgage satisfactions
- Tested on Gramercy area: 7,243 buildings scored, 117 "diamonds" found
- Currently reads from OLD big DB — needs update to use `vayo_clean.db`

### Key Insight
Buildings like 18 Gramercy Park South, 12 East 13th, 31 West 21st show active ACRIS trading (LLCs, trusts, estate transfers) but never appear on StreetEasy. Public data reveals a parallel market.

## Deleted
- `stuy-scrape-csv/stuytown.db` (38 GB) — deleted 2026-02-14. All unique data consolidated into `vayo_clean.db`.

## Unfinished Work

### 1. Update `apartment_finder.py` to use `vayo_clean.db`
- Currently queries the old 38GB DB (now deleted)
- Should be much simpler since all tables now have BBL keys
- No more BIN→BBL joins needed at query time

### 2. BIN-to-BBL Matching for `all_nyc_units.db` (~1.9M more real units)
- CSV exports ready at `nycdb_data/hpd_units.csv`, `nycdb_data/text_units_nomatch.csv`, `nycdb_data/bin_to_bbl.csv`
- Script: `scripts/add_missing_units.py`
- Would push real unit coverage from 23% → ~55%+

### 3. Product Development
- Web UI for apartment finder
- More scoring dimensions (energy efficiency, permit activity, eviction history)
- Alert system for ACRIS activity on target buildings
- Neighborhood-level aggregation

## Key Files
- `vayo_clean.db` — Consolidated clean database (8.4 GB)
- `all_nyc_units.db` — Unit-level dataset (785 MB)
- `scripts/build_clean_db.py` — Builds vayo_clean.db from scratch
- `scripts/fix_remaining.py` — Fixes DOB complaints/permits, marshal evictions
- `scripts/pull_remaining.py` — Pulls rent stab, detailed 311, certificates of occupancy
- `scripts/apartment_finder.py` — Scoring engine prototype (needs update)
- `scripts/diagnose_formats.sql` — BBL format diagnosis
- `gramercy_gems.json` — Scored results for Gramercy area

## Key Technical Decisions
- **PLUTO-anchored:** All data matched to PLUTO's 767K residential buildings via integer BBL
- **BIN→BBL bridge table:** `bin_map` enables joining HPD/DOB/ECB data that only has BIN
- **Condo lot mapping:** ACRIS per-unit lots (1001-7499) mapped back to building BBL via boro+block
- **DOB lot fix:** DOB uses 5-digit lots, PLUTO uses 4-digit — use `lot[-4:]`
- **Python over SQL:** Batch processing in Python (26s) vs SQLite cross-joins (hours)

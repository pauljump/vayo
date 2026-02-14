# vayo - Current State

**Last updated:** 2026-02-14
**Sessions:** ~5+
**Readiness:** 25%

## Goal
Build a comprehensive dataset of all NYC residential buildings and units for rental intelligence.

## Status
Data collection & assembly — building the `all_nyc_units.db` dataset.

## What We Have

### Database: `all_nyc_units.db` (785 MB)
- **3,893,293 total units** across **767,302 buildings** (100% of PLUTO residential buildings)
- **912,653 real discovered units** (23.4%) from ACRIS, HPD, text mining
- **2,980,640 placeholder units** inferred from PLUTO unit counts
- Every building has: BBL, borough, address, zipcode, building class, year built, floors

### Sources Integrated
| Source | Units | Notes |
|--------|-------|-------|
| ACRIS (deeds) | 459,184 | Highest quality, real unit IDs, condo/coop classification |
| Text Mining (DOB/ECB/311) | 453,169 | Extracted from complaint/violation text |
| HPD | 300 | Only BIN-BBL matched ones; see "unfinished" below |
| PLUTO Inferred | 2,980,640 | Placeholder units based on building unit counts |

### Coverage by Borough
| Borough | Total | Real | Real % |
|---------|-------|------|--------|
| Manhattan | 1,010,061 | 331,721 | 32.8% |
| Brooklyn | 1,162,430 | 269,663 | 23.2% |
| Queens | 945,849 | 191,732 | 20.3% |
| Bronx | 590,283 | 97,111 | 16.5% |
| Staten Island | 184,670 | 22,426 | 12.1% |

### Big Source DB: `stuy-scrape-csv/stuytown.db` (38 GB)
Contains all raw data: ACRIS, HPD, PLUTO, DOB, ECB, 311, rent stabilization, etc.
- `canonical_units`: 3.29M units (the full discovered set before PLUTO matching)
- `buildings`: 571K buildings
- Plus 20+ other tables (complaints, violations, permits, listings, etc.)

## Unfinished Work (Resume Here)

### 1. BIN-to-BBL Matching (~1.9M more real units)
**This is the biggest win.** We have:
- **1,384,650 HPD units** identified by BIN only (no BBL)
- **513,066 text-mined units** with non-standard BBLs (e.g., `BROOKLYN0232100008`)
- CSV exports already created at:
  - `nycdb_data/hpd_units.csv` (1.38M rows)
  - `nycdb_data/text_units_nomatch.csv` (861K rows)
  - `nycdb_data/bin_to_bbl.csv` (225K BIN→BBL mappings)
- Script ready: `scripts/add_missing_units.py` — reads CSVs, maps to PLUTO, inserts into `all_nyc_units.db`
- **Problem:** Python process stalled when reading from 38GB DB. CSVs are now exported, so next run should work against the small DB only.
- **Expected result:** Real coverage should jump from 23% → ~55%+

### 2. Text BBL Normalization
- Text-mined BBLs like `BROOKLYN0232100008` need converting to 10-digit numeric (`3023210008`)
- The `normalize_text_bbl()` function in `add_missing_units.py` handles this
- Part of the same script above

### 3. BIN Fill (Skipped)
- Attempted to UPDATE 3.3M rows in the 38GB DB to fill missing BINs
- Ran for 51+ minutes at 100% CPU, was killed
- Not critical — BBL is the primary key for PLUTO matching

## Key Technical Decisions
- **PLUTO-anchored approach:** Start from PLUTO (ground truth for all NYC buildings), match discovered units to it, fill gaps with placeholders
- **Condo lot mapping:** ACRIS uses per-unit lot numbers (1001-7499); mapped back to building BBLs via boro+block matching (302K units recovered)
- **Separate small DB:** Operations on the 38GB `stuytown.db` are painfully slow; built `all_nyc_units.db` (785MB) for the final dataset
- **Python over SQL:** SQLite cross-joins for placeholder generation too slow; Python generates in-memory and batch-inserts (26 seconds vs hours)

## Key Files
- `all_nyc_units.db` — The output dataset (785 MB)
- `stuy-scrape-csv/stuytown.db` — Raw source data (38 GB)
- `scripts/build_complete_db.py` — Builds all_nyc_units.db from scratch
- `scripts/add_missing_units.py` — Adds BIN-only & text-BBL units (RESUME THIS)
- `scripts/07_complete_coverage.sql` — SQL version (too slow, use Python instead)
- `scripts/06_calculate_real_coverage.sql` — Coverage stats against PLUTO
- `nycdb_data/hpd_units.csv` — Exported HPD BIN-only units
- `nycdb_data/text_units_nomatch.csv` — Exported text-mined unmatched units
- `nycdb_data/bin_to_bbl.csv` — BIN→BBL mapping from buildings table

## Next Actions
1. **Run `scripts/add_missing_units.py`** — should work now with CSV exports (no 38GB reads)
2. Verify coverage jumps to ~55%+
3. Consider additional data sources for remaining gaps (mostly 1-5 unit buildings)

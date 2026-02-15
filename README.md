# Vayo

**Zillow meets Bloomberg Terminal for apartment hunting.**

Uses NYC public data to surface hidden gems and predict availability before listings go live. Scores every residential building on quality (Gem Score) and likelihood of upcoming vacancies (Availability Score) using complaint history, ACRIS transactions, DOB permits, eviction records, and more.

## Data

All data lives in `vayo_clean.db` (8.4 GB, gitignored). 14 tables, ~49M records, all keyed on BBL integer.

| Table | Rows | Source |
|-------|------|--------|
| buildings | 767,302 | PLUTO (all NYC residential) |
| acris_transactions | 1,222,855 | ACRIS (deeds, mortgages, with buyer/seller) |
| complaints | 25,038,632 | HPD |
| service_requests_311 | 15,132,059 | 311 (with lat/lon + resolution) |
| complaints_311 | 3,299,201 | 311 (compact) |
| ecb_violations | 836,477 | ECB |
| building_contacts | 713,578 | HPD registrations (agents, owners, officers) |
| hpd_litigation | 204,005 | HPD |
| dob_permits | 950,978 | DOB |
| dob_complaints | 1,438,476 | DOB |
| marshal_evictions | 38,979 | DOI |
| rent_stabilized | 40,132 | RGB (with 421-a/J-51 flags) |
| certificates_of_occupancy | 88,437 | DOB |
| bin_map | 194,209 | BIN-to-BBL cross-reference |

A secondary unit-level database `all_nyc_units.db` (785 MB) has 3.9M individual units across all buildings.

## Active Scripts

| Script | Purpose |
|--------|---------|
| `scripts/build_clean_db.py` | Builds `vayo_clean.db` from raw source data. Creates PLUTO foundation, BIN-BBL mapping, joins ACRIS/HPD/311/ECB/DOB/litigation. |
| `scripts/fix_remaining.py` | Fixes DOB complaints (BIN-BBL match), DOB permits (boro+block+lot), marshal evictions (address match). |
| `scripts/pull_remaining.py` | Pulls rent stabilization, detailed 311, and certificates of occupancy. |
| `scripts/apartment_finder.py` | Scoring engine prototype. Gem Score (quality) + Availability Score (vacancy signals). **Needs update to use vayo_clean.db.** |
| `scripts/build_complete_db.py` | Builds `all_nyc_units.db` with unit-level coverage. |
| `scripts/add_missing_units.py` | Adds BIN-only HPD units to `all_nyc_units.db` using pre-exported CSVs. |
| `scripts/diagnose_formats.sql` | Reference queries for debugging BBL format mismatches across data sources. |

## Key Technical Decisions

- **PLUTO-anchored**: Every table matched to PLUTO's 767K residential buildings via 10-digit integer BBL
- **BIN-BBL bridge**: `bin_map` table enables joining HPD/DOB/ECB data that only has BIN
- **Condo lot mapping**: ACRIS per-unit lots (1001-7499) mapped back to building BBL via boro+block
- **DOB lot fix**: DOB uses 5-digit lots, PLUTO uses 4-digit. Use `lot[-4:]`

## Product Concepts

- **Building Carfax**: Full complaint/violation/permit history report for any building
- **Landlord Score**: Aggregate owner quality across their full portfolio
- **Availability Predictor**: ACRIS estate/trust/satisfaction signals predict upcoming vacancies
- **Noise/Rat Maps**: 2M+ noise complaints and 190K rodent complaints mapped to buildings
- **Gentrification Heatmap**: Permit activity + LLC purchases + rent stab loss animated over time

## Archive

Legacy data collection scripts, scrapers, and strategy docs from earlier phases are in `archive/`. The original 38GB raw database (`stuytown.db`) has been consolidated into `vayo_clean.db` and deleted.

# vayo - Current State

**Last updated:** 2026-02-21
**Sessions:** ~14+
**Readiness:** 60%

## Goal
Build a "Zillow meets Bloomberg Terminal" apartment finder that uses public data transparency to surface hidden gems and predict availability before listings go live.

## Data Pipeline Architecture

```
Brokerage APIs (Elliman, Corcoran)     StreetEasy (Wayback + Direct)     ACRIS (NYC Public)
         ↓                                        ↓                           ↓
   elliman_mls.db (291K)                   se_listings.db               vayo_clean.db
   corcoran.db (838K)                                                  (8.4 GB, 49M records)
         ↓                                        ↓                           ↓
         └──────────────────────┬──────────────────┘                          │
                                ↓                                             │
                 listings_unified.db (15 GB)  ←───────────────────────────────┘
                 1.13M listings, 79.8M price events
                 99.9% address→BBL match rate
                                ↓
                    Web Dashboard + Analytics
```

## Unified Listings Database — `listings_unified.db` (15 GB)

**Built 2026-02-21** via `scripts/unify_listings.py`

### Contents
- **1,128,683 listings** (Elliman 291K + Corcoran 838K)
  - Sales: 546K | Rentals: 583K
  - Closed: 1.08M | Active: 20.5K | Pending: 10.9K | Expired: 15.5K
- **79,784,091 price history events**
  - Corcoran building histories: 77.2M (full transaction timelines, decades deep)
  - Corcoran listing events: 1.6M
  - StreetEasy Wayback: 686K
  - Elliman: 266K
- **106,992 distinct BBLs** linked to PLUTO

### Coverage
- Manhattan: 82% of all BBLs (26.5K / 32.5K)
- Multi-unit residential (C/D/R) NYC-wide: 67% (107K / 161K)
- Brooklyn: 21% | Queens: 8% | Bronx: 4% | SI: 2%

### Field completeness
- BBL/address/borough/zip: 99.8-100%
- Bedrooms/bathrooms: 99%
- Close price: 93.5% | List price: 99.8%
- Lat/lon: 95% | Sqft: 65% | Year built: 85.5%

### Data quality issues to fix
- Date formats inconsistent (Corcoran M/D/YYYY vs Elliman ISO)
- Junk dates exist (1900, 2121, 0001, etc.)
- Dedup found 0 cross-source matches (date format mismatch likely cause)

### How to rebuild
```bash
python3 scripts/unify_listings.py                    # full pipeline (address match → extract → dedup)
python3 scripts/unify_listings.py --phase match      # just address matching (cached, fast on re-run)
python3 scripts/unify_listings.py --phase elliman    # just elliman extract
python3 scripts/unify_listings.py --phase corcoran   # just corcoran extract
python3 scripts/unify_listings.py --phase streeteasy # just SE wayback extract
python3 scripts/unify_listings.py --phase dedup      # just dedup
python3 scripts/unify_listings.py --status           # show counts
```

## Brokerage API Pullers

### Elliman MLS — COMPLETE
- `elliman_mls.db`: **291,100 listings** (full NYC)
- API: `core.api.elliman.com`, obfuscated timestamp auth
- 300-cap partitioning by neighborhood → bedroom → price

### Corcoran — COMPLETE
- `corcoran.db`: **837,583 listings** (36 GB with detail JSON)
- API: `backendapi.corcoranlabs.com`, key `667256B5BF6ABFF6C8BDC68E88226`
- 50K-cap price splitting, detail endpoint with building histories

## StreetEasy Scraping

### Wayback Machine — COMPLETE
- 46,627 buildings with price history data
- 1.3M raw events in `se_listings.db`

### Direct Scraper — ON HOLD
- `scripts/se_fast_scrape.py` — tls-client + Chrome cookies
- 7,304 / 943,790 buildings scraped (~0.8%)
- Manhattan target file ready: `se_sitemaps/manhattan_buildings.txt` (40,743 buildings)
- **Plan**: VPS + residential proxy (IPRoyal ~$1.75/GB), ~$200-360 budget
- Need: auto cookie refresh or proxy-based auth bypass

## ACRIS (NYC Public Records)
- Master: 16.9M documents (done)
- Legals: ~14.7M (done)
- Parties: ~70% done

## What to Resume

### Next Steps
1. **StreetEasy Manhattan scrape** — VPS + proxy setup, 40.7K buildings ready
2. **Date cleanup** — normalize formats, filter junk dates, re-run dedup
3. **Web dashboard** — query the unified DB
4. **ACRIS parties** — finish the remaining 30%

## Databases
- `listings_unified.db` (15 GB) — **Unified listing + price history, main query target**
- `elliman_mls.db` (686 MB) — Elliman source (read-only, don't modify)
- `corcoran.db` (36 GB) — Corcoran source (read-only, don't modify)
- `se_listings.db` (823 MB) — StreetEasy source (read-only for unify, written by scraper)
- `vayo_clean.db` (8.4 GB) — PLUTO/ACRIS, all tables keyed on BBL

## Key Files
- `scripts/unify_listings.py` — Builds listings_unified.db from all sources
- `scripts/pull_elliman_mls.py` — Elliman MLS puller
- `scripts/pull_corcoran.py` — Corcoran puller
- `scripts/se_fast_scrape.py` — StreetEasy direct scraper
- `scripts/streeteasy_wayback_history.py` — Wayback pipeline
- `scripts/pull_acris_partitioned.py` — ACRIS puller
- `scripts/build_clean_db.py` — Builds vayo_clean.db
- `se_sitemaps/manhattan_buildings.txt` — 40.7K Manhattan scrape targets

# vayo - Current State

**Last updated:** 2026-02-16
**Sessions:** ~9+
**Readiness:** 45%

## Goal
Build a "Zillow meets Bloomberg Terminal" apartment finder that uses public data transparency to surface hidden gems and predict availability before listings go live.

## Status
ACRIS full history pull nearly complete. Legals DONE. Parties ~70% done. Built partitioned parallel puller (10x faster than sequential). StreetEasy scraping at 7,304 buildings.

## What Needs to Resume

### ACRIS Parties Pull (partitioned)
```bash
python3 scripts/pull_acris_partitioned.py --parties-only
```
- **Script:** `scripts/pull_acris_partitioned.py`
- **Method:** Partitioned by document_id range, 6 parallel workers
- **Progress:** ~70% done. Legals complete. Parties remaining:
  - `num_2005_2010`: ~4.8M / 6.2M
  - `num_2010_2015`, `num_2015_2020`, `alpha_BK`: almost done (~95%)
  - `alpha_FT_1`: ~1.75M / 2.9M
  - `alpha_FT_2`: ~0.5M / 1.8M
  - `alpha_FT_3`, `alpha_FT_4`: not started (~5.4M + 6M)
- **Cache dir:** `acris_cache/full/parties_parts/` — each partition has its own batch files
- **Fully resumable:** skips existing batch files automatically

### StreetEasy Fast Scraper
```bash
python3 scripts/se_fast_scrape.py --from-sitemap --delay 1.0
```
- **Progress:** 7,304 / 943,790 buildings (~0.8%)
- **Resumable:** skips already-scraped slugs in `se_listings.db`

## ACRIS Cache Structure

### Completed Data
| Endpoint | Location | Records | Status |
|----------|----------|---------|--------|
| Master | `acris_cache/full/master/` | 16,908,583 | COMPLETE (339 batches) |
| Legals | `acris_cache/full/legals_parts/` | 22,510,375 | COMPLETE (5 borough partitions) |
| Parties | `acris_cache/full/parties_parts/` | ~32M so far | ~70% done (11 partitions) |

### Old sequential cache (superseded by partitioned)
- `acris_cache/full/legals/` — 371 batches, 18.55M records (incomplete, replaced by legals_parts)
- `acris_cache/full/parties/` — 377 batches, 18.85M records (incomplete, replaced by parties_parts)

## Databases

### `vayo_clean.db` (8.4 GB) — Main operational database
All tables keyed on BBL integer with proper indexes. 14+ tables, ~49M records.

| Table | Rows | Notes |
|-------|------|-------|
| buildings | 767,302 | All NYC residential from PLUTO |
| acris_transactions | 1,222,855 | BBL + condo mapping + buyer/seller |
| complaints | 25,038,632 | HPD complaints via BIN→BBL |
| service_requests_311 | 15,132,059 | Detailed 311 |
| ecb_violations | 836,477 | Via BIN→BBL |
| building_contacts | 713,578 | Managing agents, owners |
| hpd_litigation | 204,005 | HPD litigation cases |
| dob_permits | 950,978 | Via boro+block+lot |
| dob_complaints | 1,438,476 | Via BIN→BBL |
| marshal_evictions | 38,979 | Address-matched |
| rent_stabilized | 40,132 | With 421-a/J-51 flags |
| certificates_of_occupancy | 88,437 | Legal occupancy changes |
| bin_map | 194,209 | BIN→BBL cross-reference |

### `se_listings.db` — StreetEasy scraped data
| Table | Description |
|-------|-------------|
| buildings | Building info: type, units, stories, year_built, neighborhood, amenities, developer, pet_policy |
| unit_summary | Units from building modal (sale/rental, available/unavailable) |
| unit_pages | Unit page metadata (pass 2 deep scrape) |
| price_history | Full unit price timeline (pass 2) |
| scrape_log | Full audit trail |

## StreetEasy Scraping — Two Methods

### Method 1: Fast Scraper (tls-client) — `scripts/se_fast_scrape.py`
- **No browser needed.** Uses `tls-client` library to impersonate Chrome's TLS fingerprint
- Extracts PX cookies from Chrome via AppleScript (`--extract-cookies`)
- Parses building info from HTML meta tags + RSC (React Server Component) data
- Captures: address, neighborhood, building_type, units, stories, year_built, amenities, developer, pet_policy, lat/lng
- **Does NOT get unit sale/rental history** (that data is behind a modal click)
- Speed: ~0.6-0.8/s with 1s delay, auto cookie refresh
- Resumable: skips already-scraped slugs in DB

### Method 2: Browser Scraper (AppleScript) — `scripts/scrape_streeteasy.py`
- Controls user's Chrome via AppleScript (needs "Allow JavaScript from Apple Events")
- **Two-pass approach:**
  - Pass 1: Building page → click "View unavailable units" → parse modal table (sales + rentals)
  - Pass 2: Individual unit pages → click "Price history" → click "Show more" → extract full timeline
- Captures: available + unavailable units, full price history per unit
- Speed: ~0.3-0.5/s (browser overhead)
- Best for: targeted deep scrape of specific buildings after fast scraper identifies them

## Key Technical Decisions
- **PLUTO-anchored:** All data matched to PLUTO's 767K residential buildings via integer BBL
- **BIN→BBL bridge table:** `bin_map` enables joining HPD/DOB/ECB data
- **Condo lot mapping:** ACRIS per-unit lots mapped back to building BBL
- **TLS fingerprinting:** `tls-client` with `chrome_120` profile bypasses PerimeterX
- **PX cookie management:** Auto-extract from Chrome, auto-refresh on 403 blocks
- **RSC parsing:** StreetEasy's Next.js RSC flight format contains structured JSON data
- **Partitioned ACRIS pull:** Split by borough/doc_id range for parallel fetch, 10x faster than sequential offset

## Unfinished Work

### 1. Complete ACRIS Parties Pull
- Run: `python3 scripts/pull_acris_partitioned.py --parties-only`
- ~30% remaining (~14M records), should take ~30-45 min

### 2. Rebuild vayo_clean.db with Full ACRIS
- When parties done: rebuild with all 16.9M master + 22.5M legals + ~46M parties
- Update `scripts/build_clean_db.py` to read from partitioned cache dirs
- Will fix the $0 sale price bug (correct field: `document_amt`)

### 3. StreetEasy Full Scrape
- Resume fast scraper on remaining ~936K buildings
- Then targeted browser scrape for unit-level price history on key buildings

### 4. Update Scoring Engine
- `scripts/apartment_finder.py` needs update for new table names
- Wire in new datasets: hpd_violations, tax_liens, vacate_orders
- Integrate StreetEasy data (market prices, listing activity)

### 5. Product Development
- Web UI, alerts, neighborhood aggregation

## Key Files
- `vayo_clean.db` — Main database
- `se_listings.db` — StreetEasy data
- `scripts/se_fast_scrape.py` — High-speed browserless SE scraper
- `scripts/scrape_streeteasy.py` — Browser-based SE scraper (unit deep scrape)
- `scripts/pull_acris_partitioned.py` — Fast partitioned ACRIS puller (NEW)
- `scripts/pull_acris_full.py` — Sequential ACRIS puller (old, slower)
- `scripts/pull_acris.py` — ACRIS 2022+ only puller (original)
- `scripts/build_clean_db.py` — Builds vayo_clean.db
- `scripts/apartment_finder.py` — Scoring engine
- `scripts/concierge.py` — Search interface
- `research/streeteasy_scraping.md` — Scraping research
- `se_sitemaps/all_buildings.txt` — 943K SE building slugs

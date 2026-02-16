# vayo - Current State

**Last updated:** 2026-02-16
**Sessions:** ~8+
**Readiness:** 45%

## Goal
Build a "Zillow meets Bloomberg Terminal" apartment finder that uses public data transparency to surface hidden gems and predict availability before listings go live.

## Status
Massive data expansion underway. Full ACRIS history pull nearly complete. StreetEasy scraping breakthrough achieved — browserless high-speed scraper working. Building the most comprehensive NYC real estate database.

## What's Running Right Now

### StreetEasy Fast Scraper (background task b8b17a4)
- **Script:** `scripts/se_fast_scrape.py --from-sitemap --delay 1.0`
- **Method:** tls-client with Chrome cookies (no browser needed)
- **Target:** 943,790 building slugs from SE sitemaps
- **Speed:** ~0.6-0.8 buildings/sec (~50K/day)
- **Auto cookie refresh:** Detects PX blocks, refreshes cookies from Chrome automatically
- **Progress stored in:** `se_listings.db` — fully resumable (skips already-scraped slugs)
- **To resume if session lost:** Just re-run the same command. Extract fresh cookies first if needed.

### ACRIS Full History Pulls (background tasks)
| Endpoint | Task ID | Progress | Status |
|----------|---------|----------|--------|
| Master | b9cd12f | **16.9M records** | **DONE** (4.8 GB cache) |
| Legals | b6e68f3 | ~14.7M+ | Running |
| Parties | beae785 | ~16.1M+ | Running |

Cache dir: `acris_cache/full/`

### How to Resume ACRIS if Session Lost
```bash
python3 scripts/pull_acris.py  # Has offset-based resume built in
```

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

### `acris_cache/full/` — Raw ACRIS JSON (will be ~15GB total)
- `master_batch_*.json` — 339 files, 16.9M records
- `legals_batch_*.json` — ~295+ files, still growing
- `parties_batch_*.json` — ~322+ files, still growing

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

### Strategy
1. Fast scraper runs through all 943K buildings (building-level data) — ~18 days
2. Browser scraper does pass 2 on high-value buildings for unit-level price history
3. ACRIS provides independent sale price verification

## Research
- `research/streeteasy_scraping.md` — Full analysis of scraping approaches

## Key Technical Decisions
- **PLUTO-anchored:** All data matched to PLUTO's 767K residential buildings via integer BBL
- **BIN→BBL bridge table:** `bin_map` enables joining HPD/DOB/ECB data
- **Condo lot mapping:** ACRIS per-unit lots mapped back to building BBL
- **TLS fingerprinting:** `tls-client` with `chrome_120` profile bypasses PerimeterX
- **PX cookie management:** Auto-extract from Chrome, auto-refresh on 403 blocks
- **RSC parsing:** StreetEasy's Next.js RSC flight format contains structured JSON data

## Unfinished Work

### 1. Complete ACRIS Full Pull + Rebuild Database
- Legals and Parties still downloading
- When done: rebuild vayo_clean.db with full history (16.9M master + legals + parties)
- Will fix the $0 sale price bug (correct field: `document_amt`)

### 2. StreetEasy Full Scrape
- Fast scraper running on 943K buildings (will take ~18 days)
- Then targeted browser scrape for unit-level price history on key buildings

### 3. Update Scoring Engine
- `scripts/apartment_finder.py` needs update for new table names
- Wire in new datasets: hpd_violations, tax_liens, vacate_orders
- Integrate StreetEasy data (market prices, listing activity)

### 4. Product Development
- Web UI, alerts, neighborhood aggregation

## Key Files
- `vayo_clean.db` — Main database
- `se_listings.db` — StreetEasy data
- `scripts/se_fast_scrape.py` — High-speed browserless SE scraper
- `scripts/scrape_streeteasy.py` — Browser-based SE scraper (unit deep scrape)
- `scripts/se_tls_probe.py` — TLS-client test/probe tool
- `scripts/pull_acris.py` — ACRIS data puller
- `scripts/build_clean_db.py` — Builds vayo_clean.db
- `scripts/apartment_finder.py` — Scoring engine
- `scripts/concierge.py` — Search interface
- `research/streeteasy_scraping.md` — Scraping research
- `se_sitemaps/all_buildings.txt` — 943K SE building slugs

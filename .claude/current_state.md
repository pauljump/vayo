# vayo - Current State

**Last updated:** 2026-02-19
**Sessions:** ~10+
**Readiness:** 45%

## Goal
Build a "Zillow meets Bloomberg Terminal" apartment finder that uses public data transparency to surface hidden gems and predict availability before listings go live.

## Status
Elliman MLS puller rebuilt with correct methodology. Gramercy proof-of-concept complete: 4,895 listings (2,237 closed rentals, 2,426 closed sales, 9 active rentals, 144 active sales, 84 pending/under-contract). Next: cross-reference StreetEasy listings against Elliman/other APIs.

## Elliman MLS Puller — `scripts/pull_elliman_mls.py`

### Methodology (validated 2026-02-19)
- **API:** `core.api.elliman.com` — Trestle/CoreLogic MLS feed, no API key needed
- **Auth:** Trivially obfuscated timestamp header (base64 + char shift)
- **Result cap:** API returns max ~300 unique results per query, then recycles
- **Partitioning strategy:**
  1. Query a bucket → paginate until no new unique IDs appear
  2. If 300 unique (capped) → split by bedroom (0-5BR)
  3. If still 300 → split by price range, recurse with tighter ranges
  4. Under 300 = complete dataset for that bucket
- **Statuses pulled:** Closed, Active, ActiveUnderContract, Pending
- **Listing types:** ResidentialLease, ResidentialSale
- **Checkpointing:** Every partition logged in `pull_log` table for resumability

### Key findings
- **Neighborhood IDs are unreliable** — most Manhattan IDs map to wrong neighborhoods. Only ~8 of 28 are correct. Brooklyn IDs don't work at all.
- **Gramercy (id=153791) verified correct** — returns E 20th, 3rd Ave, E 22nd addresses
- **homeType filter corrupts listingType** — setting homeTypes overrides the rental/sale filter. DO NOT use.
- **API recycles results** — probe at skip=4999 always returns data (recycled), making the old >5K probe unreliable
- **Borough-level pulls hit 300 cap even at $31 price ranges** — neighborhood-level filtering essential for complete data

### Verified working neighborhood IDs (Manhattan)
- Chelsea (153783), East Village (153787), Gramercy (153791)
- SoHo (153813), TriBeCa (153818), UES (153820), UWS (153821)
- Washington Heights (153822)

### Broken/wrong IDs (need remapping)
- West Village: 153823→153792, Battery Park City: 153779→153780, Flatiron: 153790→153808
- Yorkville, Alphabet City: no separate IDs (covered by UES, East Village)
- Many others scrambled (Greenwich Village ID returns Hamilton Heights, etc.)

### Usage
```bash
python3 scripts/pull_elliman_mls.py                    # pull everything, all boroughs
python3 scripts/pull_elliman_mls.py --manhattan-only   # Manhattan only
python3 scripts/pull_elliman_mls.py --rentals-only     # closed rentals only
python3 scripts/pull_elliman_mls.py --active-only      # active + under-contract + pending
python3 scripts/pull_elliman_mls.py --details          # fetch full listing details
```

## What Needs to Resume

### ACRIS Parties Pull (partitioned)
```bash
python3 scripts/pull_acris_partitioned.py --parties-only
```
- **Progress:** ~70% done. Legals complete.
- **Cache dir:** `acris_cache/full/parties_parts/`

### StreetEasy Fast Scraper
```bash
python3 scripts/se_fast_scrape.py --from-sitemap --delay 1.0
```
- **Progress:** 7,304 / 943,790 buildings (~0.8%)

## Databases

### `elliman_mls.db` — Elliman/MLS listing data
- Currently: Gramercy proof-of-concept (4,895 listings)
- Schema: listings table with 46 columns (address, price, beds, baths, sqft, agents, etc.)
- pull_log table for checkpoint/resume

### `vayo_clean.db` (8.4 GB) — Main operational database
All tables keyed on BBL integer with proper indexes. 14+ tables, ~49M records.

### `se_listings.db` — StreetEasy scraped data
Buildings, units, price history from Wayback + direct scraping.

## Next Steps
1. Cross-reference StreetEasy Gramercy listings against Elliman API and other sources
2. Fix remaining Manhattan neighborhood ID mappings
3. Pull all Manhattan neighborhoods through Elliman
4. Complete ACRIS parties pull
5. Rebuild vayo_clean.db with full ACRIS + Elliman data

## Key Files
- `elliman_mls.db` — Elliman/MLS data (NEW)
- `vayo_clean.db` — Main database
- `se_listings.db` — StreetEasy data
- `scripts/pull_elliman_mls.py` — Elliman MLS puller (REWRITTEN)
- `scripts/se_fast_scrape.py` — High-speed browserless SE scraper
- `scripts/scrape_streeteasy.py` — Browser-based SE scraper
- `scripts/pull_acris_partitioned.py` — Fast partitioned ACRIS puller
- `scripts/build_clean_db.py` — Builds vayo_clean.db
- `se_sitemaps/all_buildings.txt` — 943K SE building slugs

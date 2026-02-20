# vayo - Current State

**Last updated:** 2026-02-19
**Sessions:** ~12+
**Readiness:** 50%

## Goal
Build a "Zillow meets Bloomberg Terminal" apartment finder that uses public data transparency to surface hidden gems and predict availability before listings go live.

## Data Pipeline Architecture

```
Brokerage APIs (Elliman, Corcoran)     StreetEasy (Wayback + Direct)     ACRIS (NYC Public)
         ↓                                        ↓                           ↓
   elliman_mls.db                           se_listings.db               vayo_clean.db
   corcoran.db                                                          (8.4 GB, 49M records)
         ↓                                        ↓                           ↓
         └──────────────────────┬──────────────────┘                          │
                                ↓                                             │
                    Unified Listings DB  ←────────────────────────────────────┘
                    (address/BBL matching)
                                ↓
                    Web Dashboard + Analytics
```

## Brokerage API Pullers

### Elliman MLS — `scripts/pull_elliman_mls.py` → `elliman_mls.db`

**API:** `core.api.elliman.com` (Trestle/CoreLogic REBNY feed, no API key)
**Auth:** Obfuscated timestamp header (base64 + char shift)

**Methodology (validated 2026-02-19):**
- API returns max ~300 unique results per query, then recycles
- Partition: query → if 300 (capped) → split by bedroom → split by price → recurse
- Under 300 = complete for that bucket
- Statuses: Closed, Active, ActiveUnderContract, Pending
- Types: ResidentialLease, ResidentialSale
- Partitioned by borough, checkpointed at every level

**Current data:** Gramercy proof-of-concept (4,895 listings)
**Target:** All NYC (~unknown total, need to discover)

**Usage:**
```bash
python3 scripts/pull_elliman_mls.py                    # all NYC
python3 scripts/pull_elliman_mls.py --manhattan-only   # Manhattan only
python3 scripts/pull_elliman_mls.py --status           # show progress
```

### Corcoran — `scripts/pull_corcoran.py` → `corcoran.db`

**API:** `backendapi.corcoranlabs.com` (Realogy/Anywhere "NewTaxi" backend)
**Auth:** `be-api-key: 667256B5BF6ABFF6C8BDC68E88226` (hardcoded in their JS)

**Methodology:**
- Standard REST pagination (100/page, deterministic price+asc sort)
- Search by listingStatus: Active, Sold, Rented, Expired
- Search by transactionType: for-rent, for-sale
- Borough-level partitioning via `citiesOrBoroughs` filter
- Detail endpoint (`/api/listings/{id}`) returns 138 fields including:
  - `listingHistories`: full building transaction history (sold prices, dates, sqft)
  - `building`: 40-key dict (floors, units, year built, amenities)
  - `closeSubways`: nearby transit with distances
- All raw JSON preserved in `detail_json` column
- Concurrent detail fetching (4 workers default, configurable)

**NYC totals:**
- Active: 4,086
- Sold: 319,493
- Rented: 356,733
- Expired: 17,348
- **Total: 697,660**

**Current data:** Gramercy complete (12,826 listings + details in progress)
**Target:** All NYC (697K search + details)

**Usage:**
```bash
python3 scripts/pull_corcoran.py                       # all NYC
python3 scripts/pull_corcoran.py --manhattan-only      # Manhattan only
python3 scripts/pull_corcoran.py --details-only        # just fetch details
python3 scripts/pull_corcoran.py --details-workers 8   # faster detail fetch
python3 scripts/pull_corcoran.py --status              # show progress
```

### Other Brokerages Investigated

| Brokerage | API? | Worth building? |
|-----------|------|-----------------|
| Compass | Partial (similarhomes API, Googlebot SSR) | Yes, later — 25-40K NYC exclusive rentals via sitemap |
| C21/CB/BHGRE/ERA | Shared API, key in JS | No — ~80 NYC rentals total |
| Brown Harris Stevens | Cloudflare-locked | No direct API access |
| Nest Seekers | GraphQL schema exposed | Listings need auth |
| Sotheby's | AWS WAF locked | No access |

## StreetEasy Scraping

### Wayback Machine Pipeline — `scripts/streeteasy_wayback_history.py`
- 3-phase: CDX index → queue → fetch
- ~190K of 944K buildings covered by Wayback
- Overnight loop: `scripts/run_wayback_overnight.sh`

### Direct Scraper — `scripts/se_fast_scrape.py`
- tls-client with Chrome cookies, bypasses PerimeterX
- Progress: 7,304 / 943,790 buildings (~0.8%)
- VPS + residential proxy planned for full scrape

## ACRIS (NYC Public Records)
- Master: 16.9M documents (done)
- Legals: ~14.7M (done)
- Parties: ~70% done
- `scripts/pull_acris_partitioned.py --parties-only`

## What to Resume

### Priority 1: Brokerage Full Pulls
```bash
# Corcoran — full NYC (search ~26min, details ~10hrs with 4 workers)
python3 scripts/pull_corcoran.py

# Elliman — full NYC (slower due to 300-cap splitting)
python3 scripts/pull_elliman_mls.py
```

### Priority 2: StreetEasy Integration
- Cross-reference brokerage data with SE listings
- Build boutique brokerage catalog (tag small brokerages from SE)
- Resume direct SE scraping for complete coverage

### Priority 3: ACRIS Parties
```bash
python3 scripts/pull_acris_partitioned.py --parties-only
```

## Databases
- `elliman_mls.db` — Elliman/REBNY MLS data
- `corcoran.db` — Corcoran data (active + sold + rented + expired + details)
- `vayo_clean.db` (8.4 GB) — Main DB, all tables keyed on BBL
- `se_listings.db` — StreetEasy scraped data

## Key Files
- `scripts/pull_elliman_mls.py` — Elliman MLS puller
- `scripts/pull_corcoran.py` — Corcoran puller (NEW)
- `scripts/se_fast_scrape.py` — StreetEasy direct scraper
- `scripts/streeteasy_wayback_history.py` — Wayback pipeline
- `scripts/pull_acris_partitioned.py` — ACRIS puller
- `scripts/build_clean_db.py` — Builds vayo_clean.db

#!/usr/bin/env python3
"""
Elliman MLS Puller — pulls listing data from Douglas Elliman's core API
(core.api.elliman.com) which exposes the full Trestle/CoreLogic MLS dataset.

Pulls:
  - Closed residential leases (historical rental prices)
  - Closed residential sales (listing prices to compare vs ACRIS closing prices)
  - Active listings (current asking prices)

The API requires a trivially-generated obfuscated header (no API key or login).
Max 100 results per request, max skip 4999 per query. We use aggressive
multi-level partitioning (borough → neighborhood → bedrooms × price range)
to bypass the 5K limit and get everything.

Usage:
    python3 scripts/pull_elliman_mls.py                    # pull everything
    python3 scripts/pull_elliman_mls.py --rentals-only     # closed rentals only
    python3 scripts/pull_elliman_mls.py --sales-only       # closed sales only
    python3 scripts/pull_elliman_mls.py --active-only      # active listings only
    python3 scripts/pull_elliman_mls.py --details          # fetch full details
"""

import argparse
import base64
import json
import re
import sqlite3
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    print("pip3 install requests")
    sys.exit(1)

DB_PATH = Path(__file__).parent.parent / "elliman_mls.db"
MAX_RETRIES = 6
BATCH_SIZE = 100
MAX_SKIP = 4999
DELAY = 0.15  # seconds between requests (was 0.4)
HIT_LIMIT = 4900  # if we get this many, assume there's more beyond 5K
NUM_WORKERS = 4  # concurrent neighborhood workers

API_BASE = "https://core.api.elliman.com"

# Rate limiter — ensures minimum gap between any two API calls across all threads
_rate_lock = threading.Lock()
_last_request_time = 0.0
MIN_GAP = 0.1  # minimum seconds between any two requests globally


def tprint(msg):
    print(msg, flush=True)


def _rate_wait():
    """Enforce minimum gap between API calls across all threads."""
    global _last_request_time
    with _rate_lock:
        now = time.monotonic()
        elapsed = now - _last_request_time
        if elapsed < MIN_GAP:
            time.sleep(MIN_GAP - elapsed)
        _last_request_time = time.monotonic()


# ── Auth ────────────────────────────────────────────────────

def make_headers():
    ts = str(int(time.time() * 1000))
    b64 = base64.b64encode(ts.encode()).decode()
    shifted = ''.join(chr(ord(c) - 10) for c in b64)
    return {
        'Cookies': 'static/' + shifted,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    }


# ── Database ────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS listings (
            core_listing_id TEXT PRIMARY KEY,
            integration_listing_id TEXT,
            legacy_listing_id TEXT,
            listing_status TEXT,
            listing_type TEXT,
            home_type TEXT,
            ownership_type TEXT,
            address TEXT,
            unit TEXT,
            city TEXT,
            state TEXT,
            zip TEXT,
            neighborhood TEXT,
            borough TEXT,
            latitude REAL,
            longitude REAL,
            list_price REAL,
            close_price REAL,
            price_per_sqft REAL,
            association_fee REAL,
            maintenance_expense REAL,
            tax_annual REAL,
            list_date TEXT,
            close_date TEXT,
            move_in_date TEXT,
            update_date TEXT,
            bedrooms INTEGER,
            bathrooms_full INTEGER,
            bathrooms_half INTEGER,
            bathrooms_total REAL,
            living_area_sqft REAL,
            lot_size_sqft REAL,
            year_built INTEGER,
            stories INTEGER,
            num_units INTEGER,
            building_name TEXT,
            pre_war INTEGER,
            listing_agent TEXT,
            listing_agent_email TEXT,
            listing_agent_phone TEXT,
            listing_brokerage TEXT,
            buyer_agent TEXT,
            buyer_brokerage TEXT,
            commission TEXT,
            source_mls TEXT,
            is_de_listing INTEGER,
            public_remarks TEXT,
            features TEXT,
            images TEXT,
            virtual_tour_url TEXT,
            fetched_at TEXT,
            detail_fetched INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS pull_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT DEFAULT (datetime('now')),
            query_type TEXT,
            partition TEXT,
            results_count INTEGER,
            new_count INTEGER
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_pull_log_key
            ON pull_log(query_type, partition);

        CREATE INDEX IF NOT EXISTS idx_listings_status ON listings(listing_status);
        CREATE INDEX IF NOT EXISTS idx_listings_type ON listings(listing_type);
        CREATE INDEX IF NOT EXISTS idx_listings_zip ON listings(zip);
        CREATE INDEX IF NOT EXISTS idx_listings_borough ON listings(borough);
        CREATE INDEX IF NOT EXISTS idx_listings_close_date ON listings(close_date);
        CREATE INDEX IF NOT EXISTS idx_listings_address ON listings(address);
        CREATE INDEX IF NOT EXISTS idx_listings_integration_id ON listings(integration_listing_id);
    """)
    conn.commit()
    return conn


def thread_conn():
    """Create a new DB connection for use in a worker thread."""
    c = sqlite3.connect(str(DB_PATH))
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    return c


# ── API ─────────────────────────────────────────────────────

def api_post(endpoint, payload, label=""):
    url = f"{API_BASE}{endpoint}"
    for attempt in range(MAX_RETRIES):
        _rate_wait()
        try:
            resp = requests.post(url, headers=make_headers(), json=payload, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code in (429, 504):
                wait = min(20 * (attempt + 1), 120)
                tprint(f"    [{label}] HTTP {resp.status_code}, wait {wait}s...")
                time.sleep(wait)
                continue
            else:
                tprint(f"    [{label}] HTTP {resp.status_code}, retry {attempt+1}")
        except requests.RequestException as e:
            tprint(f"    [{label}] error: {e}, retry {attempt+1}")
        time.sleep(5 * (attempt + 1))
    return None


def api_get(endpoint, params=None, label=""):
    url = f"{API_BASE}{endpoint}"
    for attempt in range(MAX_RETRIES):
        _rate_wait()
        try:
            resp = requests.get(url, headers=make_headers(), params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code in (429, 504):
                wait = min(20 * (attempt + 1), 120)
                time.sleep(wait)
                continue
        except requests.RequestException:
            pass
        time.sleep(5 * (attempt + 1))
    return None


# ── Place definitions ───────────────────────────────────────

NYC_BOROUGHS = [
    {"name": "Manhattan", "id": 1915, "urlKey": "manhattan-ny"},
    {"name": "Brooklyn", "id": 1908, "urlKey": "brooklyn-ny"},
    {"name": "Queens", "id": 1925, "urlKey": "queens-ny"},
    {"name": "Bronx", "id": 337218, "urlKey": "bronx--county-ny"},
    {"name": "Staten Island", "id": 1927, "urlKey": "staten-island-ny"},
]

MANHATTAN_HOODS = [
    {"name": "Upper East Side", "id": 153820, "urlKey": "upper-east-side-new-york-ny"},
    {"name": "Upper West Side", "id": 153821, "urlKey": "upper-west-side-new-york-ny"},
    {"name": "Midtown", "id": 153803, "urlKey": "midtown-new-york-ny"},
    {"name": "Midtown East", "id": 153804, "urlKey": "midtown-east-new-york-ny"},
    {"name": "Midtown West", "id": 322685, "urlKey": "midtown-west-new-york-ny"},
    {"name": "Chelsea", "id": 153783, "urlKey": "chelsea-new-york-ny"},
    # Priority neighborhoods
    {"name": "Gramercy", "id": 153791, "urlKey": "gramercy-new-york-ny"},
    {"name": "Greenwich Village", "id": 153793, "urlKey": "greenwich-village-new-york-ny"},
    {"name": "West Village", "id": 153823, "urlKey": "west-village-new-york-ny"},
    {"name": "East Village", "id": 153787, "urlKey": "east-village-new-york-ny"},
    {"name": "Stuyvesant Town", "id": 153815, "urlKey": "stuyvesant-town-new-york-ny"},
    {"name": "Flatiron", "id": 153790, "urlKey": "flatiron-new-york-ny"},
    # Remaining
    {"name": "SoHo", "id": 153813, "urlKey": "soho-new-york-ny"},
    {"name": "TriBeCa", "id": 153818, "urlKey": "tribeca-new-york-ny"},
    {"name": "Murray Hill", "id": 153806, "urlKey": "murray-hill-new-york-ny"},
    {"name": "Kips Bay", "id": 153799, "urlKey": "kips-bay-new-york-ny"},
    {"name": "Financial District", "id": 153789, "urlKey": "financial-district-new-york-ny"},
    {"name": "Battery Park City", "id": 153779, "urlKey": "battery-park-city-new-york-ny"},
    {"name": "Hell's Kitchen", "id": 153797, "urlKey": "hells-kitchen-new-york-ny"},
    {"name": "Harlem", "id": 153795, "urlKey": "harlem-new-york-ny"},
    {"name": "Lower East Side", "id": 153801, "urlKey": "lower-east-side-new-york-ny"},
    {"name": "Morningside Heights", "id": 153805, "urlKey": "morningside-heights-new-york-ny"},
    {"name": "Washington Heights", "id": 153822, "urlKey": "washington-heights-new-york-ny"},
    {"name": "Inwood", "id": 153798, "urlKey": "inwood-new-york-ny"},
    {"name": "Nolita", "id": 153808, "urlKey": "nolita-new-york-ny"},
    {"name": "NoHo", "id": 153807, "urlKey": "noho-new-york-ny"},
    {"name": "Sutton Place", "id": 153816, "urlKey": "sutton-place-new-york-ny"},
    {"name": "Yorkville", "id": 153824, "urlKey": "yorkville-new-york-ny"},
    {"name": "Turtle Bay", "id": 153819, "urlKey": "turtle-bay-new-york-ny"},
    {"name": "Alphabet City", "id": 153778, "urlKey": "alphabet-city-new-york-ny"},
    {"name": "Little Italy", "id": 153800, "urlKey": "little-italy-new-york-ny"},
    {"name": "Chinatown", "id": 153784, "urlKey": "chinatown-new-york-ny"},
]

BROOKLYN_HOODS = [
    {"name": "Brooklyn Heights", "id": 153728, "urlKey": "brooklyn-heights-brooklyn-ny"},
    {"name": "Williamsburg", "id": 153768, "urlKey": "williamsburg-brooklyn-ny"},
    {"name": "Park Slope", "id": 153752, "urlKey": "park-slope-brooklyn-ny"},
    {"name": "DUMBO", "id": 153734, "urlKey": "dumbo-brooklyn-ny"},
    {"name": "Bushwick", "id": 153730, "urlKey": "bushwick-brooklyn-ny"},
    {"name": "Bed-Stuy", "id": 153725, "urlKey": "bed-stuy-brooklyn-ny"},
    {"name": "Crown Heights", "id": 153733, "urlKey": "crown-heights-brooklyn-ny"},
    {"name": "Greenpoint", "id": 153740, "urlKey": "greenpoint-brooklyn-ny"},
    {"name": "Fort Greene", "id": 153737, "urlKey": "fort-greene-brooklyn-ny"},
    {"name": "Prospect Heights", "id": 153754, "urlKey": "prospect-heights-brooklyn-ny"},
    {"name": "Cobble Hill", "id": 153731, "urlKey": "cobble-hill-brooklyn-ny"},
    {"name": "Carroll Gardens", "id": 153729, "urlKey": "carroll-gardens-brooklyn-ny"},
    {"name": "Boerum Hill", "id": 153727, "urlKey": "boerum-hill-brooklyn-ny"},
    {"name": "Downtown Brooklyn", "id": 322690, "urlKey": "downtown-brooklyn-brooklyn-ny"},
    {"name": "Sunset Park", "id": 153762, "urlKey": "sunset-park-brooklyn-ny"},
    {"name": "Bay Ridge", "id": 153724, "urlKey": "bay-ridge-brooklyn-ny"},
    {"name": "Flatbush", "id": 153736, "urlKey": "flatbush-brooklyn-ny"},
    {"name": "Clinton Hill", "id": 153732, "urlKey": "clinton-hill-brooklyn-ny"},
    {"name": "Prospect Lefferts Gardens", "id": 153755, "urlKey": "prospect-lefferts-gardens-brooklyn-ny"},
    {"name": "Red Hook", "id": 153757, "urlKey": "red-hook-brooklyn-ny"},
    {"name": "Windsor Terrace", "id": 153769, "urlKey": "windsor-terrace-brooklyn-ny"},
    {"name": "Gowanus", "id": 153739, "urlKey": "gowanus-brooklyn-ny"},
    {"name": "Bensonhurst", "id": 153726, "urlKey": "bensonhurst-brooklyn-ny"},
    {"name": "Sheepshead Bay", "id": 153759, "urlKey": "sheepshead-bay-brooklyn-ny"},
]

# Neighborhood-level IDs are unreliable (most map to wrong areas).
# All boroughs use borough-level bedroom+price partitioning instead.
BOROUGH_HOODS = {}

# ── Price range partitions for sub-splitting ────────────────

RENTAL_PRICE_RANGES = [
    (None, 1000),
    (1000, 1500),
    (1500, 2000),
    (2000, 2500),
    (2500, 3000),
    (3000, 3500),
    (3500, 4000),
    (4000, 4500),
    (4500, 5000),
    (5000, 6000),
    (6000, 7000),
    (7000, 8500),
    (8500, 10000),
    (10000, 15000),
    (15000, 25000),
    (25000, None),
]

SALE_PRICE_RANGES = [
    (None, 500000),
    (500000, 1000000),
    (1000000, 2000000),
    (2000000, 4000000),
    (4000000, 8000000),
    (8000000, None),
]

RESULT_CAP = 300  # API recycles results after this many unique IDs
BEDROOM_VALUES = [0, 1, 2, 3, 4, 5]  # 5 = 5+


# ── Extraction ──────────────────────────────────────────────

def extract_listing(item):
    addr = item.get("address", {}) or {}
    agents = item.get("agents", []) or []
    listing_agent = agents[0] if agents else {}
    buyer_agents = item.get("buyerAgents", []) or []
    buyer_agent = buyer_agents[0] if buyer_agents else {}
    latlng = item.get("latLng", {}) or {}
    mls_info = item.get("mlsInfo", {}) or {}

    full_address = addr.get("samlsFullAddress") or addr.get("samlsPartialAddress")
    unit = addr.get("unitNumber")
    zip_code = addr.get("postalCode")
    neighborhood = addr.get("neighborhood") or item.get("neighborhood")

    if not zip_code and full_address:
        m = re.search(r'(\d{5})(?:-\d{4})?$', full_address.strip())
        if m:
            zip_code = m.group(1)

    borough = None
    city = addr.get("city", "")
    state = addr.get("stateOrProvince", "")
    if state == "NY":
        cl = (city or "").lower()
        if cl in ("new york", "manhattan"):
            borough = "Manhattan"
        elif cl == "brooklyn":
            borough = "Brooklyn"
        elif cl == "queens":
            borough = "Queens"
        elif cl in ("bronx", "the bronx"):
            borough = "Bronx"
        elif cl == "staten island":
            borough = "Staten Island"

    features = item.get("features", []) or []

    return {
        "core_listing_id": str(item.get("coreListingId", "")),
        "integration_listing_id": item.get("integrationListingId") or mls_info.get("mlsNumber"),
        "legacy_listing_id": item.get("legacyListingId"),
        "listing_status": item.get("listingStatus"),
        "listing_type": item.get("listingType"),
        "home_type": item.get("homeType"),
        "ownership_type": item.get("ownershipType"),
        "address": full_address,
        "unit": unit,
        "city": city,
        "state": state,
        "zip": zip_code,
        "neighborhood": neighborhood,
        "borough": borough,
        "latitude": latlng.get("lat"),
        "longitude": latlng.get("lng"),
        "list_price": item.get("listPrice"),
        "close_price": item.get("closePrice"),
        "price_per_sqft": item.get("pricePerSqFeet"),
        "association_fee": item.get("associationFee"),
        "maintenance_expense": item.get("maintenanceExpense"),
        "tax_annual": item.get("taxAnnualAmount"),
        "list_date": item.get("listDate"),
        "close_date": item.get("closeDate"),
        "move_in_date": item.get("moveInDate"),
        "update_date": item.get("updateDate"),
        "bedrooms": item.get("bedroomsTotal"),
        "bathrooms_full": item.get("bathroomsFull"),
        "bathrooms_half": item.get("bathroomsHalf"),
        "bathrooms_total": item.get("bathroomsTotal"),
        "living_area_sqft": item.get("livingAreaSquareFeet"),
        "lot_size_sqft": item.get("lotSizeSquareFeet"),
        "year_built": item.get("yearBuilt"),
        "stories": item.get("stories"),
        "num_units": item.get("numberOfUnits"),
        "building_name": item.get("buildingName"),
        "pre_war": 1 if item.get("preWar") else 0,
        "listing_agent": listing_agent.get("name"),
        "listing_agent_email": listing_agent.get("email"),
        "listing_agent_phone": listing_agent.get("phone"),
        "listing_brokerage": listing_agent.get("brokerageName"),
        "buyer_agent": buyer_agent.get("name") if buyer_agent else None,
        "buyer_brokerage": buyer_agent.get("brokerageName") if buyer_agent else None,
        "commission": item.get("buyerSideCommission"),
        "source_mls": item.get("sourceMlsId"),
        "is_de_listing": 1 if item.get("isDEListing") else 0,
        "public_remarks": item.get("publicRemarks"),
        "features": json.dumps(features) if features else None,
        "images": None,
        "virtual_tour_url": item.get("virtualTourURL"),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "detail_fetched": 0,
    }


def upsert_listings(conn, listings):
    if not listings:
        return 0
    cols = list(listings[0].keys())
    placeholders = ",".join(["?"] * len(cols))
    col_names = ",".join(cols)
    updates = ",".join([f"{c}=excluded.{c}" for c in cols if c != "core_listing_id"])
    conn.executemany(
        f"INSERT INTO listings ({col_names}) VALUES ({placeholders}) "
        f"ON CONFLICT(core_listing_id) DO UPDATE SET {updates}",
        [tuple(l[c] for c in cols) for l in listings]
    )
    conn.commit()
    return len(listings)


# ── Core query engine ───────────────────────────────────────

def build_filter(status, listing_type, place=None, bedrooms=None,
                 price_min=None, price_max=None, order_by="Newest",
                 home_type=None):
    """Build the full filter payload."""
    f = {
        "styles": None,
        "statuses": [status],
        "features": None,
        "homeTypes": [home_type] if home_type else None,
        "timeOnMls": None,
        "isAgencyOnly": False,
        "isPetAllowed": False,
        "hasOpenHouse": False,
        "rentalPeriods": None,
        "bedroomsTotal": [bedrooms] if bedrooms is not None else None,
        "isPriceReduced": False,
        "hasVirtualTour": False,
        "isNewConstruction": False,
        "onlyInternationalListings": False,
        "listingTypes": [listing_type],
        "checkedStatuses": [],
        "bathroom": {"queryField": "TotalDecimal", "operator": "Ge", "value": None},
        "listPrice": {"min": price_min, "max": price_max},
        "yearBuilt": {"min": None, "max": None},
        "lotSizeSquareFeet": {"min": None, "max": None},
        "livingAreaSquareFeet": {"min": None, "max": None},
        "orderBy": order_by,
        "parkingTotal": {"min": None, "max": None},
        "schoolFilter": {"score": None, "isPrivate": None},
        "moveIn": {"date": None, "skipNulls": None},
        "skip": 0,
        "take": BATCH_SIZE,
        "places": [],
    }
    if place:
        f["places"] = [place]
    return f


def paginate_unique(status, listing_type, place=None, bedrooms=None,
                    price_min=None, price_max=None):
    """Paginate a query, return set of unique (coreListingId, raw_item) pairs.
    Stops when the API starts recycling IDs (after ~300 unique)."""
    seen = {}  # id -> raw item
    for skip in range(0, MAX_SKIP + 1, BATCH_SIZE):
        filt = build_filter(status, listing_type, place, bedrooms, price_min, price_max)
        filt["skip"] = skip
        payload = {"filter": filt, "map": {"zoomLevel": 11, "geometry": None}}
        data = api_post("/listing/filter", payload, label=f"@{skip}")
        if not data:
            break
        items = data.get("listings", [])
        if not items:
            break
        new_count = 0
        for item in items:
            cid = item.get("coreListingId")
            if cid not in seen:
                seen[cid] = item
                new_count += 1
        if new_count == 0:
            break  # API is recycling
        time.sleep(DELAY)
    return seen


def make_place(p):
    return {"id": p["id"], "urlKey": p["urlKey"], "name": p["name"], "shapeId": None}


def is_done(conn, query_type, partition):
    """Check if a partition was already completed in a previous run."""
    row = conn.execute(
        "SELECT results_count FROM pull_log WHERE query_type=? AND partition=?",
        (query_type, partition)
    ).fetchone()
    return row is not None


def mark_done(conn, query_type, partition, results_count):
    """Record a completed partition."""
    conn.execute(
        "INSERT OR REPLACE INTO pull_log (query_type, partition, results_count) VALUES (?, ?, ?)",
        (query_type, partition, results_count)
    )
    conn.commit()


def exhaustive_pull(conn, status, listing_type, place, label, is_rental=True,
                    category_label=""):
    """Pull all listings for a place. Automatically splits by bedroom then
    price when the API's ~300 result cap is hit."""

    if is_done(conn, category_label, label):
        tprint(f"  [{label}] SKIP (already completed)")
        return 0

    # Try unsplit first
    results = paginate_unique(status, listing_type, place=place)

    if len(results) < RESULT_CAP:
        listings = [extract_listing(it) for it in results.values()]
        upsert_listings(conn, listings)
        tprint(f"  [{label}] {len(results)}")
        mark_done(conn, category_label, label, len(results))
        return len(results)

    # Hit cap — split by bedrooms
    tprint(f"  [{label}] {len(results)} (capped), splitting by bedroom...")
    all_items = {}

    for beds in BEDROOM_VALUES:
        bed_label = f"{label}/{beds}BR"

        if is_done(conn, category_label, bed_label):
            tprint(f"    [{bed_label}] SKIP (done)")
            continue

        bed_results = paginate_unique(status, listing_type, place=place, bedrooms=beds)

        if len(bed_results) < RESULT_CAP:
            all_items.update(bed_results)
            listings = [extract_listing(it) for it in bed_results.values()]
            upsert_listings(conn, listings)
            tprint(f"    [{bed_label}] {len(bed_results)}")
            mark_done(conn, category_label, bed_label, len(bed_results))
        else:
            # Hit cap — split by price
            tprint(f"    [{bed_label}] {len(bed_results)} (capped), splitting by price...")
            price_ranges = _initial_price_ranges(is_rental)
            bed_items = _price_split_pull(
                conn, status, listing_type, place, beds, price_ranges,
                bed_label, is_rental, category_label
            )
            all_items.update(bed_items)
            mark_done(conn, category_label, bed_label, len(bed_items))

    # Also upsert the initial unsplit results (may have IDs not in any bedroom bucket)
    all_items.update(results)
    listings = [extract_listing(it) for it in results.values()]
    upsert_listings(conn, listings)

    total = len(all_items)
    mark_done(conn, category_label, label, total)
    tprint(f"  [{label}] TOTAL: {total}")
    return total


def _initial_price_ranges(is_rental):
    if is_rental:
        return [(None, 1500), (1500, 2000), (2000, 2500), (2500, 3000),
                (3000, 3500), (3500, 4000), (4000, 5000), (5000, 7000),
                (7000, 10000), (10000, 15000), (15000, None)]
    else:
        return [(None, 500000), (500000, 750000), (750000, 1000000),
                (1000000, 1500000), (1500000, 2000000), (2000000, 3000000),
                (3000000, 5000000), (5000000, 10000000), (10000000, None)]


def _price_split_pull(conn, status, listing_type, place, beds, price_ranges,
                      parent_label, is_rental, category_label, depth=0):
    """Recursively split price ranges until each bucket is under the cap."""
    all_items = {}
    indent = "      " + "  " * depth

    for pmin, pmax in price_ranges:
        price_label = f"{parent_label}/${pmin or 0}-{pmax or 'max'}"

        if is_done(conn, category_label, price_label):
            tprint(f"{indent}[{price_label}] SKIP (done)")
            continue

        results = paginate_unique(status, listing_type, place=place,
                                  bedrooms=beds, price_min=pmin, price_max=pmax)

        if len(results) < RESULT_CAP:
            # Got everything
            listings = [extract_listing(it) for it in results.values()]
            upsert_listings(conn, listings)
            all_items.update(results)
            tprint(f"{indent}[{price_label}] {len(results)}")
            mark_done(conn, category_label, price_label, len(results))
        else:
            # Still capped — split further
            lo = pmin or 0
            hi = pmax
            if hi is None:
                hi = lo * 3 if lo > 0 else (50000 if is_rental else 20000000)
            if hi - lo >= (50 if is_rental else 10000):
                mid = (lo + hi) // 2
                tprint(f"{indent}[{price_label}] {len(results)} (capped), split → {lo}-{mid} / {mid}-{pmax or 'max'}")
                sub = _price_split_pull(
                    conn, status, listing_type, place, beds,
                    [(pmin, mid), (mid, pmax)],
                    parent_label, is_rental, category_label, depth + 1
                )
                all_items.update(sub)
                # Also save the capped results (may contain IDs not in sub-splits)
                listings = [extract_listing(it) for it in results.values()]
                upsert_listings(conn, listings)
                all_items.update(results)
            else:
                # Can't split further — save what we have
                listings = [extract_listing(it) for it in results.values()]
                upsert_listings(conn, listings)
                all_items.update(results)
                tprint(f"{indent}[{price_label}] {len(results)} (capped, min range)")
            mark_done(conn, category_label, price_label, len(results))

    return all_items


# ── Main pull orchestration ─────────────────────────────────

def pull_category(conn, status, listing_type, category_label, borough_filter=None):
    is_rental = "Lease" in listing_type
    tprint(f"\n{'='*60}")
    tprint(f"  {category_label} ({status} / {listing_type})")
    tprint(f"{'='*60}")

    count_before = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    grand_total = 0

    for borough in NYC_BOROUGHS:
        if borough_filter and borough["name"] != borough_filter:
            continue
        place = make_place(borough)
        borough_label = f"{category_label}/{borough['name']}"

        borough_count = exhaustive_pull(conn, status, listing_type, place,
                                        borough_label, is_rental,
                                        category_label=category_label)
        grand_total += borough_count

    count_after = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    new_records = count_after - count_before

    tprint(f"\n  [{category_label}] DONE — {new_records:,} new unique records")
    return new_records


# ── Detail fetcher ──────────────────────────────────────────

def fetch_details(conn, limit=None):
    query = "SELECT core_listing_id FROM listings WHERE detail_fetched = 0"
    if limit:
        query += f" LIMIT {limit}"
    ids = [r[0] for r in conn.execute(query).fetchall()]

    if not ids:
        tprint("No listings need detail fetching.")
        return 0

    tprint(f"\nFetching details for {len(ids)} listings...")
    fetched = 0

    for i, cid in enumerate(ids):
        data = api_get("/listing/details", params={"coreListingId": cid},
                       label=f"detail {i+1}/{len(ids)}")
        if not data:
            continue

        updates = {"detail_fetched": 1}
        addr = data.get("address", {}) or {}
        if addr.get("neighborhood"):
            updates["neighborhood"] = addr["neighborhood"]
        if addr.get("postalCode"):
            updates["zip"] = addr["postalCode"]
        lat = (data.get("latLng") or {}).get("lat")
        lng = (data.get("latLng") or {}).get("lng")
        if lat:
            updates["latitude"] = lat
        if lng:
            updates["longitude"] = lng
        if data.get("yearBuilt"):
            updates["year_built"] = data["yearBuilt"]
        if data.get("buildingName"):
            updates["building_name"] = data["buildingName"]
        if data.get("stories"):
            updates["stories"] = data["stories"]
        if data.get("numberOfUnits"):
            updates["num_units"] = data["numberOfUnits"]
        if data.get("publicRemarks"):
            updates["public_remarks"] = data["publicRemarks"]
        if data.get("ownershipType"):
            updates["ownership_type"] = data["ownershipType"]
        if data.get("pricePerSqFeet"):
            updates["price_per_sqft"] = data["pricePerSqFeet"]
        if data.get("associationFee"):
            updates["association_fee"] = data["associationFee"]
        if data.get("taxAnnualAmount"):
            updates["tax_annual"] = data["taxAnnualAmount"]

        images = data.get("images", []) or []
        if images:
            updates["images"] = json.dumps([img.get("url") for img in images[:10]])

        set_clause = ", ".join([f"{k}=?" for k in updates.keys()])
        conn.execute(
            f"UPDATE listings SET {set_clause} WHERE core_listing_id=?",
            list(updates.values()) + [cid]
        )
        conn.commit()
        fetched += 1

        if (i + 1) % 100 == 0:
            tprint(f"  Details: {i+1}/{len(ids)}")
        time.sleep(DELAY)

    tprint(f"  Details fetched: {fetched}/{len(ids)}")
    return fetched


def backfill_neighborhoods(conn):
    """Backfill neighborhood names for existing listings by re-querying each
    neighborhood and matching listing IDs."""
    tprint("Backfilling neighborhood names...")
    all_hoods = []
    for borough_name, hoods in BOROUGH_HOODS.items():
        for hood in hoods:
            all_hoods.append(hood)

    total_updated = 0
    for hood in all_hoods:
        hood_name = hood["name"]
        place = make_place(hood)

        # Query all listing IDs for this neighborhood (paginate through)
        skip = 0
        ids = []
        while True:
            # Query both closed and active, rentals and sales
            for status, ltype in [("Closed", "ResidentialLease"), ("Closed", "ResidentialSale"),
                                  ("Active", "ResidentialLease"), ("Active", "ResidentialSale")]:
                filt = build_filter(status, ltype, place=place)
                filt["skip"] = skip
                filt["take"] = BATCH_SIZE
                payload = {"filter": filt, "map": {"zoomLevel": 11, "geometry": None}}
                data = api_post("/listing/filter", payload, label=f"backfill/{hood_name}")
                if data:
                    for item in data.get("listings", []):
                        cid = str(item.get("coreListingId", ""))
                        if cid:
                            ids.append(cid)

            # For simplicity, just do one page per status/type combo
            # (we'll catch most listings this way)
            break

        if ids:
            # Batch update
            for i in range(0, len(ids), 500):
                batch = ids[i:i+500]
                placeholders = ",".join(["?"] * len(batch))
                conn.execute(
                    f"UPDATE listings SET neighborhood=? WHERE core_listing_id IN ({placeholders}) "
                    f"AND (neighborhood IS NULL OR neighborhood = '')",
                    [hood_name] + batch
                )
            conn.commit()
            updated = conn.execute(
                "SELECT changes()").fetchone()[0]
            total_updated += len(ids)
            tprint(f"  [{hood_name}] tagged {len(ids)} listings")
        else:
            tprint(f"  [{hood_name}] no listings found")

        time.sleep(DELAY)

    tprint(f"\nBackfill complete. Tagged {total_updated} listings.")


# ── Main ────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Pull Elliman MLS listing data")
    parser.add_argument("--rentals-only", action="store_true")
    parser.add_argument("--sales-only", action="store_true")
    parser.add_argument("--active-only", action="store_true")
    parser.add_argument("--details", action="store_true")
    parser.add_argument("--details-limit", type=int)
    parser.add_argument("--manhattan-only", action="store_true",
                        help="Only pull Manhattan listings")
    parser.add_argument("--backfill-hoods", action="store_true",
                        help="Backfill neighborhood names for existing listings")
    args = parser.parse_args()

    conn = init_db()

    tprint("Testing API connectivity...")
    test = api_get("/listing/search-options", label="test")
    if not test:
        tprint("ERROR: Cannot reach core.api.elliman.com")
        sys.exit(1)
    tprint("API is reachable.\n")

    if args.backfill_hoods:
        backfill_neighborhoods(conn)
        conn.close()
        return

    if args.details:
        fetch_details(conn, limit=args.details_limit)
        conn.close()
        return

    pull_all = not (args.rentals_only or args.sales_only or args.active_only)
    borough_filter = "Manhattan" if args.manhattan_only else None
    totals = {}

    if pull_all or args.rentals_only:
        totals["closed_rentals"] = pull_category(
            conn, "Closed", "ResidentialLease", "Closed Rentals", borough_filter)

    if pull_all or args.sales_only:
        totals["closed_sales"] = pull_category(
            conn, "Closed", "ResidentialSale", "Closed Sales", borough_filter)

    if pull_all or args.active_only:
        totals["active_rentals"] = pull_category(
            conn, "Active", "ResidentialLease", "Active Rentals", borough_filter)
        totals["active_sales"] = pull_category(
            conn, "Active", "ResidentialSale", "Active Sales", borough_filter)
        totals["under_contract_rentals"] = pull_category(
            conn, "ActiveUnderContract", "ResidentialLease", "UnderContract Rentals", borough_filter)
        totals["under_contract_sales"] = pull_category(
            conn, "ActiveUnderContract", "ResidentialSale", "UnderContract Sales", borough_filter)
        totals["pending_rentals"] = pull_category(
            conn, "Pending", "ResidentialLease", "Pending Rentals", borough_filter)
        totals["pending_sales"] = pull_category(
            conn, "Pending", "ResidentialSale", "Pending Sales", borough_filter)

    tprint(f"\n{'='*60}")
    tprint("PULL COMPLETE")
    tprint(f"{'='*60}")
    for cat, count in totals.items():
        tprint(f"  {cat}: {count:,} new unique listings")

    total_db = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    tprint(f"\n  Total in DB: {total_db:,}")
    tprint(f"  DB location: {DB_PATH}")
    conn.close()


if __name__ == "__main__":
    main()

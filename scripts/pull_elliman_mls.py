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

BOROUGH_HOODS = {
    "Manhattan": MANHATTAN_HOODS,
    "Brooklyn": BROOKLYN_HOODS,
}

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
                 price_min=None, price_max=None, order_by="Newest"):
    """Build the full filter payload."""
    f = {
        "styles": None,
        "statuses": [status],
        "features": None,
        "homeTypes": None,
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


def paginate_query(conn, status, listing_type, place=None, bedrooms=None,
                   price_min=None, price_max=None, label="", order_by="Newest"):
    """Paginate through a single query, return count of results fetched."""
    total = 0
    skip = 0

    while skip <= MAX_SKIP:
        filt = build_filter(status, listing_type, place, bedrooms, price_min, price_max, order_by)
        filt["skip"] = skip

        payload = {"filter": filt, "map": {"zoomLevel": 11, "geometry": None}}
        data = api_post("/listing/filter", payload, label=f"{label} @{skip}")

        if not data:
            tprint(f"    [{label}] FAILED at skip={skip}")
            break

        items = data.get("listings", [])
        if not items:
            break

        listings = [extract_listing(item) for item in items]
        upsert_listings(conn, listings)
        total += len(items)

        if len(items) < BATCH_SIZE:
            break

        skip += BATCH_SIZE
        time.sleep(DELAY)

    return total


def probe_exceeds_limit(status, listing_type, place=None, bedrooms=None,
                        price_min=None, price_max=None):
    """Single request to check if a query has more than 5K results."""
    filt = build_filter(status, listing_type, place, bedrooms, price_min, price_max)
    filt["skip"] = MAX_SKIP
    filt["take"] = 1
    payload = {"filter": filt, "map": {"zoomLevel": 11, "geometry": None}}
    data = api_post("/listing/filter", payload, label="probe")
    if not data:
        return False
    items = data.get("listings", [])
    time.sleep(DELAY)
    return len(items) > 0


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
    """
    Pull all listings for a place, with automatic sub-partitioning.
    Uses probe-first to skip redundant full fetches of dense buckets.
    Checkpoints each bedroom partition so restarts skip completed work.
    """
    # Check if this entire neighborhood is already done
    if is_done(conn, category_label, label):
        tprint(f"  [{label}] SKIP (already completed)")
        return 0

    # Probe: does this neighborhood exceed 5K?
    exceeds = probe_exceeds_limit(status, listing_type, place=place)

    if not exceeds:
        # Under 5K — just paginate the whole thing
        count = paginate_query(conn, status, listing_type, place=place, label=label)
        tprint(f"  [{label}] : {count}")
        mark_done(conn, category_label, label, count)
        return count

    # Over 5K — skip straight to bedroom partitioning (don't waste 50 requests on L1)
    tprint(f"  [{label}] >5K, splitting by bedrooms...")
    total = 0

    for beds in BEDROOM_VALUES:
        bed_label = f"{label}/{beds}BR"

        if is_done(conn, category_label, bed_label):
            tprint(f"    [{bed_label}] SKIP (already completed)")
            continue

        # Probe this bedroom
        bed_exceeds = probe_exceeds_limit(status, listing_type, place=place, bedrooms=beds)

        if not bed_exceeds:
            # Under 5K — paginate normally
            bed_count = paginate_query(conn, status, listing_type, place=place,
                                       bedrooms=beds, label=bed_label)
            tprint(f"    [{bed_label}] : {bed_count}")
        else:
            # Over 5K — skip straight to price splitting
            tprint(f"    [{bed_label}] >5K, splitting by price...")
            if is_rental:
                initial_ranges = [(None, 1500), (1500, 3000), (3000, 5000), (5000, 10000), (10000, None)]
            else:
                initial_ranges = [(None, 500000), (500000, 1000000), (1000000, 2000000),
                                  (2000000, 5000000), (5000000, None)]

            bed_count = _recursive_price_pull(
                conn, status, listing_type, place, beds, initial_ranges,
                bed_label, is_rental, depth=0
            )

        total += bed_count
        mark_done(conn, category_label, bed_label, bed_count)

    mark_done(conn, category_label, label, total)
    return total


def _recursive_price_pull(conn, status, listing_type, place, beds, price_ranges,
                          parent_label, is_rental, depth):
    """Recursively split price ranges until each bucket fits under 5K.
    Uses probe-first to avoid fetching 5K records from dense buckets."""
    total = 0
    indent = "      " + "  " * depth

    for pmin, pmax in price_ranges:
        price_label = f"{parent_label}/${pmin or 0}-{pmax or 'max'}"

        # Probe first: does this price range exceed 5K?
        exceeds = probe_exceeds_limit(status, listing_type, place=place,
                                       bedrooms=beds, price_min=pmin, price_max=pmax)

        if not exceeds:
            # Under 5K — paginate normally
            p_count = paginate_query(conn, status, listing_type, place=place,
                                     bedrooms=beds, price_min=pmin, price_max=pmax,
                                     label=price_label)
            tprint(f"{indent}[{price_label}] : {p_count}")
            total += p_count
        else:
            # Over 5K — try to split further
            lo = pmin or 0
            hi = pmax
            if hi is None:
                hi = lo * 3 if lo > 0 else 50000 if is_rental else 20000000
            if hi - lo >= (100 if is_rental else 50000):
                mid = (lo + hi) // 2
                sub_ranges = [(pmin, mid), (mid, pmax)]
                tprint(f"{indent}[{price_label}] >5K, splitting → {lo}-{mid} / {mid}-{pmax or 'max'}")
                total += _recursive_price_pull(
                    conn, status, listing_type, place, beds, sub_ranges,
                    parent_label, is_rental, depth + 1
                )
            else:
                # Range too narrow to split, try reverse ordering
                rev_label = f"{price_label}/oldest"
                rev_count = paginate_query(conn, status, listing_type, place=place,
                                           bedrooms=beds, price_min=pmin, price_max=pmax,
                                           label=rev_label, order_by="Oldest")
                tprint(f"{indent}  [{rev_label}] reverse: {rev_count}")
                total += rev_count

    return total


# ── Main pull orchestration ─────────────────────────────────

def pull_category(conn, status, listing_type, category_label):
    is_rental = "Lease" in listing_type
    tprint(f"\n{'='*60}")
    tprint(f"  {category_label} ({status} / {listing_type})")
    tprint(f"{'='*60}")

    count_before = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    grand_total = 0

    for borough in NYC_BOROUGHS:
        place = make_place(borough)
        borough_label = f"{category_label}/{borough['name']}"

        # Check if entire borough is already done
        if is_done(conn, category_label, borough_label):
            tprint(f"  [{borough['name']}] SKIP (already completed)")
            continue

        # Probe: does this borough exceed 5K?
        exceeds = probe_exceeds_limit(status, listing_type, place=place)

        if not exceeds:
            # Under 5K — just paginate the whole borough
            count = paginate_query(conn, status, listing_type, place=place, label=borough_label)
            tprint(f"  [{borough['name']}] borough-level: {count}")
            grand_total += count
            mark_done(conn, category_label, borough_label, count)
            continue

        # Over 5K — sub-partition by neighborhood
        hoods = BOROUGH_HOODS.get(borough["name"], [])
        if not hoods:
            # For boroughs without defined neighborhoods, split by beds+price at borough level
            tprint(f"  [{borough['name']}] no neighborhoods defined, splitting by beds+price...")
            grand_total += exhaustive_pull(conn, status, listing_type, place,
                                           borough_label, is_rental,
                                           category_label=category_label) - count
            mark_done(conn, category_label, borough_label, grand_total)
            continue

        tprint(f"  [{borough['name']}] hit limit, partitioning into {len(hoods)} neighborhoods ({NUM_WORKERS} workers)...")

        def _pull_hood(hood):
            """Worker function — runs in thread pool with own DB connection."""
            tc = thread_conn()
            hood_place = make_place(hood)
            hood_label = f"{category_label}/{hood['name']}"
            try:
                count = exhaustive_pull(tc, status, listing_type, hood_place,
                                        hood_label, is_rental,
                                        category_label=category_label)
                return hood['name'], count
            finally:
                tc.close()

        with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
            futures = {executor.submit(_pull_hood, hood): hood for hood in hoods}
            for future in as_completed(futures):
                hood = futures[future]
                try:
                    name, count = future.result()
                    grand_total += count
                except Exception as e:
                    tprint(f"  [ERROR] {hood['name']}: {e}")

        mark_done(conn, category_label, borough_label, grand_total)

    count_after = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    new_records = count_after - count_before

    tprint(f"\n  [{category_label}] DONE — {grand_total:,} fetched, {new_records:,} new unique records")
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


# ── Main ────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Pull Elliman MLS listing data")
    parser.add_argument("--rentals-only", action="store_true")
    parser.add_argument("--sales-only", action="store_true")
    parser.add_argument("--active-only", action="store_true")
    parser.add_argument("--details", action="store_true")
    parser.add_argument("--details-limit", type=int)
    args = parser.parse_args()

    conn = init_db()

    tprint("Testing API connectivity...")
    test = api_get("/listing/search-options", label="test")
    if not test:
        tprint("ERROR: Cannot reach core.api.elliman.com")
        sys.exit(1)
    tprint("API is reachable.\n")

    if args.details:
        fetch_details(conn, limit=args.details_limit)
        conn.close()
        return

    pull_all = not (args.rentals_only or args.sales_only or args.active_only)
    totals = {}

    if pull_all or args.rentals_only:
        totals["closed_rentals"] = pull_category(
            conn, "Closed", "ResidentialLease", "Closed Rentals")

    if pull_all or args.sales_only:
        totals["closed_sales"] = pull_category(
            conn, "Closed", "ResidentialSale", "Closed Sales")

    if pull_all or args.active_only:
        totals["active_rentals"] = pull_category(
            conn, "Active", "ResidentialLease", "Active Rentals")
        totals["active_sales"] = pull_category(
            conn, "Active", "ResidentialSale", "Active Sales")

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

#!/usr/bin/env python3
"""
Corcoran Puller — pulls listing data from Corcoran's backend API
(backendapi.corcoranlabs.com).

Pulls ALL NYC listings:
  - Active rentals + sales (current asking prices)
  - Sold history (closed sales with prices)
  - Rented history (closed rentals with prices)
  - Expired listings (never sold/rented)
  - Full listing detail with price history (listingHistories), building info,
    subway proximity, etc.

Data is partitioned by borough for checkpoint/resume. Detail fetching uses
concurrent workers for speed. All raw JSON is preserved.

Usage:
    python3 scripts/pull_corcoran.py                    # pull everything NYC
    python3 scripts/pull_corcoran.py --manhattan-only   # Manhattan only
    python3 scripts/pull_corcoran.py --rentals-only     # rentals only (active + rented)
    python3 scripts/pull_corcoran.py --sales-only       # sales only (active + sold)
    python3 scripts/pull_corcoran.py --active-only      # active listings only
    python3 scripts/pull_corcoran.py --gramercy-only    # Gramercy test mode
    python3 scripts/pull_corcoran.py --details-only     # only fetch details
    python3 scripts/pull_corcoran.py --details-workers 8  # concurrent detail fetches
    python3 scripts/pull_corcoran.py --status           # show progress and exit
    python3 scripts/pull_corcoran.py --reset            # clear checkpoints, re-pull
"""

import argparse
import json
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

DB_PATH = Path(__file__).parent.parent / "corcoran.db"
API_BASE = "https://backendapi.corcoranlabs.com"
API_KEY = "667256B5BF6ABFF6C8BDC68E88226"
MAX_RETRIES = 6
PAGE_SIZE = 100  # API supports up to 100
DELAY = 0.15  # seconds between requests
DETAIL_WORKERS = 4  # concurrent detail fetch workers

# Borough names for partitioning
BOROUGHS = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]

# All neighborhoods we want to pull (empty = all via borough)
# These are used for the search filter
NYC_NEIGHBORHOODS = {
    "Manhattan": None,  # None = use citiesOrBoroughs filter instead
    "Brooklyn": None,
    "Queens": None,
    "Bronx": None,
    "Staten Island": None,
}

# Rate limiter for concurrent detail fetching
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


# ── Database ────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS listings (
            listing_id TEXT PRIMARY KEY,
            property_id TEXT,
            source_id TEXT,
            listing_status TEXT,
            transaction_type TEXT,
            listing_type TEXT,
            listing_style TEXT,
            building_type TEXT,
            property_type TEXT,
            ownership TEXT,
            unit_type TEXT,
            address1 TEXT,
            address2 TEXT,
            street_name TEXT,
            city TEXT,
            state TEXT,
            zip_code TEXT,
            borough TEXT,
            neighborhood TEXT,
            latitude REAL,
            longitude REAL,
            price REAL,
            bedrooms REAL,
            bathrooms REAL,
            half_baths REAL,
            total_bathrooms REAL,
            total_bedrooms REAL,
            square_footage REAL,
            is_exclusive INTEGER,
            is_new INTEGER,
            is_reduced_price INTEGER,
            is_idx INTEGER,
            is_building INTEGER,
            has_virtual_tour INTEGER,
            is_open_house INTEGER,
            advertise_no_fee INTEGER,
            media_url TEXT,
            agent_name TEXT,
            agent_email TEXT,
            agent_phone TEXT,
            closed_rented_date TEXT,
            json_data TEXT,
            detail_fetched INTEGER DEFAULT 0,
            detail_json TEXT,
            fetched_at TEXT
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
        CREATE INDEX IF NOT EXISTS idx_listings_txn ON listings(transaction_type);
        CREATE INDEX IF NOT EXISTS idx_listings_borough ON listings(borough);
        CREATE INDEX IF NOT EXISTS idx_listings_neighborhood ON listings(neighborhood);
        CREATE INDEX IF NOT EXISTS idx_listings_zip ON listings(zip_code);
        CREATE INDEX IF NOT EXISTS idx_listings_address ON listings(address1);
        CREATE INDEX IF NOT EXISTS idx_listings_detail ON listings(detail_fetched);
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

def make_headers():
    return {
        "be-api-key": API_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36",
    }


def api_post(endpoint, payload, label=""):
    url = f"{API_BASE}{endpoint}"
    for attempt in range(MAX_RETRIES):
        _rate_wait()
        try:
            resp = requests.post(url, headers=make_headers(), json=payload, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code in (429, 502, 503, 504):
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


def api_get(endpoint, label=""):
    url = f"{API_BASE}{endpoint}"
    for attempt in range(MAX_RETRIES):
        _rate_wait()
        try:
            resp = requests.get(url, headers=make_headers(), timeout=30)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code in (404, 410):
                return None
            elif resp.status_code in (429, 502, 503, 504):
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


# ── Checkpoint ──────────────────────────────────────────────

def is_done(conn, query_type, partition):
    row = conn.execute(
        "SELECT results_count FROM pull_log WHERE query_type=? AND partition=?",
        (query_type, partition)
    ).fetchone()
    return row is not None


def mark_done(conn, query_type, partition, results_count, new_count=0):
    conn.execute(
        "INSERT OR REPLACE INTO pull_log "
        "(query_type, partition, results_count, new_count) VALUES (?, ?, ?, ?)",
        (query_type, partition, results_count, new_count)
    )
    conn.commit()


# ── Extraction ──────────────────────────────────────────────

def extract_listing(item):
    """Extract structured fields from a search result item."""
    loc = item.get("location") or {}
    agents = item.get("agents") or []
    agent = agents[0] if agents else {}
    phones = agent.get("phoneNumbers") or []
    phone = phones[0].get("phoneNumber") if phones else None

    agent_name = None
    if agent:
        parts = [agent.get("firstName"), agent.get("lastName")]
        agent_name = " ".join(p for p in parts if p)

    return {
        "listing_id": str(item.get("listingId", "")),
        "property_id": str(item.get("propertyId", "")) if item.get("propertyId") else None,
        "source_id": str(item.get("sourceId", "")) if item.get("sourceId") else None,
        "listing_status": item.get("listingStatus"),
        "transaction_type": item.get("transactionType"),
        "listing_type": item.get("listingType"),
        "listing_style": item.get("listingStyle"),
        "building_type": item.get("buildingType"),
        "property_type": item.get("propertyType"),
        "ownership": item.get("ownership"),
        "unit_type": item.get("unitType"),
        "address1": item.get("address1"),
        "address2": item.get("address2"),
        "street_name": item.get("streetName"),
        "city": item.get("city"),
        "state": item.get("state"),
        "zip_code": item.get("zipCode"),
        "borough": item.get("boroughName"),
        "neighborhood": item.get("neighborhoodName"),
        "latitude": loc.get("latitude"),
        "longitude": loc.get("longitude"),
        "price": item.get("price"),
        "bedrooms": item.get("bedrooms"),
        "bathrooms": item.get("bathrooms"),
        "half_baths": item.get("halfBaths"),
        "total_bathrooms": item.get("totalBathrooms"),
        "total_bedrooms": item.get("totalBedrooms"),
        "square_footage": item.get("squareFootage"),
        "is_exclusive": 1 if item.get("isExclusive") else 0,
        "is_new": 1 if item.get("isNew") else 0,
        "is_reduced_price": 1 if item.get("isReducedPrice") else 0,
        "is_idx": 1 if item.get("isIdx") else 0,
        "is_building": 1 if item.get("isBuilding") else 0,
        "has_virtual_tour": 1 if item.get("hasVirtualTour") else 0,
        "is_open_house": 1 if item.get("isOpenHouse") else 0,
        "advertise_no_fee": 1 if item.get("advertiseNoFee") else 0,
        "media_url": item.get("mediaUrl"),
        "agent_name": agent_name,
        "agent_email": agent.get("email"),
        "agent_phone": phone,
        "closed_rented_date": item.get("closedRentedDate"),
        "json_data": json.dumps(item),
        "detail_fetched": 0,
        "detail_json": None,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def upsert_listings(conn, listings):
    if not listings:
        return 0
    cols = list(listings[0].keys())
    placeholders = ",".join(["?"] * len(cols))
    col_names = ",".join(cols)
    skip_on_update = {"listing_id", "detail_fetched", "detail_json"}
    updates = ",".join(
        [f"{c}=excluded.{c}" for c in cols if c not in skip_on_update]
    )
    conn.executemany(
        f"INSERT INTO listings ({col_names}) VALUES ({placeholders}) "
        f"ON CONFLICT(listing_id) DO UPDATE SET {updates}",
        [tuple(row[c] for c in cols) for row in listings]
    )
    conn.commit()
    return len(listings)


# ── Search & Pagination ────────────────────────────────────

def build_search_body(transaction_type=None, page=1, page_size=PAGE_SIZE,
                      neighborhoods=None, boroughs=None,
                      listing_status=None):
    """Build search request body."""
    body = {
        "page": page,
        "pageSize": page_size,
        "regionIds": ["1"],  # NYC
        "sortBy": ["price+asc"],
        "dateTimeOffset": "-5:0:00",
    }
    if transaction_type:
        body["transactionTypes"] = [transaction_type]
    if listing_status:
        body["listingStatus"] = [listing_status]
    if neighborhoods:
        body["neighborhoods"] = neighborhoods
    if boroughs:
        body["citiesOrBoroughs"] = boroughs
    return body


def paginate_search(conn, label, transaction_type=None, listing_status=None,
                    neighborhoods=None, boroughs=None, query_type=None):
    """Paginate through all search results. Returns total fetched."""

    partition_key = label
    if is_done(conn, query_type or label, partition_key):
        tprint(f"  [{label}] SKIP (already completed)")
        return 0

    body = build_search_body(transaction_type=transaction_type,
                             listing_status=listing_status, page=1,
                             neighborhoods=neighborhoods, boroughs=boroughs)
    data = api_post("/api/search/listings", body, label=f"{label}/p1")
    if not data:
        tprint(f"  [{label}] ERROR: no response from API")
        return 0

    total_items = data.get("totalItems", 0)
    total_pages = data.get("totalPages", 0)
    tprint(f"  [{label}] {total_items:,} total listings, {total_pages} pages")

    if total_items == 0:
        mark_done(conn, query_type or label, partition_key, 0, 0)
        return 0

    # Process first page
    items = data.get("items", [])
    all_count = 0
    if items:
        listings = [extract_listing(it) for it in items]
        upsert_listings(conn, listings)
        all_count += len(items)

    time.sleep(DELAY)

    # Paginate remaining pages
    for page in range(2, total_pages + 1):
        body = build_search_body(transaction_type=transaction_type,
                                 listing_status=listing_status, page=page,
                                 neighborhoods=neighborhoods, boroughs=boroughs)
        data = api_post("/api/search/listings", body,
                        label=f"{label}/p{page}")
        if not data:
            tprint(f"    [{label}] page {page}: ERROR, stopping")
            break

        items = data.get("items", [])
        if not items:
            tprint(f"    [{label}] page {page}: empty, stopping")
            break

        listings = [extract_listing(it) for it in items]
        upsert_listings(conn, listings)
        all_count += len(items)

        if page % 10 == 0 or page == total_pages:
            tprint(f"    [{label}] page {page}/{total_pages}: "
                   f"{all_count:,}/{total_items:,}")

        time.sleep(DELAY)

    mark_done(conn, query_type or label, partition_key, all_count, all_count)
    tprint(f"  [{label}] DONE: {all_count:,} items")
    return all_count


# ── Detail Fetcher (concurrent) ────────────────────────────

def _fetch_one_detail(lid):
    """Fetch detail for a single listing. Returns (lid, data_or_None)."""
    data = api_get(f"/api/listings/{lid}", label=f"d/{lid}")
    return (lid, data)


def fetch_details(conn, limit=None, workers=DETAIL_WORKERS):
    """Fetch full detail for listings that haven't been detailed yet.
    Uses concurrent workers for speed."""
    query = "SELECT listing_id FROM listings WHERE detail_fetched = 0"
    if limit:
        query += f" LIMIT {limit}"
    ids = [r[0] for r in conn.execute(query).fetchall()]

    if not ids:
        tprint("No listings need detail fetching.")
        return 0

    tprint(f"\nFetching details for {len(ids):,} listings ({workers} workers)...")
    fetched = 0
    errors = 0
    start_time = time.time()

    # Process in batches to commit periodically
    batch_size = 50
    for batch_start in range(0, len(ids), batch_size):
        batch_ids = ids[batch_start:batch_start + batch_size]

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_fetch_one_detail, lid): lid
                       for lid in batch_ids}

            for future in as_completed(futures):
                lid, data = future.result()

                if not data:
                    conn.execute(
                        "UPDATE listings SET detail_fetched=-1 WHERE listing_id=?",
                        (lid,)
                    )
                    errors += 1
                    continue

                updates = {
                    "detail_fetched": 1,
                    "detail_json": json.dumps(data),
                }

                # Enrich from detail response
                if data.get("squareFootage"):
                    updates["square_footage"] = data["squareFootage"]
                if data.get("neighborhood") and isinstance(data["neighborhood"], dict):
                    hood_name = data["neighborhood"].get("name")
                    if hood_name:
                        updates["neighborhood"] = hood_name
                if data.get("bedrooms") is not None:
                    updates["bedrooms"] = data["bedrooms"]
                if data.get("bathrooms") is not None:
                    updates["bathrooms"] = data["bathrooms"]
                if data.get("halfBathrooms") is not None:
                    updates["half_baths"] = data["halfBathrooms"]
                if data.get("latitude"):
                    updates["latitude"] = data["latitude"]
                if data.get("longitude"):
                    updates["longitude"] = data["longitude"]

                set_clause = ", ".join([f"{k}=?" for k in updates.keys()])
                conn.execute(
                    f"UPDATE listings SET {set_clause} WHERE listing_id=?",
                    list(updates.values()) + [lid]
                )
                fetched += 1

        conn.commit()
        done = batch_start + len(batch_ids)
        elapsed = time.time() - start_time
        rate = done / elapsed if elapsed > 0 else 0
        remaining = (len(ids) - done) / rate if rate > 0 else 0
        if done % 200 == 0 or done == len(ids):
            tprint(f"  Details: {done:,}/{len(ids):,} "
                   f"({fetched:,} ok, {errors:,} err) "
                   f"[{rate:.1f}/s, ~{remaining/60:.0f}m left]")

    tprint(f"  Details complete: {fetched:,}/{len(ids):,} "
           f"({errors:,} errors)")
    return fetched


# ── Main Pull Orchestration ─────────────────────────────────

def pull_category(conn, category_label, transaction_type=None,
                  listing_status=None, neighborhoods=None,
                  borough_filter=None):
    """Pull all listings for a category, partitioned by borough."""
    tprint(f"\n{'='*60}")
    tprint(f"  {category_label}")
    tprint(f"{'='*60}")

    count_before = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    grand_total = 0

    if neighborhoods:
        # Neighborhood-specific pull (e.g., Gramercy test mode)
        total = paginate_search(
            conn, category_label,
            transaction_type=transaction_type,
            listing_status=listing_status,
            neighborhoods=neighborhoods,
            query_type=category_label,
        )
        grand_total += total
    else:
        # Borough-partitioned pull for checkpoint/resume
        boroughs = [borough_filter] if borough_filter else BOROUGHS
        for borough in boroughs:
            borough_label = f"{category_label}/{borough}"
            total = paginate_search(
                conn, borough_label,
                transaction_type=transaction_type,
                listing_status=listing_status,
                boroughs=[borough],
                query_type=category_label,
            )
            grand_total += total

    count_after = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    new_records = count_after - count_before

    tprint(f"\n  [{category_label}] DONE — {grand_total:,} fetched, "
           f"{new_records:,} new unique records")
    return new_records


def show_status():
    """Show current pull progress and exit."""
    if not DB_PATH.exists():
        tprint("No database found. Nothing pulled yet.")
        return

    conn = sqlite3.connect(str(DB_PATH))

    total = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    detailed = conn.execute(
        "SELECT COUNT(*) FROM listings WHERE detail_fetched=1").fetchone()[0]
    detail_err = conn.execute(
        "SELECT COUNT(*) FROM listings WHERE detail_fetched=-1").fetchone()[0]
    pending = conn.execute(
        "SELECT COUNT(*) FROM listings WHERE detail_fetched=0").fetchone()[0]

    tprint(f"\n{'='*60}")
    tprint(f"  CORCORAN PULL STATUS")
    tprint(f"{'='*60}")
    tprint(f"  DB: {DB_PATH}")
    tprint(f"  DB size: {DB_PATH.stat().st_size / 1024 / 1024:.1f} MB")
    tprint(f"\n  Total listings: {total:,}")
    tprint(f"  Details fetched: {detailed:,}")
    tprint(f"  Details failed:  {detail_err:,}")
    tprint(f"  Details pending: {pending:,}")

    tprint(f"\n  By status:")
    for row in conn.execute(
        "SELECT listing_status, COUNT(*) FROM listings GROUP BY listing_status ORDER BY COUNT(*) DESC"
    ).fetchall():
        tprint(f"    {row[0] or '?'}: {row[1]:,}")

    tprint(f"\n  By borough:")
    for row in conn.execute(
        "SELECT borough, COUNT(*) FROM listings GROUP BY borough ORDER BY COUNT(*) DESC"
    ).fetchall():
        tprint(f"    {row[0] or '?'}: {row[1]:,}")

    tprint(f"\n  Completed partitions:")
    for row in conn.execute(
        "SELECT query_type, partition, results_count FROM pull_log ORDER BY id"
    ).fetchall():
        tprint(f"    [{row[0]}] {row[1]}: {row[2]:,}")

    # Show what's remaining
    tprint(f"\n  Remaining work:")
    all_categories = []
    for status in ["Active", "Sold", "Rented", "Expired"]:
        for txn in (["for-rent", "for-sale"] if status == "Active"
                    else [None]):
            label = f"{'Active Rentals' if txn == 'for-rent' else 'Active Sales' if txn == 'for-sale' else status}"
            for borough in BOROUGHS:
                partition = f"{label}/{borough}"
                if not is_done(conn, label, partition):
                    all_categories.append(partition)

    if all_categories:
        for cat in all_categories[:20]:
            tprint(f"    TODO: {cat}")
        if len(all_categories) > 20:
            tprint(f"    ... and {len(all_categories) - 20} more")
    else:
        tprint(f"    All search partitions complete!")
        if pending > 0:
            tprint(f"    {pending:,} listings still need detail fetching")

    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Pull Corcoran listing data (NYC)")
    parser.add_argument("--rentals-only", action="store_true",
                        help="Only pull rental listings (active + rented)")
    parser.add_argument("--sales-only", action="store_true",
                        help="Only pull sales listings (active + sold)")
    parser.add_argument("--active-only", action="store_true",
                        help="Only active listings")
    parser.add_argument("--manhattan-only", action="store_true",
                        help="Only Manhattan")
    parser.add_argument("--gramercy-only", action="store_true",
                        help="Only Gramercy neighborhood (test mode)")
    parser.add_argument("--details-only", action="store_true",
                        help="Only fetch full details for existing listings")
    parser.add_argument("--details-limit", type=int, default=None,
                        help="Limit number of detail fetches")
    parser.add_argument("--details-workers", type=int, default=DETAIL_WORKERS,
                        help=f"Concurrent detail fetch workers (default {DETAIL_WORKERS})")
    parser.add_argument("--status", action="store_true",
                        help="Show pull progress and exit")
    parser.add_argument("--reset", action="store_true",
                        help="Clear pull_log to re-pull everything")
    args = parser.parse_args()

    if args.status:
        show_status()
        return

    conn = init_db()

    if args.reset:
        conn.execute("DELETE FROM pull_log")
        conn.commit()
        tprint("Pull log cleared — will re-pull everything.")

    # Test connectivity
    tprint("Testing API connectivity...")
    test = api_get("/api/regions/all-regions", label="test")
    if not test:
        tprint("ERROR: Cannot reach backendapi.corcoranlabs.com")
        sys.exit(1)
    tprint("API is reachable.\n")

    # Detail-only mode
    if args.details_only:
        fetch_details(conn, limit=args.details_limit,
                      workers=args.details_workers)
        show_status()
        conn.close()
        return

    # Determine filters
    neighborhoods = None
    borough_filter = None
    if args.gramercy_only:
        neighborhoods = ["Gramercy"]
        tprint("MODE: Gramercy only (test)\n")
    elif args.manhattan_only:
        borough_filter = "Manhattan"
        tprint("MODE: Manhattan only\n")

    pull_all = not (args.rentals_only or args.sales_only)
    totals = {}

    # Active listings
    if pull_all or args.rentals_only:
        totals["active_rentals"] = pull_category(
            conn, "Active Rentals",
            transaction_type="for-rent",
            neighborhoods=neighborhoods,
            borough_filter=borough_filter,
        )

    if pull_all or args.sales_only:
        totals["active_sales"] = pull_category(
            conn, "Active Sales",
            transaction_type="for-sale",
            neighborhoods=neighborhoods,
            borough_filter=borough_filter,
        )

    # Historical data (sold + rented + expired)
    if not args.active_only:
        if pull_all or args.sales_only:
            totals["sold"] = pull_category(
                conn, "Sold",
                listing_status="Sold",
                neighborhoods=neighborhoods,
                borough_filter=borough_filter,
            )

        if pull_all or args.rentals_only:
            totals["rented"] = pull_category(
                conn, "Rented",
                listing_status="Rented",
                neighborhoods=neighborhoods,
                borough_filter=borough_filter,
            )

        totals["expired"] = pull_category(
            conn, "Expired",
            listing_status="Expired",
            neighborhoods=neighborhoods,
            borough_filter=borough_filter,
        )

    # Fetch details for all new listings
    tprint(f"\n{'='*60}")
    tprint("  Fetching listing details...")
    tprint(f"{'='*60}")
    fetch_details(conn, limit=args.details_limit,
                  workers=args.details_workers)

    # Summary
    tprint(f"\n{'='*60}")
    tprint("PULL COMPLETE")
    tprint(f"{'='*60}")
    for cat, count in totals.items():
        tprint(f"  {cat}: {count:,} new unique listings")

    total_db = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    detailed = conn.execute(
        "SELECT COUNT(*) FROM listings WHERE detail_fetched=1"
    ).fetchone()[0]
    tprint(f"\n  Total in DB: {total_db:,}")
    tprint(f"  With details: {detailed:,}")
    tprint(f"  DB location: {DB_PATH}")
    conn.close()


if __name__ == "__main__":
    main()

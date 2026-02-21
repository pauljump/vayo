#!/usr/bin/env python3
"""
Unify listing data from Elliman MLS, Corcoran, and StreetEasy Wayback
into a single listings_unified.db linked to BBL.

Read-only on all source databases. Creates listings_unified.db from scratch.

Usage:
    python3 scripts/unify_listings.py                     # full run
    python3 scripts/unify_listings.py --phase match       # address matching only
    python3 scripts/unify_listings.py --phase elliman      # elliman extract only
    python3 scripts/unify_listings.py --phase corcoran     # corcoran extract only
    python3 scripts/unify_listings.py --phase streeteasy   # SE wayback extract only
    python3 scripts/unify_listings.py --phase dedup        # dedup only
    python3 scripts/unify_listings.py --status             # show counts
"""

import argparse
import json
import logging
import os
import re
import sqlite3
import sys
import time
import urllib.request
import urllib.parse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_UNIFIED = BASE_DIR / "listings_unified.db"
DB_VAYO = BASE_DIR / "vayo_clean.db"
DB_ELLIMAN = BASE_DIR / "elliman_mls.db"
DB_CORCORAN = BASE_DIR / "corcoran.db"
DB_SE = BASE_DIR / "se_listings.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ── Address normalization (from build_clean_db.py) ──────────────

def normalize_addr(addr):
    """Normalize address for matching: uppercase, abbreviate street types."""
    if not addr:
        return ""
    a = addr.upper().strip()
    a = a.replace("_", "-")
    for full, abbr in [
        (" AVENUE", " AVE"), (" STREET", " ST"), (" BOULEVARD", " BLVD"),
        (" DRIVE", " DR"), (" PLACE", " PL"), (" ROAD", " RD"),
        (" COURT", " CT"), (" LANE", " LN"), (" TERRACE", " TERR"),
        (" PARKWAY", " PKWY"), (" SQUARE", " SQ"), (" CRESCENT", " CRES"),
    ]:
        a = a.replace(full, abbr)
    return a


# Borough normalization
BOROUGH_MAP = {
    "Manhattan": "MN", "New York": "MN", "New York City": "MN",
    "Brooklyn": "BK", "Kings": "BK",
    "Queens": "QN",
    "Bronx": "BX", "The Bronx": "BX",
    "Staten Island": "SI",
    "MN": "MN", "BK": "BK", "QN": "QN", "BX": "BX", "SI": "SI",
}


def norm_borough(b):
    if not b:
        return None
    return BOROUGH_MAP.get(b.strip(), None)


# ── Elliman address parsing ────────────────────────────────

# Elliman format: "200 E 23RD ST 7C, New York, NY 10010"
# or "1 1ST Pl 7, Brooklyn, NY 11231"
# Street suffixes to detect where street name ends and unit begins
STREET_SUFFIXES = {
    "ST", "AVE", "BLVD", "DR", "PL", "RD", "CT", "LN", "TERR", "PKWY",
    "SQ", "CRES", "WAY", "CIR", "HWY", "OVAL", "ALY", "ROW", "WALK",
    "BROADWAY", "BOWERY",
}
# Regex for ordinal streets like "23RD", "1ST", "2ND", "3RD", "4TH" etc
ORDINAL_RE = re.compile(r"^\d+(ST|ND|RD|TH)$", re.IGNORECASE)


def parse_elliman_address(addr_str):
    """Parse Elliman address into (street, unit, city, state, zip, borough_code).

    Input: "200 E 23RD ST 7C, New York, NY 10010"
    Output: ("200 E 23RD ST", "7C", "New York", "NY", "10010", "MN")
    """
    if not addr_str:
        return None, None, None, None, None, None

    # Split on commas
    parts = [p.strip() for p in addr_str.split(",")]
    street_unit = parts[0] if parts else ""
    city = parts[1] if len(parts) > 1 else ""
    state_zip = parts[2] if len(parts) > 2 else ""

    state, zipcode = "", ""
    if state_zip:
        sz = state_zip.strip().split()
        state = sz[0] if sz else ""
        zipcode = sz[1] if len(sz) > 1 else ""

    borough_code = norm_borough(city)

    # Split street from unit in the first part
    # Strategy: tokenize, find last street suffix, everything after is unit
    tokens = street_unit.split()
    street = street_unit
    unit = ""

    last_suffix_idx = -1
    for i, tok in enumerate(tokens):
        tok_upper = tok.upper().rstrip(".,")
        if tok_upper in STREET_SUFFIXES or ORDINAL_RE.match(tok_upper):
            last_suffix_idx = i

    if last_suffix_idx >= 0 and last_suffix_idx < len(tokens) - 1:
        street = " ".join(tokens[: last_suffix_idx + 1])
        unit = " ".join(tokens[last_suffix_idx + 1 :])

    return street, unit, city, state, zipcode, borough_code


def parse_corcoran_borough(borough_raw):
    """Normalize Corcoran borough field."""
    return norm_borough(borough_raw)


# ── SE slug/URL parsing ────────────────────────────────────

def parse_se_url(url):
    """Extract building slug and unit from SE URL.

    Input: "https://streeteasy.com/building/9115-colonial-road-brooklyn/5g"
    Output: ("9115-colonial-road-brooklyn", "5g")
    """
    if not url:
        return None, None
    m = re.search(r"/building/([^/]+?)(?:/([^/?]+))?(?:\?|$)", url)
    if not m:
        return None, None
    slug = m.group(1)
    unit = m.group(2) if m.group(2) else None
    return slug, unit


def slug_to_borough(slug):
    """Guess borough from SE building slug suffix."""
    s = slug.lower()
    for tag, code in [
        ("-new_york", "MN"), ("-manhattan", "MN"),
        ("-brooklyn", "BK"),
        ("-queens", "QN"),
        ("-bronx", "BX"), ("-the_bronx", "BX"),
        ("-staten_island", "SI"),
    ]:
        if s.endswith(tag):
            return code
    return None


# ── SE event type normalization ────────────────────────────

def normalize_se_event(event_type):
    """Normalize StreetEasy event_type text to a canonical event.

    Returns (event_type, broker) or (None, None) to skip.
    """
    if not event_type:
        return "price_point", None

    e = event_type.strip()

    # Skip noise
    skip_patterns = [
        "Browse Buildings", "Market Data", "Facts", "Building Class",
        "District", "Owned by", "Documents and Permits", "Previously Listed",
        "Amenities", "Floor Plans", "Schools", "Transportation",
        "Nearby Buildings", "Similar", "Landmark", "Historical",
    ]
    for pat in skip_patterns:
        if pat in e:
            return None, None

    if e == "-" or e == "":
        return "price_point", None

    # Listed
    m = re.match(r"^Listed by (.+)$", e)
    if m:
        return "listed", m.group(1).strip()
    if e == "LISTED":
        return "listed", None

    # Sold
    if e in ("Listing sold", "SOLD"):
        return "sold", None

    # Rented
    if e == "RENTED":
        return "rented", None

    # In contract
    if e in ("Listing entered contract", "IN_CONTRACT"):
        return "in_contract", None

    # No longer available / delisted
    if e in ("Listing is no longer available", "NO_LONGER_AVAILABLE",
             "No longer available"):
        return "delisted", None

    # Price changes
    if "Price decreased" in e or e == "PRICE_DECREASE":
        return "price_change", None
    if "Price increased" in e or e == "PRICE_INCREASE":
        return "price_change", None

    # Relisted
    if "Relisted" in e or e == "RELISTED":
        return "relisted", None

    # Previous sale recorded
    if "Previous Sale recorded" in e or e == "RECORDED_SALE":
        return "sold", None

    # Catch-all: if it has a price, treat as price_point
    return "price_point", None


# ── Status normalization ───────────────────────────────────

ELLIMAN_STATUS_MAP = {
    "Active": "active",
    "ActiveUnderContract": "pending",
    "Pending": "pending",
    "Closed": "closed",
    "TemporaryOffMarket": "off_market",
}

CORCORAN_STATUS_MAP = {
    "Active": "active",
    "Back on Market": "active",
    "PreListing": "active",
    "Contract Signed": "pending",
    "Sold": "closed",
    "Rented": "closed",
    "Expired": "expired",
}

ELLIMAN_TXN_MAP = {
    "Rental": "rental",
    "Residential": "sale",
}

CORCORAN_TXN_MAP = {
    "For Rent": "rental",
    "Rent": "rental",
    "For Sale": "sale",
    "Sale": "sale",
}


# ── Database setup ─────────────────────────────────────────

def init_unified_db():
    """Create or connect to the unified database."""
    conn = sqlite3.connect(str(DB_UNIFIED))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-512000")  # 512MB cache
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS address_bbl_cache (
            address_key TEXT PRIMARY KEY,
            bbl INTEGER,
            match_method TEXT,
            raw_address TEXT,
            borough TEXT,
            latitude REAL,
            longitude REAL,
            matched_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS unified_listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            source_listing_id TEXT NOT NULL,
            bbl INTEGER,
            address TEXT,
            unit TEXT,
            borough TEXT,
            neighborhood TEXT,
            zipcode TEXT,
            lat REAL,
            lon REAL,
            transaction_type TEXT,
            listing_status TEXT,
            list_price REAL,
            close_price REAL,
            price_per_sqft REAL,
            list_date TEXT,
            close_date TEXT,
            bedrooms REAL,
            bathrooms REAL,
            sqft REAL,
            year_built INTEGER,
            home_type TEXT,
            ownership_type TEXT,
            listing_agent TEXT,
            listing_brokerage TEXT,
            buyer_agent TEXT,
            buyer_brokerage TEXT,
            is_duplicate INTEGER DEFAULT 0,
            canonical_id INTEGER,
            UNIQUE(source, source_listing_id)
        );

        CREATE TABLE IF NOT EXISTS unified_price_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id INTEGER,
            bbl INTEGER,
            address TEXT,
            unit TEXT,
            source TEXT NOT NULL,
            source_id TEXT,
            event_date TEXT,
            event_type TEXT,
            price REAL,
            price_change REAL,
            broker TEXT,
            transaction_type TEXT,
            bedrooms REAL,
            bathrooms REAL,
            sqft REAL,
            FOREIGN KEY (listing_id) REFERENCES unified_listings(id)
        );

        CREATE TABLE IF NOT EXISTS etl_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            phase TEXT NOT NULL,
            started_at TEXT DEFAULT (datetime('now')),
            finished_at TEXT,
            rows_processed INTEGER,
            rows_inserted INTEGER,
            notes TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_ul_bbl ON unified_listings(bbl);
        CREATE INDEX IF NOT EXISTS idx_ul_source ON unified_listings(source);
        CREATE INDEX IF NOT EXISTS idx_ul_status ON unified_listings(listing_status);
        CREATE INDEX IF NOT EXISTS idx_ul_txn ON unified_listings(transaction_type);
        CREATE INDEX IF NOT EXISTS idx_ul_borough ON unified_listings(borough);
        CREATE INDEX IF NOT EXISTS idx_ul_dedup ON unified_listings(is_duplicate);

        CREATE INDEX IF NOT EXISTS idx_uph_bbl ON unified_price_history(bbl);
        CREATE INDEX IF NOT EXISTS idx_uph_listing ON unified_price_history(listing_id);
        CREATE INDEX IF NOT EXISTS idx_uph_source ON unified_price_history(source);
        CREATE INDEX IF NOT EXISTS idx_uph_date ON unified_price_history(event_date);
        CREATE INDEX IF NOT EXISTS idx_uph_type ON unified_price_history(event_type);

        CREATE INDEX IF NOT EXISTS idx_abc_bbl ON address_bbl_cache(bbl);
    """)
    conn.commit()
    return conn


def log_etl_run(conn, phase, rows_processed, rows_inserted, notes=""):
    conn.execute(
        "INSERT INTO etl_runs (phase, finished_at, rows_processed, rows_inserted, notes) "
        "VALUES (?, datetime('now'), ?, ?, ?)",
        (phase, rows_processed, rows_inserted, notes),
    )
    conn.commit()


# ── Phase 1: Address Matching ──────────────────────────────

def build_pluto_index():
    """Load PLUTO address index from vayo_clean.db (read-only)."""
    log.info("Building PLUTO address index from vayo_clean.db...")
    vayo = sqlite3.connect(f"file:{DB_VAYO}?mode=ro", uri=True)
    pluto = {}
    for bbl, address, borough in vayo.execute(
        "SELECT bbl, address, borough FROM buildings"
    ):
        key = f"{normalize_addr(address)}|{borough}"
        pluto[key] = bbl
    vayo.close()
    log.info(f"  PLUTO index: {len(pluto):,} addresses")
    return pluto


def geocode_address(address, borough=None):
    """Use NYC GeoSearch API to resolve address to BBL.

    Returns (bbl, lat, lon) or (None, None, None).
    """
    try:
        q = address
        if borough:
            boro_names = {"MN": "Manhattan", "BK": "Brooklyn", "QN": "Queens",
                          "BX": "Bronx", "SI": "Staten Island"}
            q += f", {boro_names.get(borough, borough)}, NY"
        url = f"https://geosearch.planninglabs.nyc/v2/search?text={urllib.parse.quote(q)}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        features = data.get("features", [])
        if not features:
            return None, None, None
        props = features[0].get("properties", {})
        # BBL can be at props.pad_bbl (old) or props.addendum.pad.bbl (new API format)
        pad_bbl = props.get("pad_bbl")
        if not pad_bbl:
            addendum = props.get("addendum")
            if isinstance(addendum, dict):
                pad = addendum.get("pad")
                if isinstance(pad, dict):
                    pad_bbl = pad.get("bbl")
        coords = features[0].get("geometry", {}).get("coordinates", [None, None])
        if pad_bbl:
            return int(pad_bbl), coords[1], coords[0]
        return None, None, None
    except Exception:
        return None, None, None


def phase_match(conn):
    """Phase 1: Build address_bbl_cache from all source databases."""
    log.info("=" * 60)
    log.info("PHASE 1: Address Matching")
    log.info("=" * 60)

    pluto = build_pluto_index()

    # Collect all distinct addresses from sources
    addresses = {}  # key -> (raw_address, borough)

    # Elliman addresses
    log.info("Collecting Elliman addresses...")
    ell = sqlite3.connect(f"file:{DB_ELLIMAN}?mode=ro", uri=True)
    ell_count = 0
    for (addr_raw, borough_raw) in ell.execute(
        "SELECT DISTINCT address, borough FROM listings WHERE address IS NOT NULL"
    ):
        street, unit, city, state, zipcode, borough_code = parse_elliman_address(addr_raw)
        if not street:
            continue
        if not borough_code:
            borough_code = norm_borough(borough_raw)
        if not borough_code:
            continue
        key = f"{normalize_addr(street)}|{borough_code}"
        if key not in addresses:
            addresses[key] = (street, borough_code)
            ell_count += 1
    ell.close()
    log.info(f"  Elliman: {ell_count:,} distinct addresses")

    # Corcoran addresses
    log.info("Collecting Corcoran addresses...")
    cor = sqlite3.connect(f"file:{DB_CORCORAN}?mode=ro", uri=True)
    cor_count = 0
    for (addr1, borough_raw) in cor.execute(
        "SELECT DISTINCT address1, borough FROM listings WHERE address1 IS NOT NULL"
    ):
        borough_code = norm_borough(borough_raw)
        if not borough_code:
            continue
        key = f"{normalize_addr(addr1)}|{borough_code}"
        if key not in addresses:
            addresses[key] = (addr1, borough_code)
            cor_count += 1
    cor.close()
    log.info(f"  Corcoran: {cor_count:,} distinct addresses")

    # SE building addresses
    log.info("Collecting StreetEasy addresses...")
    se = sqlite3.connect(f"file:{DB_SE}?mode=ro", uri=True)
    se_count = 0
    for (slug, addr) in se.execute(
        "SELECT slug, address FROM buildings WHERE address IS NOT NULL"
    ):
        borough_code = slug_to_borough(slug)
        if not borough_code:
            continue
        key = f"{normalize_addr(addr)}|{borough_code}"
        if key not in addresses:
            addresses[key] = (addr, borough_code)
            se_count += 1
    se.close()
    log.info(f"  StreetEasy: {se_count:,} distinct addresses")

    total = len(addresses)
    log.info(f"Total distinct addresses to match: {total:,}")

    # Check which are already cached
    cached = set()
    for (key,) in conn.execute("SELECT address_key FROM address_bbl_cache"):
        cached.add(key)
    log.info(f"  Already cached: {len(cached):,}")

    to_match = {k: v for k, v in addresses.items() if k not in cached}
    log.info(f"  Need to match: {len(to_match):,}")

    # Phase 1a: PLUTO exact match
    pluto_matched = 0
    geocode_needed = []
    batch = []

    for key, (raw_addr, borough) in to_match.items():
        bbl = pluto.get(key)
        if bbl:
            batch.append((key, bbl, "pluto_exact", raw_addr, borough, None, None))
            pluto_matched += 1
        else:
            geocode_needed.append((key, raw_addr, borough))

        if len(batch) >= 5000:
            conn.executemany(
                "INSERT OR IGNORE INTO address_bbl_cache "
                "(address_key, bbl, match_method, raw_address, borough, latitude, longitude) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                batch,
            )
            conn.commit()
            batch = []

    if batch:
        conn.executemany(
            "INSERT OR IGNORE INTO address_bbl_cache "
            "(address_key, bbl, match_method, raw_address, borough, latitude, longitude) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            batch,
        )
        conn.commit()

    log.info(f"  PLUTO exact match: {pluto_matched:,}")
    log.info(f"  Need geocoding: {len(geocode_needed):,}")

    # Phase 1b: Geocode unmatched via NYC GeoSearch (concurrent)
    if geocode_needed:
        log.info("Geocoding unmatched addresses via NYC GeoSearch API (20 workers)...")
        geo_matched = 0
        geo_failed = 0
        batch = []
        completed = 0

        def _geocode_one(item):
            key, raw_addr, borough = item
            bbl, lat, lon = geocode_address(raw_addr, borough)
            if bbl:
                return (key, bbl, "geocoder", raw_addr, borough, lat, lon)
            else:
                return (key, None, "unmatched", raw_addr, borough, None, None)

        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = {executor.submit(_geocode_one, item): item for item in geocode_needed}
            for future in as_completed(futures):
                result = future.result()
                completed += 1
                if result[1] is not None:  # bbl matched
                    geo_matched += 1
                else:
                    geo_failed += 1
                batch.append(result)

                if len(batch) >= 500:
                    conn.executemany(
                        "INSERT OR IGNORE INTO address_bbl_cache "
                        "(address_key, bbl, match_method, raw_address, borough, latitude, longitude) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        batch,
                    )
                    conn.commit()
                    batch = []

                if completed % 2000 == 0:
                    log.info(
                        f"    Geocoded {completed:,}/{len(geocode_needed):,} "
                        f"(matched={geo_matched:,}, failed={geo_failed:,})"
                    )

        if batch:
            conn.executemany(
                "INSERT OR IGNORE INTO address_bbl_cache "
                "(address_key, bbl, match_method, raw_address, borough, latitude, longitude) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                batch,
            )
            conn.commit()

        log.info(f"  Geocoder matched: {geo_matched:,}")
        log.info(f"  Unmatched: {geo_failed:,}")

    # Summary
    stats = conn.execute(
        "SELECT match_method, count(*) FROM address_bbl_cache GROUP BY match_method"
    ).fetchall()
    total_cached = sum(c for _, c in stats)
    matched = sum(c for m, c in stats if m != "unmatched")
    log.info(f"Address match summary: {matched:,}/{total_cached:,} "
             f"({100*matched/total_cached:.1f}% match rate)")
    for method, count in stats:
        log.info(f"  {method}: {count:,}")

    log_etl_run(conn, "match", total, pluto_matched + len(geocode_needed),
                f"pluto={pluto_matched}, geocoded={len(geocode_needed)}")


def lookup_bbl(conn, address, borough_code):
    """Look up BBL from address_bbl_cache."""
    if not address or not borough_code:
        return None
    key = f"{normalize_addr(address)}|{borough_code}"
    row = conn.execute(
        "SELECT bbl FROM address_bbl_cache WHERE address_key = ?", (key,)
    ).fetchone()
    return row[0] if row else None


# ── Phase 2: Elliman Extract ───────────────────────────────

def phase_elliman(conn):
    """Extract Elliman MLS listings into unified tables."""
    log.info("=" * 60)
    log.info("PHASE 2: Elliman Extract")
    log.info("=" * 60)

    ell = sqlite3.connect(f"file:{DB_ELLIMAN}?mode=ro", uri=True)
    ell.row_factory = sqlite3.Row

    total = ell.execute("SELECT count(*) FROM listings").fetchone()[0]
    log.info(f"  Elliman source: {total:,} listings")

    inserted_listings = 0
    inserted_events = 0
    processed = 0
    batch_listings = []
    batch_events = []

    for row in ell.execute("SELECT * FROM listings"):
        processed += 1

        # Parse address
        street, unit_parsed, city, state, zipcode, borough_code = parse_elliman_address(
            row["address"]
        )
        if not borough_code:
            borough_code = norm_borough(row["borough"])

        # Unit: prefer parsed unit from address, fall back to unit column
        unit = unit_parsed or row["unit"] or ""

        # BBL lookup
        bbl = lookup_bbl(conn, street, borough_code)

        # Normalize fields
        status = ELLIMAN_STATUS_MAP.get(row["listing_status"], row["listing_status"])
        txn_type = ELLIMAN_TXN_MAP.get(row["listing_type"], row["listing_type"])
        bathrooms = (row["bathrooms_full"] or 0) + 0.5 * (row["bathrooms_half"] or 0)

        batch_listings.append((
            "elliman",
            row["core_listing_id"],
            bbl,
            street,
            unit,
            borough_code,
            row["neighborhood"],
            zipcode or row["zip"],
            row["latitude"],
            row["longitude"],
            txn_type,
            status,
            row["list_price"],
            row["close_price"],
            row["price_per_sqft"],
            row["list_date"],
            row["close_date"],
            row["bedrooms"],
            bathrooms,
            row["living_area_sqft"],
            row["year_built"],
            row["home_type"],
            row["ownership_type"],
            row["listing_agent"],
            row["listing_brokerage"],
            row["buyer_agent"],
            row["buyer_brokerage"],
        ))

        # Generate price history events
        if row["list_date"] and row["list_price"]:
            batch_events.append((
                None,  # listing_id filled later if needed
                bbl,
                street,
                unit,
                "elliman",
                row["core_listing_id"],
                row["list_date"],
                "listed",
                row["list_price"],
                None,
                row["listing_brokerage"],
                txn_type,
                row["bedrooms"],
                bathrooms,
                row["living_area_sqft"],
            ))

        if row["close_date"] and row["close_price"]:
            event = "sold" if txn_type == "sale" else "rented"
            batch_events.append((
                None,
                bbl,
                street,
                unit,
                "elliman",
                row["core_listing_id"],
                row["close_date"],
                event,
                row["close_price"],
                None,
                row["listing_brokerage"],
                txn_type,
                row["bedrooms"],
                bathrooms,
                row["living_area_sqft"],
            ))

        if len(batch_listings) >= 5000:
            conn.executemany(
                "INSERT OR IGNORE INTO unified_listings "
                "(source, source_listing_id, bbl, address, unit, borough, neighborhood, "
                "zipcode, lat, lon, transaction_type, listing_status, list_price, close_price, "
                "price_per_sqft, list_date, close_date, bedrooms, bathrooms, sqft, "
                "year_built, home_type, ownership_type, listing_agent, listing_brokerage, "
                "buyer_agent, buyer_brokerage) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                batch_listings,
            )
            inserted_listings += conn.execute(
                "SELECT changes()"
            ).fetchone()[0]
            conn.executemany(
                "INSERT INTO unified_price_history "
                "(listing_id, bbl, address, unit, source, source_id, event_date, "
                "event_type, price, price_change, broker, transaction_type, "
                "bedrooms, bathrooms, sqft) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                batch_events,
            )
            inserted_events += conn.execute("SELECT changes()").fetchone()[0]
            conn.commit()
            batch_listings = []
            batch_events = []

        if processed % 50000 == 0:
            log.info(f"    Processed {processed:,}/{total:,}")

    # Final batch
    if batch_listings:
        conn.executemany(
            "INSERT OR IGNORE INTO unified_listings "
            "(source, source_listing_id, bbl, address, unit, borough, neighborhood, "
            "zipcode, lat, lon, transaction_type, listing_status, list_price, close_price, "
            "price_per_sqft, list_date, close_date, bedrooms, bathrooms, sqft, "
            "year_built, home_type, ownership_type, listing_agent, listing_brokerage, "
            "buyer_agent, buyer_brokerage) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            batch_listings,
        )
        inserted_listings += conn.execute("SELECT changes()").fetchone()[0]
    if batch_events:
        conn.executemany(
            "INSERT INTO unified_price_history "
            "(listing_id, bbl, address, unit, source, source_id, event_date, "
            "event_type, price, price_change, broker, transaction_type, "
            "bedrooms, bathrooms, sqft) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            batch_events,
        )
        inserted_events += conn.execute("SELECT changes()").fetchone()[0]
    conn.commit()
    ell.close()

    log.info(f"  Elliman: {inserted_listings:,} listings, {inserted_events:,} price events inserted")
    log_etl_run(conn, "elliman", processed, inserted_listings,
                f"events={inserted_events}")


# ── Phase 3: Corcoran Extract ──────────────────────────────

def phase_corcoran(conn):
    """Extract Corcoran listings into unified tables."""
    log.info("=" * 60)
    log.info("PHASE 3: Corcoran Extract")
    log.info("=" * 60)

    cor = sqlite3.connect(f"file:{DB_CORCORAN}?mode=ro", uri=True)
    cor.row_factory = sqlite3.Row

    total = cor.execute("SELECT count(*) FROM listings").fetchone()[0]
    log.info(f"  Corcoran source: {total:,} listings")

    inserted_listings = 0
    inserted_events = 0
    history_events = 0
    processed = 0
    batch_listings = []
    batch_events = []
    batch_history = []

    for row in cor.execute("SELECT * FROM listings"):
        processed += 1

        addr1 = row["address1"] or ""
        unit = row["address2"] or ""
        borough_code = norm_borough(row["borough"])

        bbl = lookup_bbl(conn, addr1, borough_code)

        status_raw = row["listing_status"] or ""
        status = CORCORAN_STATUS_MAP.get(status_raw, status_raw.lower() if status_raw else None)
        txn_raw = row["transaction_type"] or ""
        txn_type = CORCORAN_TXN_MAP.get(txn_raw, txn_raw.lower() if txn_raw else None)

        # Enrich from detail_json if available
        list_price = row["price"]
        close_price = None
        list_date = None
        close_date = row["closed_rented_date"]
        bedrooms = row["bedrooms"]
        bathrooms = row["bathrooms"]
        half_baths = row["half_baths"]
        sqft = row["square_footage"]
        year_built = None
        home_type = None
        ownership_type = None
        listing_agent = row["agent_name"]
        listing_brokerage = None
        neighborhood = row["neighborhood"]
        lat = row["latitude"]
        lon = row["longitude"]
        zipcode = row["zip_code"]
        buyer_agent = None
        buyer_brokerage = None

        detail = None
        if row["detail_fetched"] and row["detail_json"]:
            try:
                detail = json.loads(row["detail_json"])
            except (json.JSONDecodeError, TypeError):
                pass

        if detail:
            # Enrich with detail fields
            sqft = detail.get("squareFootage") or sqft
            year_built = detail.get("yearBuilt")
            if isinstance(year_built, str):
                try:
                    year_built = int(year_built)
                except ValueError:
                    year_built = None
            ownership_type = detail.get("buildingOwnershipType")
            list_date = detail.get("dateListed") or detail.get("listedDate")
            if list_date and "T" in str(list_date):
                list_date = str(list_date).split("T")[0]
            close_date_d = detail.get("dateSold")
            if close_date_d:
                if "T" in str(close_date_d):
                    close_date = str(close_date_d).split("T")[0]
                else:
                    close_date = close_date_d
            listing_brokerage = detail.get("brokerageName")
            lat = detail.get("latitude") or lat
            lon = detail.get("longitude") or lon

            agents = detail.get("agents", [])
            if agents and isinstance(agents, list):
                a = agents[0]
                listing_agent = f"{a.get('firstName', '')} {a.get('lastName', '')}".strip()

            nb = detail.get("neighborhood")
            if isinstance(nb, dict):
                neighborhood = nb.get("name") or neighborhood

            # Price from detail
            detail_price = detail.get("price")
            if detail_price:
                if status == "closed":
                    close_price = detail_price
                else:
                    list_price = detail_price

        # Compute bathrooms
        if bathrooms is not None or half_baths is not None:
            bathrooms = (bathrooms or 0) + 0.5 * (half_baths or 0)

        batch_listings.append((
            "corcoran",
            row["listing_id"],
            bbl,
            addr1,
            unit,
            borough_code,
            neighborhood,
            zipcode,
            lat,
            lon,
            txn_type,
            status,
            list_price,
            close_price,
            None,  # price_per_sqft
            list_date,
            close_date,
            bedrooms,
            bathrooms,
            sqft,
            year_built,
            home_type,
            ownership_type,
            listing_agent,
            listing_brokerage,
            buyer_agent,
            buyer_brokerage,
        ))

        # Generate basic price events from listing itself
        if list_date and list_price:
            batch_events.append((
                None, bbl, addr1, unit, "corcoran", row["listing_id"],
                list_date, "listed", list_price, None, listing_brokerage,
                txn_type, bedrooms, bathrooms, sqft,
            ))
        if close_date and (close_price or list_price):
            event = "sold" if txn_type == "sale" else "rented"
            batch_events.append((
                None, bbl, addr1, unit, "corcoran", row["listing_id"],
                close_date, event, close_price or list_price, None,
                listing_brokerage, txn_type, bedrooms, bathrooms, sqft,
            ))

        # Extract listingHistories from detail (building-wide transaction history)
        if detail and detail.get("listingHistories"):
            for hist in detail["listingHistories"]:
                h_date = hist.get("dateSold")
                if h_date and "T" in str(h_date):
                    h_date = str(h_date).split("T")[0]
                h_unit = hist.get("unitNumber", "")
                h_status = hist.get("listingStatus", "")
                h_event = "sold" if "Sold" in h_status else "rented" if "Rent" in h_status else "closed"
                h_price = hist.get("soldPrice") or hist.get("originalPrice")
                h_beds = hist.get("bedrooms")
                h_baths = hist.get("bathrooms")
                h_sqft_obj = hist.get("squareFootage") or (
                    hist.get("square", {}).get("value") if isinstance(hist.get("square"), dict) else None
                )

                if h_date and h_price:
                    batch_history.append((
                        None, bbl, addr1, h_unit, "corcoran_history",
                        str(hist.get("listingId", "")),
                        h_date, h_event, h_price, None, None,
                        "sale" if h_event == "sold" else "rental",
                        h_beds, h_baths, h_sqft_obj,
                    ))

        if len(batch_listings) >= 5000:
            conn.executemany(
                "INSERT OR IGNORE INTO unified_listings "
                "(source, source_listing_id, bbl, address, unit, borough, neighborhood, "
                "zipcode, lat, lon, transaction_type, listing_status, list_price, close_price, "
                "price_per_sqft, list_date, close_date, bedrooms, bathrooms, sqft, "
                "year_built, home_type, ownership_type, listing_agent, listing_brokerage, "
                "buyer_agent, buyer_brokerage) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                batch_listings,
            )
            inserted_listings += conn.execute("SELECT changes()").fetchone()[0]
            if batch_events:
                conn.executemany(
                    "INSERT INTO unified_price_history "
                    "(listing_id, bbl, address, unit, source, source_id, event_date, "
                    "event_type, price, price_change, broker, transaction_type, "
                    "bedrooms, bathrooms, sqft) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    batch_events,
                )
                inserted_events += conn.execute("SELECT changes()").fetchone()[0]
            if batch_history:
                conn.executemany(
                    "INSERT INTO unified_price_history "
                    "(listing_id, bbl, address, unit, source, source_id, event_date, "
                    "event_type, price, price_change, broker, transaction_type, "
                    "bedrooms, bathrooms, sqft) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    batch_history,
                )
                history_events += conn.execute("SELECT changes()").fetchone()[0]
            conn.commit()
            batch_listings = []
            batch_events = []
            batch_history = []

        if processed % 50000 == 0:
            log.info(f"    Processed {processed:,}/{total:,}")

    # Final batch
    if batch_listings:
        conn.executemany(
            "INSERT OR IGNORE INTO unified_listings "
            "(source, source_listing_id, bbl, address, unit, borough, neighborhood, "
            "zipcode, lat, lon, transaction_type, listing_status, list_price, close_price, "
            "price_per_sqft, list_date, close_date, bedrooms, bathrooms, sqft, "
            "year_built, home_type, ownership_type, listing_agent, listing_brokerage, "
            "buyer_agent, buyer_brokerage) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            batch_listings,
        )
        inserted_listings += conn.execute("SELECT changes()").fetchone()[0]
    if batch_events:
        conn.executemany(
            "INSERT INTO unified_price_history "
            "(listing_id, bbl, address, unit, source, source_id, event_date, "
            "event_type, price, price_change, broker, transaction_type, "
            "bedrooms, bathrooms, sqft) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            batch_events,
        )
        inserted_events += conn.execute("SELECT changes()").fetchone()[0]
    if batch_history:
        conn.executemany(
            "INSERT INTO unified_price_history "
            "(listing_id, bbl, address, unit, source, source_id, event_date, "
            "event_type, price, price_change, broker, transaction_type, "
            "bedrooms, bathrooms, sqft) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            batch_history,
        )
        history_events += conn.execute("SELECT changes()").fetchone()[0]
    conn.commit()
    cor.close()

    log.info(f"  Corcoran: {inserted_listings:,} listings, "
             f"{inserted_events:,} listing events, {history_events:,} history events inserted")
    log_etl_run(conn, "corcoran", processed, inserted_listings,
                f"events={inserted_events}, history={history_events}")


# ── Phase 4: StreetEasy Wayback Extract ────────────────────

def phase_streeteasy(conn):
    """Extract StreetEasy Wayback price events into unified_price_history."""
    log.info("=" * 60)
    log.info("PHASE 4: StreetEasy Wayback Extract")
    log.info("=" * 60)

    se = sqlite3.connect(f"file:{DB_SE}?mode=ro", uri=True)
    se.row_factory = sqlite3.Row

    # Build slug→address map from buildings table
    slug_addr = {}
    for slug_row in se.execute("SELECT slug, address FROM buildings WHERE address IS NOT NULL"):
        slug_addr[slug_row["slug"]] = slug_row["address"]
    log.info(f"  SE building address map: {len(slug_addr):,} slugs")

    total = se.execute("SELECT count(*) FROM wb_price_history").fetchone()[0]
    log.info(f"  SE Wayback source: {total:,} raw events")

    inserted = 0
    skipped = 0
    processed = 0
    batch = []

    for row in se.execute("SELECT * FROM wb_price_history"):
        processed += 1

        event_type_raw = row["event_type"]
        price = row["price"]

        # Normalize event type
        event_norm, broker = normalize_se_event(event_type_raw)
        if event_norm is None:
            skipped += 1
            if processed % 200000 == 0:
                log.info(f"    Processed {processed:,}/{total:,} (inserted={inserted:,}, skipped={skipped:,})")
            continue

        # Must have a price or a meaningful event
        if not price and event_norm == "price_point":
            skipped += 1
            if processed % 200000 == 0:
                log.info(f"    Processed {processed:,}/{total:,} (inserted={inserted:,}, skipped={skipped:,})")
            continue

        # Parse URL
        slug, unit = parse_se_url(row["url"])
        if not slug:
            skipped += 1
            continue

        # Resolve address
        address = slug_addr.get(slug)
        borough_code = slug_to_borough(slug)

        # BBL lookup
        bbl = None
        if address and borough_code:
            bbl = lookup_bbl(conn, address, borough_code)

        # Determine transaction type from event
        txn = None
        if event_norm == "sold":
            txn = "sale"
        elif event_norm == "rented":
            txn = "rental"

        # Use broker from event or from row
        broker_final = broker or row["broker"]

        batch.append((
            None,  # listing_id
            bbl,
            address,
            unit,
            "streeteasy",
            row["se_id"] or slug,
            row["event_date"],
            event_norm,
            price,
            row["price_change"],
            broker_final,
            txn,
            None,  # bedrooms
            None,  # bathrooms
            None,  # sqft
        ))

        if len(batch) >= 10000:
            conn.executemany(
                "INSERT INTO unified_price_history "
                "(listing_id, bbl, address, unit, source, source_id, event_date, "
                "event_type, price, price_change, broker, transaction_type, "
                "bedrooms, bathrooms, sqft) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                batch,
            )
            inserted += conn.execute("SELECT changes()").fetchone()[0]
            conn.commit()
            batch = []

        if processed % 200000 == 0:
            log.info(f"    Processed {processed:,}/{total:,} (inserted={inserted:,}, skipped={skipped:,})")

    if batch:
        conn.executemany(
            "INSERT INTO unified_price_history "
            "(listing_id, bbl, address, unit, source, source_id, event_date, "
            "event_type, price, price_change, broker, transaction_type, "
            "bedrooms, bathrooms, sqft) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            batch,
        )
        inserted += conn.execute("SELECT changes()").fetchone()[0]
    conn.commit()
    se.close()

    log.info(f"  StreetEasy: {inserted:,} events inserted, {skipped:,} skipped")
    log_etl_run(conn, "streeteasy", processed, inserted, f"skipped={skipped}")


# ── Phase 5: Deduplication ─────────────────────────────────

def phase_dedup(conn):
    """Cross-source deduplication of unified_listings."""
    log.info("=" * 60)
    log.info("PHASE 5: Deduplication")
    log.info("=" * 60)

    # Reset previous dedup flags
    conn.execute("UPDATE unified_listings SET is_duplicate = 0, canonical_id = NULL")
    conn.commit()

    # Find duplicates: same BBL + normalized unit + close_date + transaction_type
    # with prices within 5% of each other
    log.info("Finding cross-source duplicates...")

    # Build groups keyed on (bbl, unit_normalized, close_date, transaction_type)
    groups = defaultdict(list)
    cursor = conn.execute(
        "SELECT id, source, bbl, unit, close_date, transaction_type, "
        "close_price, list_price, listing_status "
        "FROM unified_listings "
        "WHERE bbl IS NOT NULL AND close_date IS NOT NULL"
    )
    for row in cursor:
        lid, source, bbl, unit, close_date, txn, close_price, list_price, status = row
        unit_norm = (unit or "").upper().strip().lstrip("#").replace(" ", "")
        key = (bbl, unit_norm, close_date, txn)
        groups[key].append({
            "id": lid, "source": source, "close_price": close_price,
            "list_price": list_price, "status": status,
        })

    # Priority: corcoran (with detail) > elliman > others
    SOURCE_PRIORITY = {"corcoran": 0, "elliman": 1, "streeteasy": 2}

    dup_count = 0
    batch = []

    for key, listings in groups.items():
        if len(listings) < 2:
            continue

        # Check if listings span multiple sources
        sources = set(l["source"] for l in listings)
        if len(sources) < 2:
            continue

        # Sort by priority (best first)
        listings.sort(key=lambda l: SOURCE_PRIORITY.get(l["source"], 99))

        # Check price similarity (within 10%)
        canonical = listings[0]
        canon_price = canonical["close_price"] or canonical["list_price"] or 0

        for dup in listings[1:]:
            dup_price = dup["close_price"] or dup["list_price"] or 0
            if canon_price > 0 and dup_price > 0:
                diff = abs(canon_price - dup_price) / canon_price
                if diff > 0.10:
                    continue  # Prices too different, not a real duplicate

            batch.append((canonical["id"], dup["id"]))
            dup_count += 1

            if len(batch) >= 5000:
                conn.executemany(
                    "UPDATE unified_listings SET is_duplicate = 1, canonical_id = ? WHERE id = ?",
                    batch,
                )
                conn.commit()
                batch = []

    if batch:
        conn.executemany(
            "UPDATE unified_listings SET is_duplicate = 1, canonical_id = ? WHERE id = ?",
            batch,
        )
        conn.commit()

    log.info(f"  Flagged {dup_count:,} duplicate listings")
    log_etl_run(conn, "dedup", len(groups), dup_count)


# ── Status ─────────────────────────────────────────────────

def show_status():
    """Show counts and stats from the unified database."""
    if not DB_UNIFIED.exists():
        print("No listings_unified.db found. Run the pipeline first.")
        return

    conn = sqlite3.connect(str(DB_UNIFIED))

    print("\n" + "=" * 60)
    print("UNIFIED LISTINGS DATABASE STATUS")
    print("=" * 60)

    # Address cache
    total_addr = conn.execute("SELECT count(*) FROM address_bbl_cache").fetchone()[0]
    matched_addr = conn.execute(
        "SELECT count(*) FROM address_bbl_cache WHERE bbl IS NOT NULL"
    ).fetchone()[0]
    print(f"\nAddress Cache: {total_addr:,} total, {matched_addr:,} matched "
          f"({100*matched_addr/max(total_addr,1):.1f}%)")
    for method, cnt in conn.execute(
        "SELECT match_method, count(*) FROM address_bbl_cache GROUP BY match_method ORDER BY count(*) DESC"
    ):
        print(f"  {method}: {cnt:,}")

    # Listings
    total_listings = conn.execute("SELECT count(*) FROM unified_listings").fetchone()[0]
    print(f"\nUnified Listings: {total_listings:,}")

    print("\n  By source:")
    for source, cnt in conn.execute(
        "SELECT source, count(*) FROM unified_listings GROUP BY source ORDER BY count(*) DESC"
    ):
        with_bbl = conn.execute(
            "SELECT count(*) FROM unified_listings WHERE source=? AND bbl IS NOT NULL", (source,)
        ).fetchone()[0]
        print(f"    {source}: {cnt:,} ({100*with_bbl/max(cnt,1):.0f}% with BBL)")

    print("\n  By status:")
    for status, cnt in conn.execute(
        "SELECT listing_status, count(*) FROM unified_listings GROUP BY listing_status ORDER BY count(*) DESC"
    ):
        print(f"    {status}: {cnt:,}")

    print("\n  By transaction type:")
    for txn, cnt in conn.execute(
        "SELECT transaction_type, count(*) FROM unified_listings GROUP BY transaction_type ORDER BY count(*) DESC"
    ):
        print(f"    {txn}: {cnt:,}")

    dupes = conn.execute(
        "SELECT count(*) FROM unified_listings WHERE is_duplicate = 1"
    ).fetchone()[0]
    print(f"\n  Duplicates flagged: {dupes:,}")

    # Price history
    total_events = conn.execute("SELECT count(*) FROM unified_price_history").fetchone()[0]
    print(f"\nUnified Price History: {total_events:,}")

    print("\n  By source:")
    for source, cnt in conn.execute(
        "SELECT source, count(*) FROM unified_price_history GROUP BY source ORDER BY count(*) DESC"
    ):
        print(f"    {source}: {cnt:,}")

    print("\n  By event type:")
    for etype, cnt in conn.execute(
        "SELECT event_type, count(*) FROM unified_price_history GROUP BY event_type ORDER BY count(*) DESC LIMIT 15"
    ):
        print(f"    {etype}: {cnt:,}")

    # BBL coverage
    distinct_bbls = conn.execute(
        "SELECT count(DISTINCT bbl) FROM unified_listings WHERE bbl IS NOT NULL"
    ).fetchone()[0]
    print(f"\nDistinct BBLs in listings: {distinct_bbls:,}")

    distinct_bbls_events = conn.execute(
        "SELECT count(DISTINCT bbl) FROM unified_price_history WHERE bbl IS NOT NULL"
    ).fetchone()[0]
    print(f"Distinct BBLs in price history: {distinct_bbls_events:,}")

    # ETL runs
    print("\nETL Runs:")
    for phase, started, finished, rows_p, rows_i, notes in conn.execute(
        "SELECT phase, started_at, finished_at, rows_processed, rows_inserted, notes "
        "FROM etl_runs ORDER BY id"
    ):
        print(f"  {phase}: {rows_p:,} processed → {rows_i:,} inserted "
              f"({started} → {finished}) {notes or ''}")

    # DB file size
    size_mb = DB_UNIFIED.stat().st_size / 1024 / 1024
    print(f"\nDatabase size: {size_mb:.1f} MB")
    print("=" * 60)

    conn.close()


# ── Main ───────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Unify listing data across sources")
    parser.add_argument("--phase", choices=["match", "elliman", "corcoran", "streeteasy", "dedup"],
                        help="Run only a specific phase")
    parser.add_argument("--status", action="store_true", help="Show database status")
    args = parser.parse_args()

    if args.status:
        show_status()
        return

    # Verify source databases exist
    for db, name in [(DB_VAYO, "vayo_clean.db"), (DB_ELLIMAN, "elliman_mls.db"),
                     (DB_CORCORAN, "corcoran.db"), (DB_SE, "se_listings.db")]:
        if not db.exists():
            log.error(f"Source database not found: {db}")
            sys.exit(1)

    conn = init_unified_db()

    phases = {
        "match": phase_match,
        "elliman": phase_elliman,
        "corcoran": phase_corcoran,
        "streeteasy": phase_streeteasy,
        "dedup": phase_dedup,
    }

    if args.phase:
        log.info(f"Running phase: {args.phase}")
        phases[args.phase](conn)
    else:
        log.info("Running full pipeline...")
        for name, func in phases.items():
            func(conn)

    conn.close()
    log.info("Done!")

    # Show status after run
    show_status()


if __name__ == "__main__":
    main()

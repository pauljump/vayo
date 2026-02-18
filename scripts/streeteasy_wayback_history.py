#!/usr/bin/env python3
"""
StreetEasy Wayback Machine Scraper — 3-Phase Pipeline

Extracts rental/sale price history for NYC units from archived StreetEasy pages.

Phase 1 (index):  Bulk CDX index download — fetches full Wayback Machine index
Phase 2 (queue):  Deduplicates URLs, keeps latest snapshot, builds SQLite work queue
Phase 3 (fetch):  Async fetch & extract with aiohttp workers

Usage:
    python3 scripts/streeteasy_wayback_history.py index
    python3 scripts/streeteasy_wayback_history.py queue
    python3 scripts/streeteasy_wayback_history.py fetch --concurrency 10 --rate 8
    python3 scripts/streeteasy_wayback_history.py status
    python3 scripts/streeteasy_wayback_history.py retry
    python3 scripts/streeteasy_wayback_history.py export --output prices.csv
"""

import argparse
import asyncio
import csv
import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

import aiohttp

CDX_API = "http://web.archive.org/cdx/search/cdx"
WAYBACK_RAW = "http://web.archive.org/web"

DB_PATH = Path(__file__).parent.parent / "se_listings.db"
CDX_DIR = Path(__file__).parent.parent / "data_cache" / "cdx"

# CDX patterns to index
CDX_PATTERNS = [
    "streeteasy.com/building/*",
    "streeteasy.com/rental/*",
    "streeteasy.com/sale/*",
]

# URL regex for unit-level pages (rental/1234 or sale/1234)
UNIT_URL_RE = re.compile(
    r"https?://(?:www\.)?streeteasy\.com/(rental|sale)/(\d+)"
)

# URL regex for building pages
BUILDING_URL_RE = re.compile(
    r"https?://(?:www\.)?streeteasy\.com/building/([\w-]+)"
)

# ── Extraction patterns ─────────────────────────────────────────────

# dataLayer JSON extraction — SE embeds this in a <script> tag
DATALAYER_RE = re.compile(
    r"(?:dataLayer\s*=\s*\[|dataLayer\.push\s*\()\s*(\{.*?\})\s*[\])]",
    re.DOTALL,
)

# PastListingsExperience React JSON blob
PAST_LISTINGS_RE = re.compile(
    r'"PastListingsExperience"[^{]*(\{.*?\})\s*(?:,\s*"|\})',
    re.DOTALL,
)

# Alternative: look for pastListings or listing_history in any JSON blob
PAST_LISTINGS_ARRAY_RE = re.compile(
    r'"(?:pastListings|past_listings|listing_history|priceHistory|price_history)"\s*:\s*(\[.*?\])',
    re.DOTALL,
)

# RSC flight format — self-closing JSON segments
RSC_JSON_RE = re.compile(r'(?:^|\n)\w+:.*?(\{[^{}]{20,}\})', re.MULTILINE)

# HTML table date/price patterns (fallback)
DATE_PATTERNS = ["%m/%d/%Y", "%m/%d/%y", "%b %d, %Y", "%B %d, %Y"]
DATE_RE = re.compile(
    r"\b(\d{1,2}/\d{1,2}/\d{2,4}|"
    r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+\d{1,2},\s+\d{4})\b",
    re.IGNORECASE,
)
PRICE_RE = re.compile(r"\$\s*([0-9][0-9,]*(?:\.[0-9]+)?)")


# ════════════════════════════════════════════════════════════════════
#  Database Setup
# ════════════════════════════════════════════════════════════════════

def ensure_tables(conn: sqlite3.Connection) -> None:
    """Create wb_* tables if they don't exist."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS wb_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT NOT NULL,
            url_type TEXT NOT NULL,          -- 'rental', 'sale', 'building'
            se_id TEXT,                       -- numeric SE id (rental/sale) or slug (building)
            latest_timestamp TEXT NOT NULL,   -- YYYYMMDDHHMMSS
            status TEXT NOT NULL DEFAULT 'pending',  -- pending/fetched/error/skip
            error TEXT,
            fetch_attempts INTEGER DEFAULT 0,
            fetched_at DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(url)
        );
        CREATE INDEX IF NOT EXISTS idx_wb_queue_status ON wb_queue(status);
        CREATE INDEX IF NOT EXISTS idx_wb_queue_type ON wb_queue(url_type);

        CREATE TABLE IF NOT EXISTS wb_unit_metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT NOT NULL,
            se_id TEXT,
            url_type TEXT,
            price REAL,
            beds INTEGER,
            baths REAL,
            sqft REAL,
            ppsf REAL,
            status TEXT,
            listing_type TEXT,
            agent TEXT,
            brokerage TEXT,
            building_name TEXT,
            address TEXT,
            neighborhood TEXT,
            borough TEXT,
            city TEXT,
            state TEXT,
            zipcode TEXT,
            latitude REAL,
            longitude REAL,
            year_built INTEGER,
            building_type TEXT,
            amenities TEXT,
            raw_datalayer TEXT,
            snapshot_timestamp TEXT,
            snapshot_url TEXT,
            extracted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(url)
        );

        CREATE TABLE IF NOT EXISTS wb_price_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT NOT NULL,
            se_id TEXT,
            event_date TEXT,
            event_type TEXT,
            price REAL,
            price_change REAL,
            broker TEXT,
            source TEXT,                     -- 'datalayer', 'past_listings', 'html_table'
            raw_json TEXT,
            extracted_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_wb_price_history_url ON wb_price_history(url);
        CREATE INDEX IF NOT EXISTS idx_wb_price_history_seid ON wb_price_history(se_id);

        CREATE TABLE IF NOT EXISTS wb_discovered_units (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            building_url TEXT NOT NULL,
            unit_url TEXT NOT NULL,
            discovered_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(building_url, unit_url)
        );
    """)
    conn.commit()


# ════════════════════════════════════════════════════════════════════
#  Phase 1: Bulk CDX Index Download
# ════════════════════════════════════════════════════════════════════

async def fetch_cdx_page(
    session: aiohttp.ClientSession,
    pattern: str,
    page: int,
    sem: asyncio.Semaphore,
) -> Optional[str]:
    """Fetch a single CDX page. Returns text or None on error."""
    params = {
        "url": pattern,
        "output": "json",
        "showNumPages": "false",
        "page": str(page),
        "pageSize": "50000",
        "filter": "statuscode:200",
        "fl": "timestamp,original,statuscode,mimetype,length",
    }
    async with sem:
        for attempt in range(3):
            try:
                async with session.get(CDX_API, params=params, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                    if resp.status == 200:
                        return await resp.text()
                    if resp.status in (429, 503):
                        wait = (2 ** attempt) * 5
                        print(f"  CDX {resp.status} on page {page}, waiting {wait}s...")
                        await asyncio.sleep(wait)
                        continue
                    print(f"  CDX error {resp.status} on page {page}")
                    return None
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                wait = (2 ** attempt) * 3
                print(f"  CDX fetch error page {page}: {e}, retrying in {wait}s...")
                await asyncio.sleep(wait)
    return None


async def get_cdx_num_pages(session: aiohttp.ClientSession, pattern: str) -> int:
    """Get total number of CDX pages for a pattern."""
    params = {
        "url": pattern,
        "showNumPages": "true",
        "pageSize": "50000",
        "filter": "statuscode:200",
    }
    for attempt in range(3):
        try:
            async with session.get(CDX_API, params=params, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    return int(text.strip())
                await asyncio.sleep(2 ** attempt)
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
            await asyncio.sleep(2 ** attempt)
    return 0


async def download_cdx_index(concurrency: int = 5) -> None:
    """Phase 1: Download full CDX index for all SE patterns."""
    CDX_DIR.mkdir(parents=True, exist_ok=True)
    sem = asyncio.Semaphore(concurrency)

    async with aiohttp.ClientSession() as session:
        for pattern in CDX_PATTERNS:
            safe_name = pattern.replace("/", "_").replace("*", "STAR")
            pattern_dir = CDX_DIR / safe_name

            print(f"\n{'='*60}")
            print(f"Pattern: {pattern}")

            num_pages = await get_cdx_num_pages(session, pattern)
            print(f"  Total CDX pages: {num_pages}")

            if num_pages == 0:
                print("  No pages found, skipping")
                continue

            pattern_dir.mkdir(parents=True, exist_ok=True)

            # Find pages already downloaded
            existing = set()
            for f in pattern_dir.glob("page_*.jsonl"):
                try:
                    page_num = int(f.stem.split("_")[1])
                    if f.stat().st_size > 0:
                        existing.add(page_num)
                except (ValueError, IndexError):
                    pass

            todo = [p for p in range(num_pages) if p not in existing]
            print(f"  Already downloaded: {len(existing)}, remaining: {len(todo)}")

            if not todo:
                continue

            # Fetch remaining pages with concurrency limit
            batch_size = concurrency * 2
            total_done = 0
            for batch_start in range(0, len(todo), batch_size):
                batch = todo[batch_start:batch_start + batch_size]
                tasks = [
                    fetch_cdx_page(session, pattern, page, sem)
                    for page in batch
                ]
                results = await asyncio.gather(*tasks)

                for page, text in zip(batch, results):
                    if text is not None:
                        out_path = pattern_dir / f"page_{page:04d}.jsonl"
                        out_path.write_text(text)
                        total_done += 1

                print(f"  Progress: {total_done + len(existing)}/{num_pages} pages", end="\r")

            print(f"\n  Done: {total_done} new pages downloaded")

    print(f"\nCDX index complete. Files in {CDX_DIR}")


# ════════════════════════════════════════════════════════════════════
#  Phase 2: Build URL Queue
# ════════════════════════════════════════════════════════════════════

def parse_cdx_line(line: str) -> Optional[Dict[str, str]]:
    """Parse a CDX JSON line or space-separated line."""
    line = line.strip()
    if not line or line.startswith("["):
        return None

    # Try JSON first
    if line.startswith("{"):
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            return None

    # Try JSON array format (CDX returns [[headers], [row], ...])
    if line.startswith("["):
        return None

    # Space-separated: timestamp original statuscode mimetype length
    parts = line.split()
    if len(parts) >= 2:
        return {"timestamp": parts[0], "original": parts[1]}

    return None


def classify_url(url: str) -> Tuple[Optional[str], Optional[str]]:
    """Classify a URL as rental/sale/building and extract its SE id.

    Returns (url_type, se_id) or (None, None) if not a unit/building page.
    """
    # Skip non-unit pages
    lower = url.lower()
    for skip in [
        "/search", "/for-sale", "/for-rent", "/neighborhoods",
        "/no-fee", "/agents", "/login", "/signup", "/blog",
        "/sitemap", "?", "/amenity/", "/reviews", "/floorplans",
    ]:
        if skip in lower:
            return None, None

    m = UNIT_URL_RE.search(url)
    if m:
        return m.group(1), m.group(2)

    m = BUILDING_URL_RE.search(url)
    if m:
        slug = m.group(1)
        # Skip building index/search pages
        if slug in ("search", "no-fee", "featured"):
            return None, None
        return "building", slug

    return None, None


def build_queue(db_path: Path) -> None:
    """Phase 2: Process CDX files into a deduplicated work queue."""
    conn = sqlite3.connect(db_path)
    ensure_tables(conn)

    # Collect: url -> (url_type, se_id, latest_timestamp)
    url_map: Dict[str, Tuple[str, str, str]] = {}
    files_processed = 0
    lines_processed = 0

    for cdx_file in sorted(CDX_DIR.rglob("page_*.jsonl")):
        files_processed += 1
        content = cdx_file.read_text()

        # Handle JSON array format: [[headers], [row1], [row2], ...]
        if content.strip().startswith("["):
            try:
                data = json.loads(content)
                if isinstance(data, list) and len(data) > 1:
                    headers = data[0]
                    ts_idx = headers.index("timestamp") if "timestamp" in headers else 0
                    url_idx = headers.index("original") if "original" in headers else 1
                    for row in data[1:]:
                        if not isinstance(row, list) or len(row) <= max(ts_idx, url_idx):
                            continue
                        timestamp = row[ts_idx]
                        original = row[url_idx]
                        lines_processed += 1

                        url_type, se_id = classify_url(original)
                        if url_type is None:
                            continue

                        # Normalize URL
                        canonical = original.split("?")[0].rstrip("/")
                        if canonical.startswith("http://"):
                            canonical = "https://" + canonical[7:]

                        existing = url_map.get(canonical)
                        if existing is None or timestamp > existing[2]:
                            url_map[canonical] = (url_type, se_id, timestamp)
            except (json.JSONDecodeError, ValueError):
                pass
            continue

        # Line-by-line format
        for line in content.splitlines():
            parsed = parse_cdx_line(line)
            if parsed is None:
                continue
            lines_processed += 1

            timestamp = parsed.get("timestamp", "")
            original = parsed.get("original", "")
            if not timestamp or not original:
                continue

            url_type, se_id = classify_url(original)
            if url_type is None:
                continue

            # Normalize URL
            canonical = original.split("?")[0].rstrip("/")
            if canonical.startswith("http://"):
                canonical = "https://" + canonical[7:]

            existing = url_map.get(canonical)
            if existing is None or timestamp > existing[2]:
                url_map[canonical] = (url_type, se_id, timestamp)

        if files_processed % 100 == 0:
            print(f"  Processed {files_processed} CDX files, {lines_processed} lines, {len(url_map)} unique URLs...", end="\r")

    print(f"\n  Processed {files_processed} CDX files, {lines_processed} lines")
    print(f"  Found {len(url_map)} unique URLs")

    # Count by type
    type_counts: Dict[str, int] = {}
    for url_type, _, _ in url_map.values():
        type_counts[url_type] = type_counts.get(url_type, 0) + 1
    for t, c in sorted(type_counts.items()):
        print(f"    {t}: {c:,}")

    # Check how many already exist in queue
    cur = conn.cursor()
    cur.execute("SELECT url FROM wb_queue")
    existing_urls = {row[0] for row in cur.fetchall()}
    new_urls = {url for url in url_map if url not in existing_urls}
    print(f"  Already in queue: {len(existing_urls):,}, new: {len(new_urls):,}")

    # Insert new URLs
    batch = []
    for url in new_urls:
        url_type, se_id, timestamp = url_map[url]
        batch.append((url, url_type, se_id, timestamp))

    if batch:
        cur.executemany(
            "INSERT OR IGNORE INTO wb_queue (url, url_type, se_id, latest_timestamp) VALUES (?, ?, ?, ?)",
            batch,
        )
        conn.commit()
        print(f"  Inserted {cur.rowcount:,} new URLs into wb_queue")

    conn.close()


# ════════════════════════════════════════════════════════════════════
#  Data Extraction
# ════════════════════════════════════════════════════════════════════

def safe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        if isinstance(val, str):
            val = val.replace(",", "").replace("$", "").strip()
        return float(val) if val else None
    except (ValueError, TypeError):
        return None


def safe_int(val: Any) -> Optional[int]:
    if val is None:
        return None
    try:
        if isinstance(val, str):
            val = val.replace(",", "").strip()
        return int(float(val)) if val else None
    except (ValueError, TypeError):
        return None


def extract_datalayer(html: str) -> Optional[Dict]:
    """Extract the dataLayer object from page HTML."""
    for m in DATALAYER_RE.finditer(html):
        try:
            obj = json.loads(m.group(1))
            # SE dataLayer typically has 'listing' or 'property' or 'event' keys
            if isinstance(obj, dict) and any(
                k in obj for k in (
                    "listing", "property", "ecommerce", "price",
                    "listingPrice", "beds", "propertyType", "address",
                    "buildingName", "neighborhood",
                )
            ):
                return obj
        except json.JSONDecodeError:
            continue
    return None


def extract_past_listings(html: str) -> List[Dict]:
    """Extract PastListingsExperience or similar price history arrays."""
    results = []

    # Try the array pattern first (most common)
    for m in PAST_LISTINGS_ARRAY_RE.finditer(html):
        try:
            arr = json.loads(m.group(1))
            if isinstance(arr, list):
                results.extend(arr)
        except json.JSONDecodeError:
            continue

    if results:
        return results

    # Try PastListingsExperience component props
    for m in PAST_LISTINGS_RE.finditer(html):
        try:
            obj = json.loads(m.group(1))
            if isinstance(obj, dict):
                # Look for listings array within
                for key in ("listings", "pastListings", "items", "history"):
                    if key in obj and isinstance(obj[key], list):
                        results.extend(obj[key])
                if not results and isinstance(obj, dict):
                    results.append(obj)
        except json.JSONDecodeError:
            continue

    if results:
        return results

    # Try to find price history in any JSON blob in the page
    # Look for arrays of objects with date+price keys
    json_blob_re = re.compile(r'(\[\s*\{[^[\]]{10,5000}\}\s*\])')
    for m in json_blob_re.finditer(html):
        try:
            arr = json.loads(m.group(1))
            if not isinstance(arr, list) or len(arr) < 1:
                continue
            # Check if items look like price history
            sample = arr[0]
            if isinstance(sample, dict) and any(
                k in sample for k in ("date", "price", "event", "eventType", "event_type")
            ):
                results.extend(arr)
                break
        except json.JSONDecodeError:
            continue

    return results


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def parse_date(text: str) -> Optional[str]:
    if not text:
        return None
    text = normalize_whitespace(text)
    for fmt in DATE_PATTERNS:
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    m = DATE_RE.search(text)
    if m:
        return parse_date(m.group(1))
    return None


def parse_price(text: str) -> Optional[float]:
    if not text:
        return None
    m = PRICE_RE.search(text)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None


def extract_html_table_history(html: str) -> List[Dict]:
    """Fallback: extract price history from HTML tables."""
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return []

    soup = BeautifulSoup(html, "html.parser")
    records = []

    for table in soup.find_all("table"):
        headers = [normalize_whitespace(th.get_text(" ", strip=True)).lower() for th in table.find_all("th")]
        header_text = " ".join(headers)
        if headers and not ("date" in header_text and ("price" in header_text or "rent" in header_text)):
            continue

        for tr in table.find_all("tr"):
            cells = [normalize_whitespace(td.get_text(" ", strip=True)) for td in tr.find_all("td")]
            if len(cells) < 2:
                continue

            raw_date = next((c for c in cells if DATE_RE.search(c)), None)
            raw_price = next((c for c in cells if "$" in c), None)
            date_iso = parse_date(raw_date) if raw_date else None
            price_val = parse_price(raw_price) if raw_price else None
            event_candidates = [c for c in cells if c not in {raw_date, raw_price}]
            raw_event = event_candidates[0] if event_candidates else None

            if date_iso or price_val or raw_event:
                records.append({
                    "event_date": date_iso,
                    "event_type": raw_event,
                    "price": price_val,
                    "source": "html_table",
                })

    return records


def extract_all(html: str, url: str) -> Tuple[Optional[Dict], List[Dict]]:
    """Extract metadata and price history from page HTML.

    Returns (metadata_dict, price_history_list).
    """
    metadata = {}
    price_history = []

    # 1. dataLayer extraction
    dl = extract_datalayer(html)
    if dl:
        # Flatten nested structures — SE uses various layouts
        flat = {}
        for key, val in dl.items():
            if isinstance(val, dict):
                for k2, v2 in val.items():
                    flat[k2] = v2
            else:
                flat[key] = val

        metadata = {
            "price": safe_float(flat.get("price") or flat.get("listingPrice") or flat.get("listing_price")),
            "beds": safe_int(flat.get("beds") or flat.get("bedrooms") or flat.get("numBedrooms")),
            "baths": safe_float(flat.get("baths") or flat.get("bathrooms") or flat.get("numBathrooms")),
            "sqft": safe_float(flat.get("sqft") or flat.get("squareFeet") or flat.get("area")),
            "ppsf": safe_float(flat.get("ppsf") or flat.get("pricePerSqFt")),
            "status": flat.get("status") or flat.get("listingStatus"),
            "listing_type": flat.get("listingType") or flat.get("listing_type") or flat.get("propertyType"),
            "agent": flat.get("agent") or flat.get("agentName") or flat.get("listing_agent"),
            "brokerage": flat.get("brokerage") or flat.get("brokerageName") or flat.get("office"),
            "building_name": flat.get("buildingName") or flat.get("building_name"),
            "address": flat.get("address") or flat.get("streetAddress"),
            "neighborhood": flat.get("neighborhood") or flat.get("area_name"),
            "borough": flat.get("borough") or flat.get("city"),
            "city": flat.get("city"),
            "state": flat.get("state"),
            "zipcode": flat.get("zipcode") or flat.get("postalCode"),
            "latitude": safe_float(flat.get("latitude") or flat.get("lat")),
            "longitude": safe_float(flat.get("longitude") or flat.get("lng") or flat.get("lon")),
            "year_built": safe_int(flat.get("yearBuilt") or flat.get("year_built")),
            "building_type": flat.get("buildingType") or flat.get("building_type"),
            "amenities": json.dumps(flat.get("amenities")) if flat.get("amenities") else None,
            "raw_datalayer": json.dumps(dl),
        }

    # 2. PastListingsExperience / price history JSON
    past = extract_past_listings(html)
    for item in past:
        if not isinstance(item, dict):
            continue
        event_date = (
            item.get("date") or item.get("eventDate") or
            item.get("event_date") or item.get("closingDate") or
            item.get("listedDate")
        )
        if event_date:
            event_date = parse_date(str(event_date)) or str(event_date)

        event_type = (
            item.get("event") or item.get("eventType") or
            item.get("event_type") or item.get("type") or
            item.get("status")
        )

        price = safe_float(
            item.get("price") or item.get("listPrice") or
            item.get("salePrice") or item.get("amount")
        )

        price_change = safe_float(item.get("priceChange") or item.get("price_change"))
        broker = item.get("broker") or item.get("agent") or item.get("brokerage")

        price_history.append({
            "event_date": event_date,
            "event_type": str(event_type) if event_type else None,
            "price": price,
            "price_change": price_change,
            "broker": str(broker) if broker else None,
            "source": "past_listings",
            "raw_json": json.dumps(item),
        })

    # 3. Fallback: HTML table extraction
    if not price_history:
        table_rows = extract_html_table_history(html)
        price_history.extend(table_rows)

    return metadata if metadata else None, price_history


def extract_unit_urls_from_building(html: str) -> Set[str]:
    """Extract unit URLs from a building page snapshot."""
    urls = set()
    for m in re.finditer(r"(?:https?://(?:www\.)?streeteasy\.com)?/(rental|sale)/(\d+)", html):
        urls.add(f"https://www.streeteasy.com/{m.group(1)}/{m.group(2)}")
    return urls


# ════════════════════════════════════════════════════════════════════
#  Phase 3: Async Fetch & Extract
# ════════════════════════════════════════════════════════════════════

class RateLimiter:
    """Token bucket rate limiter for async operations."""

    def __init__(self, rate: float):
        self.rate = rate
        self.interval = 1.0 / rate
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def acquire(self):
        async with self._lock:
            now = asyncio.get_event_loop().time()
            wait = self._last + self.interval - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._last = asyncio.get_event_loop().time()


async def fetch_one(
    session: aiohttp.ClientSession,
    url: str,
    timestamp: str,
    rate_limiter: RateLimiter,
) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    """Fetch a single Wayback page. Returns (html, status_code, error)."""
    wb_url = f"{WAYBACK_RAW}/{timestamp}id_/{url}"

    await rate_limiter.acquire()

    for attempt in range(4):
        try:
            async with session.get(
                wb_url,
                timeout=aiohttp.ClientTimeout(total=60),
                headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"},
            ) as resp:
                if resp.status == 200:
                    html = await resp.text(errors="replace")
                    return html, 200, None
                if resp.status in (429, 503):
                    wait = (2 ** attempt) * 5
                    await asyncio.sleep(wait)
                    continue
                if resp.status == 404:
                    return None, 404, "not_found"
                return None, resp.status, f"http_{resp.status}"
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt < 3:
                await asyncio.sleep(2 ** attempt)
                continue
            return None, None, str(e)

    return None, None, "max_retries"


async def worker(
    worker_id: int,
    queue: asyncio.Queue,
    session: aiohttp.ClientSession,
    db_path: Path,
    rate_limiter: RateLimiter,
    stats: Dict[str, int],
):
    """Worker coroutine that fetches and processes URLs from the queue."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            break

        row_id, url, url_type, se_id, timestamp = item

        try:
            html, status_code, error = await fetch_one(session, url, timestamp, rate_limiter)

            if error:
                conn.execute(
                    "UPDATE wb_queue SET status='error', error=?, fetch_attempts=fetch_attempts+1, fetched_at=CURRENT_TIMESTAMP WHERE id=?",
                    (error, row_id),
                )
                conn.commit()
                stats["errors"] += 1
                queue.task_done()
                continue

            # Extract data
            metadata, price_history = extract_all(html, url)
            snapshot_url = f"{WAYBACK_RAW}/{timestamp}id_/{url}"

            # Save metadata
            if metadata:
                cols = ["url", "se_id", "url_type", "snapshot_timestamp", "snapshot_url"]
                vals = [url, se_id, url_type, timestamp, snapshot_url]
                for key in (
                    "price", "beds", "baths", "sqft", "ppsf", "status", "listing_type",
                    "agent", "brokerage", "building_name", "address", "neighborhood",
                    "borough", "city", "state", "zipcode", "latitude", "longitude",
                    "year_built", "building_type", "amenities", "raw_datalayer",
                ):
                    if metadata.get(key) is not None:
                        cols.append(key)
                        vals.append(metadata[key])

                placeholders = ",".join(["?"] * len(cols))
                col_names = ",".join(cols)
                # Use INSERT OR REPLACE to update if already exists
                conn.execute(
                    f"INSERT OR REPLACE INTO wb_unit_metadata ({col_names}) VALUES ({placeholders})",
                    vals,
                )

            # Save price history
            if price_history:
                for ph in price_history:
                    conn.execute(
                        "INSERT INTO wb_price_history (url, se_id, event_date, event_type, price, price_change, broker, source, raw_json) VALUES (?,?,?,?,?,?,?,?,?)",
                        (
                            url, se_id,
                            ph.get("event_date"),
                            ph.get("event_type"),
                            ph.get("price"),
                            ph.get("price_change"),
                            ph.get("broker"),
                            ph.get("source"),
                            ph.get("raw_json"),
                        ),
                    )

            # If building page, extract discovered unit URLs
            if url_type == "building":
                discovered = extract_unit_urls_from_building(html)
                for unit_url in discovered:
                    conn.execute(
                        "INSERT OR IGNORE INTO wb_discovered_units (building_url, unit_url) VALUES (?, ?)",
                        (url, unit_url),
                    )

            # Mark as fetched
            conn.execute(
                "UPDATE wb_queue SET status='fetched', fetch_attempts=fetch_attempts+1, fetched_at=CURRENT_TIMESTAMP WHERE id=?",
                (row_id,),
            )
            conn.commit()

            has_data = bool(metadata) or bool(price_history)
            stats["fetched"] += 1
            if has_data:
                stats["with_data"] += 1
            if price_history:
                stats["history_rows"] += len(price_history)

        except Exception as e:
            try:
                conn.execute(
                    "UPDATE wb_queue SET status='error', error=?, fetch_attempts=fetch_attempts+1, fetched_at=CURRENT_TIMESTAMP WHERE id=?",
                    (str(e)[:500], row_id),
                )
                conn.commit()
            except Exception:
                pass
            stats["errors"] += 1

        queue.task_done()

    conn.close()


async def run_fetch(
    db_path: Path,
    concurrency: int = 10,
    rate: float = 8.0,
    limit: Optional[int] = None,
    url_type: Optional[str] = None,
) -> None:
    """Phase 3: Fetch and extract data from Wayback snapshots."""
    conn = sqlite3.connect(db_path)
    ensure_tables(conn)

    # Get pending items
    query = "SELECT id, url, url_type, se_id, latest_timestamp FROM wb_queue WHERE status='pending'"
    params = []
    if url_type:
        query += " AND url_type=?"
        params.append(url_type)
    query += " ORDER BY id"
    if limit:
        query += " LIMIT ?"
        params.append(limit)

    rows = conn.execute(query, params).fetchall()
    total = len(rows)
    conn.close()

    if total == 0:
        print("No pending URLs to fetch.")
        return

    print(f"Fetching {total:,} URLs with {concurrency} workers at {rate} req/sec")

    rate_limiter = RateLimiter(rate)
    work_queue: asyncio.Queue = asyncio.Queue()
    stats = {"fetched": 0, "errors": 0, "with_data": 0, "history_rows": 0}

    for row in rows:
        await work_queue.put(row)

    # Add poison pills
    for _ in range(concurrency):
        await work_queue.put(None)

    start_time = time.time()

    async with aiohttp.ClientSession() as session:
        workers = [
            asyncio.create_task(worker(i, work_queue, session, db_path, rate_limiter, stats))
            for i in range(concurrency)
        ]

        # Progress reporter
        while not all(w.done() for w in workers):
            await asyncio.sleep(5)
            done = stats["fetched"] + stats["errors"]
            elapsed = time.time() - start_time
            rate_actual = done / elapsed if elapsed > 0 else 0
            eta_s = (total - done) / rate_actual if rate_actual > 0 else 0
            eta_m = eta_s / 60
            print(
                f"  Progress: {done:,}/{total:,} ({done*100/total:.1f}%) | "
                f"data: {stats['with_data']:,} | history: {stats['history_rows']:,} | "
                f"errors: {stats['errors']:,} | "
                f"{rate_actual:.1f} req/s | ETA: {eta_m:.0f}m",
                end="\r",
            )

        await asyncio.gather(*workers)

    elapsed = time.time() - start_time
    print(f"\n\nDone in {elapsed/60:.1f} minutes")
    print(f"  Fetched: {stats['fetched']:,}")
    print(f"  With data: {stats['with_data']:,}")
    print(f"  History rows: {stats['history_rows']:,}")
    print(f"  Errors: {stats['errors']:,}")


# ════════════════════════════════════════════════════════════════════
#  Status / Retry / Export
# ════════════════════════════════════════════════════════════════════

def show_status(db_path: Path) -> None:
    """Show current pipeline status."""
    conn = sqlite3.connect(db_path)
    ensure_tables(conn)

    print("=" * 60)
    print("StreetEasy Wayback Pipeline Status")
    print("=" * 60)

    # CDX index status
    print("\n── CDX Index ──")
    if CDX_DIR.exists():
        for pattern_dir in sorted(CDX_DIR.iterdir()):
            if pattern_dir.is_dir():
                files = list(pattern_dir.glob("page_*.jsonl"))
                total_size = sum(f.stat().st_size for f in files)
                print(f"  {pattern_dir.name}: {len(files)} pages ({total_size/1024/1024:.1f} MB)")
    else:
        print("  No CDX index downloaded yet. Run: index")

    # Queue status
    print("\n── Work Queue ──")
    cur = conn.execute("SELECT status, COUNT(*) FROM wb_queue GROUP BY status ORDER BY status")
    total_queue = 0
    for status, count in cur:
        print(f"  {status}: {count:,}")
        total_queue += count
    if total_queue == 0:
        print("  Empty. Run: queue")
    else:
        print(f"  Total: {total_queue:,}")

    # Queue by type
    cur = conn.execute("SELECT url_type, COUNT(*) FROM wb_queue GROUP BY url_type ORDER BY url_type")
    rows = cur.fetchall()
    if rows:
        print("\n  By type:")
        for url_type, count in rows:
            print(f"    {url_type}: {count:,}")

    # Extracted data stats
    print("\n── Extracted Data ──")
    cur = conn.execute("SELECT COUNT(*) FROM wb_unit_metadata")
    meta_count = cur.fetchone()[0]
    print(f"  Unit metadata rows: {meta_count:,}")

    cur = conn.execute("SELECT COUNT(*) FROM wb_price_history")
    hist_count = cur.fetchone()[0]
    print(f"  Price history rows: {hist_count:,}")

    cur = conn.execute("SELECT COUNT(*) FROM wb_discovered_units")
    disc_count = cur.fetchone()[0]
    print(f"  Discovered units: {disc_count:,}")

    # Price history by source
    cur = conn.execute("SELECT source, COUNT(*) FROM wb_price_history GROUP BY source ORDER BY source")
    rows = cur.fetchall()
    if rows:
        print("\n  Price history by source:")
        for source, count in rows:
            print(f"    {source}: {count:,}")

    # Error summary
    cur = conn.execute(
        "SELECT error, COUNT(*) FROM wb_queue WHERE status='error' GROUP BY error ORDER BY COUNT(*) DESC LIMIT 10"
    )
    errors = cur.fetchall()
    if errors:
        print("\n── Top Errors ──")
        for error, count in errors:
            print(f"  {count:,}: {error[:80]}")

    conn.close()


def retry_errors(db_path: Path, max_attempts: int = 3) -> None:
    """Reset errored items back to pending for retry."""
    conn = sqlite3.connect(db_path)
    ensure_tables(conn)

    cur = conn.execute(
        "UPDATE wb_queue SET status='pending' WHERE status='error' AND fetch_attempts < ?",
        (max_attempts,),
    )
    conn.commit()
    print(f"Reset {cur.rowcount:,} errored items to pending (max attempts: {max_attempts})")
    conn.close()


def export_data(db_path: Path, output: str) -> None:
    """Export price history to CSV."""
    conn = sqlite3.connect(db_path)
    ensure_tables(conn)

    cur = conn.execute("""
        SELECT
            ph.url, ph.se_id, ph.event_date, ph.event_type, ph.price,
            ph.price_change, ph.broker, ph.source,
            m.beds, m.baths, m.sqft, m.address, m.neighborhood, m.borough,
            m.building_name, m.building_type
        FROM wb_price_history ph
        LEFT JOIN wb_unit_metadata m ON ph.url = m.url
        ORDER BY ph.url, ph.event_date
    """)

    fieldnames = [
        "url", "se_id", "event_date", "event_type", "price",
        "price_change", "broker", "source",
        "beds", "baths", "sqft", "address", "neighborhood", "borough",
        "building_name", "building_type",
    ]

    out_path = Path(output)
    count = 0
    with out_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(fieldnames)
        for row in cur:
            writer.writerow(row)
            count += 1

    print(f"Exported {count:,} rows to {out_path}")
    conn.close()


# ════════════════════════════════════════════════════════════════════
#  CLI
# ════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="StreetEasy Wayback Machine Scraper — 3-Phase Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  index     Download CDX index from Wayback Machine (~45 min)
  queue     Build work queue from CDX index (seconds)
  fetch     Fetch and extract data from archived pages (resumable)
  status    Show pipeline progress
  retry     Reset errored items for retry
  export    Export price history to CSV
        """,
    )

    subparsers = parser.add_subparsers(dest="command")

    # index
    idx_parser = subparsers.add_parser("index", help="Download CDX index")
    idx_parser.add_argument("--concurrency", type=int, default=5, help="Concurrent CDX requests (default: 5)")

    # queue
    q_parser = subparsers.add_parser("queue", help="Build work queue from CDX index")
    q_parser.add_argument("--db", default=str(DB_PATH), help=f"SQLite DB path (default: {DB_PATH})")

    # fetch
    f_parser = subparsers.add_parser("fetch", help="Fetch and extract archived pages")
    f_parser.add_argument("--db", default=str(DB_PATH), help=f"SQLite DB path (default: {DB_PATH})")
    f_parser.add_argument("--concurrency", type=int, default=10, help="Concurrent workers (default: 10)")
    f_parser.add_argument("--rate", type=float, default=8.0, help="Max requests/sec (default: 8)")
    f_parser.add_argument("--limit", type=int, help="Limit number of URLs to fetch")
    f_parser.add_argument("--type", dest="url_type", choices=["rental", "sale", "building"], help="Filter by URL type")

    # status
    s_parser = subparsers.add_parser("status", help="Show pipeline progress")
    s_parser.add_argument("--db", default=str(DB_PATH), help=f"SQLite DB path (default: {DB_PATH})")

    # retry
    r_parser = subparsers.add_parser("retry", help="Reset errored items for retry")
    r_parser.add_argument("--db", default=str(DB_PATH), help=f"SQLite DB path (default: {DB_PATH})")
    r_parser.add_argument("--max-attempts", type=int, default=3, help="Max attempts before skipping (default: 3)")

    # export
    e_parser = subparsers.add_parser("export", help="Export price history to CSV")
    e_parser.add_argument("--db", default=str(DB_PATH), help=f"SQLite DB path (default: {DB_PATH})")
    e_parser.add_argument("--output", "-o", default="streeteasy_prices.csv", help="Output CSV path")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    if args.command == "index":
        asyncio.run(download_cdx_index(concurrency=args.concurrency))

    elif args.command == "queue":
        build_queue(Path(args.db))

    elif args.command == "fetch":
        asyncio.run(run_fetch(
            db_path=Path(args.db),
            concurrency=args.concurrency,
            rate=args.rate,
            limit=args.limit,
            url_type=args.url_type,
        ))

    elif args.command == "status":
        show_status(Path(args.db))

    elif args.command == "retry":
        retry_errors(Path(args.db), max_attempts=args.max_attempts)

    elif args.command == "export":
        export_data(Path(args.db), args.output)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Unified apartment listing puller — plugin architecture for multiple NYC sources.

Sources:
  - stuytown:  Beam Living / StuyTown public JSON API (5 properties, ~240 units)
  - durst:     Durst MRI ProspectConnect HTML scraper (7 buildings, ~80 units)
  - glenwood:  Glenwood Management WordPress HTML scraper (26 buildings, ~30 units)

Usage:
  python3 scripts/pull_unified.py                   # pull all sources
  python3 scripts/pull_unified.py --source stuytown  # pull one source
  python3 scripts/pull_unified.py --source durst
  python3 scripts/pull_unified.py --source glenwood
  python3 scripts/pull_unified.py --status           # show counts
  python3 scripts/pull_unified.py --reset stuytown   # clear + re-pull one source
"""

import argparse, json, os, re, sqlite3, sys, time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from html.parser import HTMLParser
from http.cookiejar import CookieJar
from urllib.request import build_opener, HTTPCookieProcessor

DB_PATH = Path(__file__).resolve().parent.parent / "pullers.db"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"


# ─── Database ────────────────────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS listings (
            source          TEXT NOT NULL,
            source_id       TEXT NOT NULL,
            building_name   TEXT,
            address         TEXT,
            unit_number     TEXT,
            bedrooms        REAL,
            bathrooms       REAL,
            price           REAL,
            sqft            REAL,
            available_date  TEXT,
            lease_terms     TEXT,   -- JSON: [{term, price}, ...]
            amenities       TEXT,   -- JSON array
            description     TEXT,
            floor_plan_url  TEXT,
            image_urls      TEXT,   -- JSON array
            latitude        REAL,
            longitude       REAL,
            neighborhood    TEXT,
            borough         TEXT,
            zipcode         TEXT,
            is_flex         INTEGER,
            is_rent_stabilized INTEGER,
            finish_level    TEXT,
            raw_json        TEXT,
            first_seen      TEXT NOT NULL,
            last_seen       TEXT NOT NULL,
            status          TEXT DEFAULT 'active',
            PRIMARY KEY (source, source_id)
        );

        CREATE INDEX IF NOT EXISTS idx_listings_source ON listings(source);
        CREATE INDEX IF NOT EXISTS idx_listings_status ON listings(status);
        CREATE INDEX IF NOT EXISTS idx_listings_address ON listings(address);
        CREATE INDEX IF NOT EXISTS idx_listings_building ON listings(building_name);
        CREATE INDEX IF NOT EXISTS idx_listings_price ON listings(price);
        CREATE INDEX IF NOT EXISTS idx_listings_bedrooms ON listings(bedrooms);

        CREATE TABLE IF NOT EXISTS price_history (
            source       TEXT NOT NULL,
            source_id    TEXT NOT NULL,
            snapshot_date TEXT NOT NULL,
            price        REAL,
            lease_terms  TEXT,
            status       TEXT,
            PRIMARY KEY (source, source_id, snapshot_date)
        );

        CREATE TABLE IF NOT EXISTS pull_log (
            source       TEXT NOT NULL,
            pulled_at    TEXT NOT NULL,
            listings_count INTEGER,
            new_count    INTEGER,
            updated_count INTEGER
        );
    """)
    conn.commit()
    return conn


def upsert_listings(conn, listings, source):
    """Insert or update listings, tracking first_seen/last_seen and price history."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    new_count = 0
    updated_count = 0

    for item in listings:
        item["source"] = source
        item["last_seen"] = now

        existing = conn.execute(
            "SELECT price, first_seen FROM listings WHERE source=? AND source_id=?",
            (source, item["source_id"])
        ).fetchone()

        if existing is None:
            item["first_seen"] = now
            item["status"] = "active"
            new_count += 1
        else:
            item["first_seen"] = existing[1]
            item["status"] = "active"
            updated_count += 1

        cols = list(item.keys())
        placeholders = ",".join(["?"] * len(cols))
        col_names = ",".join(cols)
        updates = ",".join(
            f"{c}=excluded.{c}" for c in cols
            if c not in ("source", "source_id", "first_seen")
        )
        conn.execute(
            f"INSERT INTO listings ({col_names}) VALUES ({placeholders}) "
            f"ON CONFLICT(source, source_id) DO UPDATE SET {updates}",
            tuple(item.get(c) for c in cols)
        )

        # Price snapshot
        conn.execute(
            "INSERT OR REPLACE INTO price_history (source, source_id, snapshot_date, price, lease_terms, status) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (source, item["source_id"], today, item.get("price"), item.get("lease_terms"), "active")
        )

    # Mark listings not seen this pull as inactive
    conn.execute(
        "UPDATE listings SET status='inactive' WHERE source=? AND last_seen < ?",
        (source, now)
    )

    conn.execute(
        "INSERT INTO pull_log (source, pulled_at, listings_count, new_count, updated_count) "
        "VALUES (?, ?, ?, ?, ?)",
        (source, now, len(listings), new_count, updated_count)
    )
    conn.commit()
    return new_count, updated_count


# ─── HTTP helpers ────────────────────────────────────────────────────────────

def fetch(url, headers=None, data=None, method=None, timeout=30):
    """Simple HTTP fetch with retries."""
    hdrs = {"User-Agent": UA}
    if headers:
        hdrs.update(headers)
    for attempt in range(4):
        try:
            req = Request(url, data=data, headers=hdrs, method=method)
            with urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            if attempt == 3:
                raise
            time.sleep(2 * (attempt + 1))
    return ""


# ─── Base Adapter ────────────────────────────────────────────────────────────

class SourceAdapter(ABC):
    """Base class for all listing source adapters."""

    name: str = ""
    description: str = ""

    @abstractmethod
    def pull(self) -> list[dict]:
        """Pull all current listings. Returns list of dicts with standard fields."""
        ...

    def run(self, conn):
        """Pull listings and upsert into DB."""
        print(f"\n{'='*60}")
        print(f"  Pulling: {self.name} — {self.description}")
        print(f"{'='*60}")
        t0 = time.monotonic()
        listings = self.pull()
        elapsed = time.monotonic() - t0
        if not listings:
            print(f"  No listings found ({elapsed:.1f}s)")
            return
        new, updated = upsert_listings(conn, listings, self.name)
        total = len(listings)
        print(f"  {total} listings ({new} new, {updated} updated) in {elapsed:.1f}s")


# ─── StuyTown Adapter ───────────────────────────────────────────────────────

class StuyTownAdapter(SourceAdapter):
    name = "stuytown"
    description = "Beam Living (StuyTown, PCV, Kips Bay, Parker Towers, 8 Spruce)"

    API_URL = "https://units.stuytown.com/api/units?itemsOnPage=500&Order=low-price"

    PROPERTY_HOODS = {
        "Stuyvesant Town": ("Gramercy Park", "Manhattan"),
        "Peter Cooper Village": ("Gramercy Park", "Manhattan"),
        "Kips Bay Court": ("Kips Bay", "Manhattan"),
        "Parker Towers": ("Forest Hills", "Queens"),
        "8 Spruce": ("Financial District", "Manhattan"),
    }

    def pull(self):
        raw = fetch(self.API_URL)
        data = json.loads(raw)
        units = data.get("unitModels", [])
        print(f"  API returned {len(units)} units")

        listings = []
        for u in units:
            if not u.get("isAvailable"):
                continue
            bldg = u.get("building", {})
            prop = u.get("property", {})
            prop_name = prop.get("name", "")
            hood, boro = self.PROPERTY_HOODS.get(prop_name, ("", ""))

            rates = u.get("unitRates") or {}
            lease_terms = json.dumps([
                {"term": f"{k} months", "price": v}
                for k, v in sorted(rates.items(), key=lambda x: int(x[0]))
            ]) if rates else None

            amenities = json.dumps([
                a.get("friendlyDescription") or a.get("description", "")
                for a in (u.get("amenities") or [])
            ])

            raw_images = u.get("images") or []
            images = json.dumps([
                img if isinstance(img, str) else (img.get("url", "") if isinstance(img, dict) else str(img))
                for img in raw_images
            ]) if raw_images else None

            listings.append({
                "source_id": u.get("unitSpk", ""),
                "building_name": f"{prop_name} — {bldg.get('name', '')}".strip(" —"),
                "address": bldg.get("address", ""),
                "unit_number": u.get("unitNumber", ""),
                "bedrooms": u.get("bedrooms", 0),
                "bathrooms": u.get("bathrooms"),
                "price": u.get("price"),
                "sqft": u.get("sqft"),
                "available_date": u.get("availableDate", "")[:10] if u.get("availableDate") else None,
                "lease_terms": lease_terms,
                "amenities": amenities,
                "description": u.get("description", ""),
                "floor_plan_url": None,
                "image_urls": images,
                "latitude": float(bldg["latitude"]) if bldg.get("latitude") else None,
                "longitude": float(bldg["longitude"]) if bldg.get("longitude") else None,
                "neighborhood": hood,
                "borough": boro,
                "zipcode": None,
                "is_flex": 1 if u.get("isFlex") else 0,
                "is_rent_stabilized": 1 if u.get("isCapped") else 0,
                "finish_level": (u.get("finish") or {}).get("name"),
                "raw_json": json.dumps(u),
            })

        return listings


# ─── Durst Adapter ───────────────────────────────────────────────────────────

class DurstAdapter(SourceAdapter):
    name = "durst"
    description = "Durst Organization — MRI ProspectConnect (7 Manhattan/Queens buildings)"

    BASE = "https://durst.mriprospectconnect.com"

    PROPERTIES = [
        {"id": "44001", "name": "VIA 57 West", "address": "625 West 57th Street", "hood": "Hell's Kitchen", "boro": "Manhattan"},
        {"id": "44301", "name": "Frank 57 West", "address": "600 West 58th Street", "hood": "Hell's Kitchen", "boro": "Manhattan"},
        {"id": "36601", "name": "Helena 57 West", "address": "601 West 57th Street", "hood": "Hell's Kitchen", "boro": "Manhattan"},
        {"id": "52501", "name": "Sven", "address": "500 West 56th Street", "hood": "Hell's Kitchen", "boro": "Manhattan"},
        {"id": "40101", "name": "EOS", "address": "100 West 31st Street", "hood": "Chelsea", "boro": "Manhattan"},
        {"id": "49501", "name": "Halletts Point 10", "address": "10 Halletts Point", "hood": "Astoria", "boro": "Queens"},
        {"id": "47701", "name": "Halletts Point 20", "address": "20 Halletts Point", "hood": "Astoria", "boro": "Queens"},
    ]

    def _get_csrf_and_cookies(self, prop_id):
        """Fetch search index page to get CSRF token and session cookies."""
        cj = CookieJar()
        opener = build_opener(HTTPCookieProcessor(cj))
        opener.addheaders = [("User-Agent", UA)]
        url = f"{self.BASE}/Search/Index/{prop_id}/"
        resp = opener.open(url, timeout=30)
        html = resp.read().decode("utf-8", errors="replace")

        # Extract __RequestVerificationToken from hidden form field
        m = re.search(r'name="__RequestVerificationToken"\s+.*?value="([^"]+)"', html)
        if not m:
            m = re.search(r'value="([^"]+)".*?name="__RequestVerificationToken"', html)
        token = m.group(1) if m else ""

        return token, cj, opener

    def _search_property(self, prop):
        """Search one property and return list of unit dicts parsed from HTML."""
        prop_id = prop["id"]
        token, cj, opener = self._get_csrf_and_cookies(prop_id)

        form_data = urlencode({
            "__RequestVerificationToken": token,
            "Community": prop_id,
            "Bedroom": "-2",        # all bedrooms
            "ApartmentNumber": "",
        }).encode("utf-8")

        req = Request(
            f"{self.BASE}/Search/Search",
            data=form_data,
            headers={
                "User-Agent": UA,
                "X-Requested-With": "XMLHttpRequest",
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": f"{self.BASE}/Search/Index/{prop_id}/",
            },
            method="POST",
        )
        resp = opener.open(req, timeout=30)
        html = resp.read().decode("utf-8", errors="replace")
        return self._parse_units_html(html, prop)

    def _parse_units_html(self, html, prop):
        """Parse MRI ProspectConnect search results HTML into unit dicts.

        Structure: pc-card sections contain a header with bed/bath type,
        then a table of individual units (data-unitid) with sqft, available
        date, lease terms (<option> elements), and floor plan images.

        Buildings with only waitlist units (data-unittypeid but no data-unitid)
        are rent-stabilized and have no market-rate availability.
        """
        units = []

        # Split HTML into sections by unit-type card headers
        # Each pc-card has a title like "Studio 1 Bath" or "2 Bed 2 Bath"
        sections = re.split(r'<h4\s+class="pc-card-title">', html)

        current_beds, current_baths = 0, 1

        for section in sections[1:]:  # skip preamble before first card
            # Parse bed/bath from card title
            title_m = re.match(r'(.*?)</h4>', section, re.DOTALL)
            if title_m:
                title = title_m.group(1).strip()
                if "studio" in title.lower():
                    current_beds = 0
                else:
                    bm = re.search(r'(\d+)\s*bed', title, re.I)
                    if bm:
                        current_beds = int(bm.group(1))
                btm = re.search(r'(\d+)\s*bath', title, re.I)
                if btm:
                    current_baths = int(btm.group(1))

            # Find all units in this section
            unit_rows = re.findall(
                r'data-unitid="(\d+)"(.*?)(?=data-unitid="|<h4\s|$)',
                section, re.DOTALL
            )

            for uid, block in unit_rows:
                # Available date
                avail_m = re.search(r'data-available-date="([^"]+)"', block)
                avail = avail_m.group(1) if avail_m else None

                # Unit number (data-title is the display name)
                title_m = re.search(r'data-title="([^"]+)"', block)
                unit_num = title_m.group(1) if title_m else uid

                # Sqft (handles commas like "1,020")
                sqft_m = re.search(r'data-th="Sqft"[^>]*>\s*([\d,]+)', block)
                sqft = int(sqft_m.group(1).replace(",", "")) if sqft_m else None

                # Floor plan image
                fp_m = re.search(r'data-src="(https://[^"]+)"', block)
                fp_url = fp_m.group(1) if fp_m else None

                # Lease terms from <option> elements:
                # <option value="24">24 Months (3310.00 USD)</option>
                lease_terms = []
                for om in re.finditer(
                    r'<option\s+value="(\d+)">\s*(\d+)\s+Months?\s+\(([\d,.]+)\s+USD\)',
                    block, re.I
                ):
                    term_months = int(om.group(2))
                    price_val = float(om.group(3).replace(",", ""))
                    lease_terms.append({
                        "term": f"{term_months} months",
                        "price": int(price_val)
                    })

                # Primary price = 12-month lease, or shortest available
                price = None
                if lease_terms:
                    twelve = [t for t in lease_terms if t["term"] == "12 months"]
                    price = twelve[0]["price"] if twelve else min(t["price"] for t in lease_terms)

                # Check rent stabilization (DHCR)
                dhcr_m = re.search(r'data-all-dhcr-units="True"', block)
                is_stabilized = 1 if dhcr_m else 0

                units.append({
                    "source_id": f"durst-{prop['id']}-{uid}",
                    "building_name": prop["name"],
                    "address": prop["address"],
                    "unit_number": unit_num,
                    "bedrooms": current_beds,
                    "bathrooms": current_baths,
                    "price": price,
                    "sqft": sqft,
                    "available_date": avail,
                    "lease_terms": json.dumps(lease_terms) if lease_terms else None,
                    "amenities": None,
                    "description": None,
                    "floor_plan_url": fp_url,
                    "image_urls": None,
                    "latitude": None,
                    "longitude": None,
                    "neighborhood": prop["hood"],
                    "borough": prop["boro"],
                    "zipcode": None,
                    "is_flex": 0,
                    "is_rent_stabilized": is_stabilized,
                    "finish_level": None,
                    "raw_json": None,
                })

        return units

    def pull(self):
        all_units = []
        for prop in self.PROPERTIES:
            try:
                units = self._search_property(prop)
                print(f"  {prop['name']}: {len(units)} units")
                all_units.extend(units)
            except Exception as e:
                print(f"  {prop['name']}: ERROR — {e}")
        return all_units


# ─── Glenwood Adapter ────────────────────────────────────────────────────────

class GlenwoodAdapter(SourceAdapter):
    name = "glenwood"
    description = "Glenwood Management — 26 luxury Manhattan buildings"

    BASE = "https://www.glenwoodnyc.com"

    BUILDINGS = [
        {"slug": "downtown/barclay-tower", "bid": "107", "name": "Barclay Tower", "addr": "10 Barclay Street", "hood": "Financial District"},
        {"slug": "downtown/liberty-plaza", "bid": "19", "name": "Liberty Plaza", "addr": "10 Liberty Street", "hood": "Financial District"},
        {"slug": "downtown/tribeca-bridge-tower", "bid": "18", "name": "Tribeca Bridge Tower", "addr": "450 North End Avenue", "hood": "Tribeca"},
        {"slug": "midtown-east/paramount-tower", "bid": "17", "name": "Paramount Tower", "addr": "240 East 39th Street", "hood": "Murray Hill"},
        {"slug": "midtown-east/the-bamford", "bid": "14", "name": "The Bamford", "addr": "333 East 56th Street", "hood": "Midtown East"},
        {"slug": "midtown-east/the-belmont", "bid": "15", "name": "The Belmont", "addr": "320 East 46th Street", "hood": "Midtown East"},
        {"slug": "midtown-east/the-bristol", "bid": "23", "name": "The Bristol", "addr": "300 East 56th Street", "hood": "Midtown East"},
        {"slug": "midtown-west/crystal-green", "bid": "35", "name": "Crystal Green", "addr": "330 West 39th Street", "hood": "Midtown West"},
        {"slug": "midtown-west/emerald-green", "bid": "109", "name": "Emerald Green", "addr": "320 West 38th Street", "hood": "Midtown West"},
        {"slug": "midtown-west/the-sage", "bid": "118", "name": "The Sage", "addr": "329 West 38th Street", "hood": "Midtown West"},
        {"slug": "riverdale/briar-hill", "bid": "20", "name": "Briar Hill", "addr": "600 West 246th Street", "hood": "Riverdale"},
        {"slug": "upper-east-side/hampton-court", "bid": "6", "name": "Hampton Court", "addr": "333 East 102nd Street", "hood": "Upper East Side"},
        {"slug": "upper-east-side/the-andover", "bid": "1", "name": "The Andover", "addr": "1675 York Avenue", "hood": "Upper East Side"},
        {"slug": "upper-east-side/the-barclay", "bid": "4", "name": "The Barclay", "addr": "1755 York Avenue", "hood": "Upper East Side"},
        {"slug": "upper-east-side/the-brittany", "bid": "3", "name": "The Brittany", "addr": "1775 York Avenue", "hood": "Upper East Side"},
        {"slug": "upper-east-side/the-cambridge", "bid": "2", "name": "The Cambridge", "addr": "500 East 85th Street", "hood": "Upper East Side"},
        {"slug": "upper-east-side/the-fairmont", "bid": "8", "name": "The Fairmont", "addr": "300 East 75th Street", "hood": "Upper East Side"},
        {"slug": "upper-east-side/the-lucerne", "bid": "9", "name": "The Lucerne", "addr": "350 East 79th Street", "hood": "Upper East Side"},
        {"slug": "upper-east-side/the-marlowe", "bid": "10", "name": "The Marlowe", "addr": "145 East 81st Street", "hood": "Upper East Side"},
        {"slug": "upper-east-side/the-pavilion", "bid": "11", "name": "The Pavilion", "addr": "500 East 77th Street", "hood": "Upper East Side"},
        {"slug": "upper-east-side/the-somerset", "bid": "12", "name": "The Somerset", "addr": "1365 York Avenue", "hood": "Upper East Side"},
        {"slug": "upper-east-side/the-stratford", "bid": "13", "name": "The Stratford", "addr": "1385 York Avenue", "hood": "Upper East Side"},
        {"slug": "upper-west-side/grand-tier", "bid": "21", "name": "Grand Tier", "addr": "1930 Broadway", "hood": "Upper West Side"},
        {"slug": "upper-west-side/hawthorn-park", "bid": "117", "name": "Hawthorn Park", "addr": "160 West 62nd Street", "hood": "Upper West Side"},
        {"slug": "upper-west-side/the-encore", "bid": "119", "name": "The Encore", "addr": "175 West 60th Street", "hood": "Upper West Side"},
        {"slug": "upper-west-side/the-regent", "bid": "22", "name": "The Regent", "addr": "45 West 60th Street", "hood": "Upper West Side"},
    ]

    def _scrape_building(self, bldg):
        """Scrape a building page for listing IDs, then fetch each listing detail."""
        url = f"{self.BASE}/properties/{bldg['slug']}/"
        try:
            html = fetch(url)
        except Exception:
            return []

        # Find listing IDs (lid values) in the building page
        lids = re.findall(r'[?&]lid=(\d+)', html)
        lids = list(set(lids))  # deduplicate

        if not lids:
            return []

        listings = []
        for lid in lids:
            try:
                listing = self._scrape_listing(lid, bldg)
                if listing:
                    listings.append(listing)
                time.sleep(0.3)
            except Exception as e:
                print(f"    lid={lid}: {e}")
        return listings

    def _scrape_listing(self, lid, bldg, lat=None, lng=None):
        """Fetch and parse a single listing detail page."""
        url = f"{self.BASE}/listing-detail/?lid={lid}"
        html = fetch(url)

        # Primary: extract price from schedule-appointment iframe URL
        price = None
        pm = re.search(
            rf'schedule-appointment-listing-today/\?lid={lid}[^"]*?price=(\d+)',
            html
        )
        if pm:
            price = int(pm.group(1))

        # Extract bed/bath from pprice elements (skip phone numbers like 212.535.0500)
        beds = None
        baths = None
        for m in re.finditer(r'class="pprice"[^>]*>([^<]+)', html):
            content = m.group(1).strip()
            if not content or re.match(r'^\d{3}\.\d{3}', content):
                continue
            # Check if it's a bed/bath string
            bed_m = re.search(r'(\d+)\s*BR', content, re.I)
            if bed_m:
                beds = int(bed_m.group(1))
            elif 'studio' in content.lower():
                beds = 0
            bath_m = re.search(r'(\d+(?:\.\d+)?)\s*Bath', content, re.I)
            if bath_m:
                baths = float(bath_m.group(1))
            # Check if it's a price string (digits with optional commas)
            if price is None and re.match(r'^[\d,]+$', content):
                price = int(content.replace(',', ''))

        # Check for convertible
        if beds is None:
            cm = re.search(r'CONV(\d)', html, re.I)
            if cm:
                beds = int(cm.group(1))

        # Extract floor plan
        fp = None
        fpm = re.search(r'glenwoodadmin\.com/webdav/images/floorplans/[^"\']+', html)
        if fpm:
            fp = "https://" + fpm.group(0)

        # Extract description
        desc = None
        dm = re.search(r'class="[^"]*listing-description[^"]*"[^>]*>(.*?)</div>', html, re.DOTALL | re.I)
        if dm:
            desc = re.sub(r'<[^>]+>', '', dm.group(1)).strip()

        if price is None and beds is None:
            return None  # Skip empty/broken listings

        return {
            "source_id": f"glenwood-{lid}",
            "building_name": bldg["name"],
            "address": bldg["addr"],
            "unit_number": None,
            "bedrooms": beds,
            "bathrooms": baths,
            "price": price,
            "sqft": None,
            "available_date": None,
            "lease_terms": None,
            "amenities": None,
            "description": desc,
            "floor_plan_url": fp,
            "image_urls": None,
            "latitude": None,
            "longitude": None,
            "neighborhood": bldg["hood"],
            "borough": "Manhattan" if bldg["hood"] != "Riverdale" else "Bronx",
            "zipcode": None,
            "is_flex": 0,
            "is_rent_stabilized": 0,
            "finish_level": None,
            "raw_json": None,
        }

    def pull(self):
        all_listings = []
        for bldg in self.BUILDINGS:
            listings = self._scrape_building(bldg)
            if listings:
                print(f"  {bldg['name']}: {len(listings)} listings")
            all_listings.extend(listings)
            time.sleep(0.5)
        return all_listings


# ─── SecureCafe/Yardi Adapter ────────────────────────────────────────────────

class SecureCafeAdapter(SourceAdapter):
    name = "securecafe"
    description = "SecureCafe/Yardi — multi-landlord leasing platform"

    # Each portal: (label, subdomain, property_slug, address, neighborhood, borough)
    # We can add more portals trivially — just need subdomain + slug
    PORTALS = [
        # Clipper Equity / Clipper Realty
        {"label": "50 Murray (Tribeca House)", "subdomain": "50murray", "slug": "50-murray",
         "address": "50 Murray Street", "hood": "Tribeca", "boro": "Manhattan"},
        {"label": "53 Park Place (Tribeca House)", "subdomain": "53parkplace", "slug": "53-park-place",
         "address": "53 Park Place", "hood": "Tribeca", "boro": "Manhattan"},
        {"label": "The Aspen", "subdomain": "theaspen", "slug": "the-aspen0",
         "address": "1955 First Avenue", "hood": "East Harlem", "boro": "Manhattan"},
        {"label": "Clover House", "subdomain": "cloverhousebk", "slug": "107-columbia-heights-brooklyn-ny-11201",
         "address": "107 Columbia Heights", "hood": "Brooklyn Heights", "boro": "Brooklyn"},
        {"label": "233 Schermerhorn", "subdomain": "233schermerhorn", "slug": "security-equity-llc",
         "address": "233 Schermerhorn Street", "hood": "Downtown Brooklyn", "boro": "Brooklyn"},
        {"label": "Tower 77", "subdomain": "tower77bk", "slug": "tower-77",
         "address": "77 Commercial Street", "hood": "Greenpoint", "boro": "Brooklyn"},
        {"label": "Prospect House", "subdomain": "prospecthousebk-rentcafewebsite", "slug": "dean-street0",
         "address": "953 Dean Street", "hood": "Crown Heights", "boro": "Brooklyn"},
        {"label": "Pacific House", "subdomain": "pacifichousebk-rentcafewebsite", "slug": "pacific-house",
         "address": "1010 Pacific Street", "hood": "Crown Heights", "boro": "Brooklyn"},
        {"label": "Flatbush Gardens", "subdomain": "flatbushgardens", "slug": "flatbush-gardens",
         "address": "1403 New York Avenue", "hood": "Flatbush", "boro": "Brooklyn"},
        {"label": "Riverwatch", "subdomain": "riverwatch", "slug": "riverwatch",
         "address": "70 Battery Place", "hood": "Battery Park City", "boro": "Manhattan"},
        {"label": "The Brewster", "subdomain": "thebrewster", "slug": "the-brewster",
         "address": "21 West 86th Street", "hood": "Upper West Side", "boro": "Manhattan"},
        {"label": "Casa Hope", "subdomain": "casahope-rentcafewebsite", "slug": "casa-hope0",
         "address": "130 Hope Street", "hood": "Williamsburg", "boro": "Brooklyn"},
        {"label": "Bedford Square", "subdomain": "bedford-square1-rentcafewebsite", "slug": "bedford-square1",
         "address": "2360 Bedford Avenue", "hood": "Flatbush", "boro": "Brooklyn"},
        {"label": "Parkside BK", "subdomain": "123parkside", "slug": "123-parkside-ave-brooklyn-ny-11226",
         "address": "125 Parkside Avenue", "hood": "Flatbush", "boro": "Brooklyn"},
        {"label": "1350 Fifteenth (NJ)", "subdomain": "1350nj", "slug": "1350-15-street",
         "address": "1350 15th Street", "hood": "Fort Lee", "boro": "NJ"},
        # Rudin Management
        {"label": "Rudin Portfolio", "subdomain": "rudin-reslisting", "slug": "rudin-management-co-inc",
         "address": "", "hood": "", "boro": "Manhattan"},
        # Westminster
        {"label": "Westminster Portfolio", "subdomain": "westminster", "slug": "westminster-management",
         "address": "", "hood": "", "boro": "Manhattan"},
        # Finkelstein-Timberger
        {"label": "FTRE Portfolio", "subdomain": "ftre-reslisting", "slug": "finkelstein-timberger-east-llc",
         "address": "", "hood": "", "boro": "Manhattan"},
        # Goldfarb
        {"label": "Goldfarb Portfolio", "subdomain": "goldfarbproperties", "slug": "goldfarb-properties",
         "address": "", "hood": "", "boro": "Manhattan"},
        # Bronstein
        {"label": "Bronstein Portfolio", "subdomain": "bronsteinproperties", "slug": "bronstein-properties-llc",
         "address": "", "hood": "", "boro": "Brooklyn"},
        # 9300 Realty
        {"label": "9300 Realty Portfolio", "subdomain": "centpropny", "slug": "century-property-management-ny",
         "address": "", "hood": "", "boro": "Manhattan"},
    ]

    def _fetch_portal(self, portal):
        """Fetch and parse one SecureCafe availability portal."""
        url = f"https://{portal['subdomain']}.securecafe.com/onlineleasing/{portal['slug']}/availableunits.aspx"
        try:
            html = fetch(url, timeout=30)
        except Exception as e:
            return [], str(e)

        if len(html) < 500 or "404" in html[:200]:
            return [], "404 or empty"

        units = []

        # Parse floor plan sections
        # Each section: "Floor Plan: {name} - {N} Bedroom(s), {M} Bathroom(s)"
        # followed by a table of units
        sections = re.split(r'<caption[^>]*>Apartment Details.*?Floor Plan:\s*', html, flags=re.I | re.DOTALL)

        for section in sections[1:]:
            # Extract bed/bath from section header
            beds = 0
            baths = 1
            header_m = re.match(r'([^<]+)', section)
            if header_m:
                header = header_m.group(1)
                bed_m = re.search(r'(\d+)\s*Bed', header, re.I)
                if bed_m:
                    beds = int(bed_m.group(1))
                elif 'studio' in header.lower():
                    beds = 0
                bath_m = re.search(r'(\d+(?:\.\d+)?)\s*Bath', header, re.I)
                if bath_m:
                    baths = float(bath_m.group(1))

            # Find unit rows: <th data-label='Apartment'>#UNIT</th>
            # <td data-label=Sq.Ft.>SQFT</td>
            # <td data-label='Rent'>$PRICE</td>
            for um in re.finditer(
                r"data-label='Apartment'[^>]*>#?(\w+)</th>"
                r".*?data-label=Sq\.Ft\.>(\d+)</td>"
                r".*?data-label='Rent'>\$([\d,]+)</td>",
                section, re.DOTALL
            ):
                unit_num = um.group(1)
                sqft = int(um.group(2))
                price = int(um.group(3).replace(",", ""))

                # Extract move-in date from ApplyNowClick
                avail = None
                apply_m = re.search(
                    rf"id='{re.escape(unit_num)}'.*?ApplyNowClick\([^,]+,[^,]+,[^,]+,\"([^\"]+)\"",
                    section, re.DOTALL
                )
                if apply_m:
                    avail = apply_m.group(1)

                units.append({
                    "source_id": f"sc-{portal['subdomain']}-{unit_num}",
                    "building_name": portal["label"],
                    "address": portal["address"],
                    "unit_number": unit_num,
                    "bedrooms": beds,
                    "bathrooms": baths,
                    "price": price,
                    "sqft": sqft,
                    "available_date": avail,
                    "lease_terms": None,
                    "amenities": None,
                    "description": None,
                    "floor_plan_url": None,
                    "image_urls": None,
                    "latitude": None,
                    "longitude": None,
                    "neighborhood": portal["hood"],
                    "borough": portal["boro"],
                    "zipcode": None,
                    "is_flex": 0,
                    "is_rent_stabilized": 0,
                    "finish_level": None,
                    "raw_json": None,
                })

        return units, None

    def pull(self):
        all_units = []
        for portal in self.PORTALS:
            units, err = self._fetch_portal(portal)
            if err:
                print(f"  {portal['label']}: ERROR — {err}")
            elif units:
                print(f"  {portal['label']}: {len(units)} units")
                all_units.extend(units)
            else:
                print(f"  {portal['label']}: 0 units")
            time.sleep(0.5)
        return all_units


# ─── Stonehenge Adapter ──────────────────────────────────────────────────────

class StonehengeAdapter(SourceAdapter):
    name = "stonehenge"
    description = "Stonehenge Management — Salesforce API (20 luxury NYC buildings)"

    API_URL = "https://stonehenge.my.site.com/services/apexrest/webflow/apply-now/"

    def pull(self):
        # Step 1: Get all buildings
        raw = fetch(self.API_URL, headers={"Accept": "application/json"})
        buildings = json.loads(raw)
        print(f"  {len(buildings)} buildings")

        all_units = []
        for bldg in buildings:
            code = bldg.get("value", "")
            label = bldg.get("label", "")
            try:
                units_raw = fetch(
                    f"{self.API_URL}?buildingCode={code}",
                    headers={"Accept": "application/json"}
                )
                units = json.loads(units_raw)
            except Exception:
                continue

            for u in units:
                # Parse: "1 Bedroom | Bath 1.0 | Apt 015M | $4945"
                info = u.get("label", "")
                beds = 0
                baths = 1
                unit_num = ""
                price = None

                bed_m = re.search(r'(\d+)\s*Bedroom', info, re.I)
                if bed_m:
                    beds = int(bed_m.group(1))
                elif 'Studio' in info:
                    beds = 0

                bath_m = re.search(r'Bath\s*([\d.]+)', info, re.I)
                if bath_m:
                    baths = float(bath_m.group(1))

                apt_m = re.search(r'Apt\s*(\S+)', info, re.I)
                if apt_m:
                    unit_num = apt_m.group(1)

                price_m = re.search(r'\$([\d,]+)', info)
                if price_m:
                    price = int(price_m.group(1).replace(",", ""))

                all_units.append({
                    "source_id": f"stonehenge-{u.get('value', '')}",
                    "building_name": label,
                    "address": "",
                    "unit_number": unit_num,
                    "bedrooms": beds,
                    "bathrooms": baths,
                    "price": price,
                    "sqft": None,
                    "available_date": None,
                    "lease_terms": None,
                    "amenities": None,
                    "description": None,
                    "floor_plan_url": None,
                    "image_urls": None,
                    "latitude": None,
                    "longitude": None,
                    "neighborhood": "",
                    "borough": "Manhattan",
                    "zipcode": None,
                    "is_flex": 0,
                    "is_rent_stabilized": 0,
                    "finish_level": None,
                    "raw_json": json.dumps(u),
                })

            if units:
                print(f"    {label}: {len(units)} units")
            time.sleep(0.3)

        return all_units


# ─── Bozzuto Adapter ─────────────────────────────────────────────────────────

class BozzutoAdapter(SourceAdapter):
    name = "bozzuto"
    description = "Bozzuto Management — Algolia search (NYC properties)"

    ALGOLIA_APP = "5868YKQCN6"
    ALGOLIA_KEY = "e0bf88810e07743ac0020cc216ed45c2"
    ALGOLIA_URL = f"https://{ALGOLIA_APP}-dsn.algolia.net/1/indexes/www.bozzuto.com_wp_floor_plans/query"

    # NYC metro area states
    NYC_STATES = ["NY", "NJ"]

    def pull(self):
        all_units = []
        page = 0

        while True:
            body = json.dumps({
                "query": "new york",
                "hitsPerPage": 100,
                "page": page,
                "attributesToHighlight": [],
            }).encode()

            raw = fetch(
                self.ALGOLIA_URL,
                headers={
                    "X-Algolia-Application-Id": self.ALGOLIA_APP,
                    "X-Algolia-API-Key": self.ALGOLIA_KEY,
                    "Content-Type": "application/json",
                },
                data=body,
                method="POST",
            )
            data = json.loads(raw)
            hits = data.get("hits", [])
            if not hits:
                break

            for fp in hits:
                prop_name = fp.get("property_name", "")
                fp_name = fp.get("name", "")
                fp_type = fp.get("type")
                beds = 0
                if isinstance(fp_type, dict):
                    type_name = fp_type.get("name", "").lower()
                    bed_m = re.search(r'(\d+)', type_name)
                    if bed_m:
                        beds = int(bed_m.group(1))
                elif isinstance(fp_type, str):
                    bed_m = re.search(r'(\d+)', fp_type)
                    if bed_m:
                        beds = int(bed_m.group(1))

                # Parse move-in date (can be dict with 'date' key)
                mid = fp.get("move_in_date")
                avail_date = None
                if isinstance(mid, dict):
                    avail_date = mid.get("date", "")[:10]
                elif isinstance(mid, str):
                    avail_date = mid[:10]

                # Price and size can be strings
                price = fp.get("price")
                if isinstance(price, str):
                    price = int(price) if price.isdigit() else None
                sqft = fp.get("size")
                if isinstance(sqft, str):
                    sqft = int(sqft) if sqft.isdigit() else None

                all_units.append({
                    "source_id": f"bozzuto-{fp.get('objectID', '')}",
                    "building_name": prop_name,
                    "address": "",
                    "unit_number": fp_name,
                    "bedrooms": beds,
                    "bathrooms": None,
                    "price": price,
                    "sqft": sqft,
                    "available_date": avail_date,
                    "lease_terms": None,
                    "amenities": None,
                    "description": None,
                    "floor_plan_url": fp.get("image") if isinstance(fp.get("image"), str) else None,
                    "image_urls": None,
                    "latitude": None,
                    "longitude": None,
                    "neighborhood": "",
                    "borough": "",
                    "zipcode": "",
                    "is_flex": 0,
                    "is_rent_stabilized": 0,
                    "finish_level": None,
                    "raw_json": json.dumps(fp, default=str),
                })

            print(f"  Page {page}: {len(hits)} floor plans")
            page += 1
            if page >= data.get("nbPages", 0):
                break

        return all_units


# ─── Registry ────────────────────────────────────────────────────────────────

ADAPTERS = {
    "stuytown": StuyTownAdapter,
    "durst": DurstAdapter,
    "glenwood": GlenwoodAdapter,
    "securecafe": SecureCafeAdapter,
    "stonehenge": StonehengeAdapter,
    "bozzuto": BozzutoAdapter,
}


def show_status(conn):
    print(f"\n{'='*60}")
    print(f"  Unified Puller Database: {DB_PATH}")
    print(f"{'='*60}")

    row = conn.execute("SELECT COUNT(*) FROM listings").fetchone()
    print(f"\n  Total listings: {row[0]:,}")

    print(f"\n  By source:")
    for row in conn.execute(
        "SELECT source, status, COUNT(*) FROM listings GROUP BY source, status ORDER BY source, status"
    ):
        print(f"    {row[0]:12s}  {row[1]:10s}  {row[2]:,}")

    print(f"\n  By borough:")
    for row in conn.execute(
        "SELECT borough, COUNT(*) FROM listings WHERE status='active' GROUP BY borough ORDER BY COUNT(*) DESC"
    ):
        boro = row[0] or "Unknown"
        print(f"    {boro:20s}  {row[1]:,}")

    print(f"\n  Price range (active):")
    row = conn.execute(
        "SELECT MIN(price), AVG(price), MAX(price) FROM listings WHERE status='active' AND price > 0"
    ).fetchone()
    if row and row[0]:
        print(f"    Min: ${row[0]:,.0f}  Avg: ${row[1]:,.0f}  Max: ${row[2]:,.0f}")

    print(f"\n  Pull history (last 10):")
    for row in conn.execute(
        "SELECT source, pulled_at, listings_count, new_count, updated_count "
        "FROM pull_log ORDER BY pulled_at DESC LIMIT 10"
    ):
        print(f"    {row[0]:12s}  {row[1]}  {row[2]:4d} total  {row[3]:3d} new  {row[4]:3d} updated")

    print(f"\n  Price history snapshots: {conn.execute('SELECT COUNT(*) FROM price_history').fetchone()[0]:,}")
    print()


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Unified NYC apartment listing puller")
    parser.add_argument("--source", choices=list(ADAPTERS.keys()), help="Pull only this source")
    parser.add_argument("--status", action="store_true", help="Show database status")
    parser.add_argument("--reset", metavar="SOURCE", help="Clear all data for a source and re-pull")
    args = parser.parse_args()

    conn = init_db()

    if args.status:
        show_status(conn)
        conn.close()
        return

    if args.reset:
        if args.reset in ADAPTERS:
            conn.execute("DELETE FROM listings WHERE source=?", (args.reset,))
            conn.execute("DELETE FROM price_history WHERE source=?", (args.reset,))
            conn.execute("DELETE FROM pull_log WHERE source=?", (args.reset,))
            conn.commit()
            print(f"  Reset {args.reset}")
        else:
            print(f"  Unknown source: {args.reset}")
            conn.close()
            return

    sources = [args.source] if args.source else list(ADAPTERS.keys())

    for name in sources:
        adapter = ADAPTERS[name]()
        try:
            adapter.run(conn)
        except Exception as e:
            print(f"  ERROR pulling {name}: {e}")
            import traceback
            traceback.print_exc()

    print()
    show_status(conn)
    conn.close()


if __name__ == "__main__":
    main()

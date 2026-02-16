#!/usr/bin/env python3
"""
StreetEasy High-Speed Scraper — TLS-Client Edition

Uses tls-client with Chrome cookies to fetch building pages at ~2 req/s
without a browser. Parses RSC (React Server Component) data from the HTML.

Setup:
  1. pip3 install tls-client
  2. Open Chrome, visit streeteasy.com (solve any captcha)
  3. python3 scripts/se_fast_scrape.py --extract-cookies
  4. python3 scripts/se_fast_scrape.py --from-sitemap --limit 1000

Cookie refresh:
  Cookies last ~1 hour. When you see 403s, re-run --extract-cookies.
"""

import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
import random
from pathlib import Path

try:
    import tls_client
except ImportError:
    print("pip3 install tls-client")
    sys.exit(1)

SE_DB_PATH = Path(__file__).parent.parent / "se_listings.db"
COOKIE_FILE = Path(__file__).parent.parent / "se_cache" / "chrome_cookies.json"
SITEMAP_DIR = Path(__file__).parent.parent / "se_sitemaps"


# ── Database ─────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(str(SE_DB_PATH))
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS buildings (
            slug TEXT PRIMARY KEY,
            url TEXT NOT NULL,
            address TEXT,
            neighborhood TEXT,
            title TEXT,
            building_type TEXT,
            total_units INTEGER,
            stories INTEGER,
            year_built INTEGER,
            pet_policy TEXT,
            amenities TEXT,
            description TEXT,
            scraped_at TEXT,
            scrape_duration_ms INTEGER,
            status TEXT,
            raw_info TEXT
        );

        CREATE TABLE IF NOT EXISTS unit_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            building_slug TEXT NOT NULL REFERENCES buildings(slug),
            unit TEXT NOT NULL,
            listing_type TEXT,
            availability TEXT,
            date TEXT,
            price TEXT,
            price_numeric REAL,
            status TEXT,
            beds TEXT,
            baths TEXT,
            sqft TEXT,
            sqft_numeric INTEGER,
            asking_price REAL,
            discount_pct REAL,
            scraped_at TEXT
        );

        CREATE TABLE IF NOT EXISTS unit_pages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            building_slug TEXT NOT NULL,
            unit TEXT NOT NULL,
            url TEXT NOT NULL,
            title TEXT,
            scraped_at TEXT,
            scrape_duration_ms INTEGER,
            status TEXT,
            record_count INTEGER
        );

        CREATE TABLE IF NOT EXISTS price_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            building_slug TEXT NOT NULL,
            unit TEXT NOT NULL,
            date TEXT,
            price TEXT,
            price_numeric REAL,
            event TEXT,
            broker TEXT,
            event_type TEXT,
            scraped_at TEXT
        );

        CREATE TABLE IF NOT EXISTS scrape_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT DEFAULT (datetime('now')),
            action TEXT,
            url TEXT,
            slug TEXT,
            unit TEXT,
            duration_ms INTEGER,
            status TEXT,
            detail TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_us_slug ON unit_summary(building_slug);
        CREATE INDEX IF NOT EXISTS idx_up_slug ON unit_pages(building_slug);
        CREATE INDEX IF NOT EXISTS idx_ph_slug ON price_history(building_slug);
        CREATE INDEX IF NOT EXISTS idx_ph_unit ON price_history(building_slug, unit);
    """)
    conn.commit()

    # Migrations
    cols = [r[1] for r in conn.execute("PRAGMA table_info(unit_summary)").fetchall()]
    if "availability" not in cols:
        conn.execute("ALTER TABLE unit_summary ADD COLUMN availability TEXT DEFAULT 'unavailable'")
    bcols = [r[1] for r in conn.execute("PRAGMA table_info(buildings)").fetchall()]
    for col, typ in [("neighborhood", "TEXT"), ("building_type", "TEXT"),
                     ("pet_policy", "TEXT"), ("amenities", "TEXT"), ("description", "TEXT")]:
        if col not in bcols:
            conn.execute(f"ALTER TABLE buildings ADD COLUMN {col} {typ}")
    conn.commit()
    return conn


# ── Cookie management ────────────────────────────────────────

def extract_cookies_from_chrome():
    """Extract cookies from Chrome via AppleScript."""
    nav = 'tell application "Google Chrome" to set URL of active tab of first window to "https://streeteasy.com/"'
    subprocess.run(["osascript", "-e", nav], capture_output=True, text=True, timeout=10)
    time.sleep(4)

    js = "document.cookie"
    escaped = js.replace("\\", "\\\\").replace('"', '\\"')
    script = f'tell application "Google Chrome" to execute active tab of first window javascript "{escaped}"'
    r = subprocess.run(["osascript", "-e", script], capture_output=True, text=True, timeout=10)
    if r.returncode != 0:
        print(f"Error: {r.stderr}")
        return None

    raw = r.stdout.strip()
    cookies = {}
    for pair in raw.split("; "):
        if "=" in pair:
            k, v = pair.split("=", 1)
            cookies[k.strip()] = v.strip()

    js_ua = "navigator.userAgent"
    escaped_ua = js_ua.replace("\\", "\\\\").replace('"', '\\"')
    script_ua = f'tell application "Google Chrome" to execute active tab of first window javascript "{escaped_ua}"'
    r_ua = subprocess.run(["osascript", "-e", script_ua], capture_output=True, text=True, timeout=10)
    user_agent = r_ua.stdout.strip() if r_ua.returncode == 0 else None

    result = {"cookies": cookies, "user_agent": user_agent, "extracted_at": time.strftime("%Y-%m-%d %H:%M:%S")}
    COOKIE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(COOKIE_FILE, "w") as f:
        json.dump(result, f, indent=2)

    px_keys = [k for k in cookies if k.startswith("_px")]
    print(f"Extracted {len(cookies)} cookies ({len(px_keys)} PX)")
    print(f"Saved to {COOKIE_FILE}")
    return result


def load_cookies():
    if not COOKIE_FILE.exists():
        print(f"No cookies. Run: python3 {sys.argv[0]} --extract-cookies")
        return None
    with open(COOKIE_FILE) as f:
        data = json.load(f)
    age = time.time() - time.mktime(time.strptime(data["extracted_at"], "%Y-%m-%d %H:%M:%S"))
    age_min = int(age / 60)
    if age_min > 50:
        print(f"WARNING: Cookies are {age_min}min old — may be expired. Re-run --extract-cookies")
    return data


def create_session(cookie_data):
    session = tls_client.Session(client_identifier="chrome_120", random_tls_extension_order=True)
    for k, v in cookie_data["cookies"].items():
        session.cookies.set(k, v, domain=".streeteasy.com")
    ua = cookie_data.get("user_agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
    session.headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"macOS"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    }
    return session


# ── Parsing ──────────────────────────────────────────────────

def parse_price(price_str):
    if not price_str:
        return None
    cleaned = re.sub(r'[^\d.]', '', str(price_str).split('\n')[0])
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def parse_sqft(sqft_str):
    if not sqft_str or str(sqft_str).strip() in ('-', '- ft\u00b2', ''):
        return None
    cleaned = re.sub(r'[^\d]', '', str(sqft_str))
    try:
        return int(cleaned) if cleaned else None
    except ValueError:
        return None


def parse_status(status_str):
    asking = None
    discount = None
    m = re.search(r'asking:\s*\$([\d,]+)', status_str or '')
    if m:
        asking = float(m.group(1).replace(',', ''))
    m = re.search(r'([+-]?\d+\.?\d*)%', status_str or '')
    if m:
        discount = float(m.group(1))
    return asking, discount


def extract_rsc_data(html):
    """Extract all RSC chunks from HTML and combine."""
    chunks = re.findall(r'self\.__next_f\.push\(\[\d+,"(.*?)"\]\)', html, re.S)
    rsc = ""
    for content in chunks:
        try:
            rsc += content.replace("\\n", "\n").replace("\\t", "\t").replace('\\"', '"').replace("\\/", "/")
        except:
            pass
    return rsc


def parse_building_page(html, slug):
    """Parse a building page HTML into structured data."""
    info = {"slug": slug}

    # Title
    m = re.search(r'<title>([^<]+)</title>', html)
    if m:
        info["title"] = m.group(1)
        # Parse neighborhood from title: "740 Park Avenue in Lenox Hill : Sales..."
        tm = re.match(r'(.+?)\s+in\s+(.+?)\s*:', info["title"])
        if tm:
            info["address"] = tm.group(1).strip()
            info["neighborhood"] = tm.group(2).strip()
        else:
            info["address"] = info["title"].split("|")[0].strip()

    # Meta description has building type, year built, stories, units
    m = re.search(r'<meta\s+name="description"\s+content="([^"]*)"', html)
    if m:
        desc = m.group(1)
        info["meta_description"] = desc

        bt = re.search(r'is a (condo|co-op|condop|rental|townhouse|multi-family)\s*(?:building)?', desc, re.I)
        if bt:
            info["building_type"] = bt.group(1).capitalize()

        yr = re.search(r'built in (\d{4})', desc)
        if yr:
            info["year_built"] = int(yr.group(1))

        st = re.search(r'has (\d+) stor', desc)
        if st:
            info["stories"] = int(st.group(1))

        un = re.search(r'(\d+) unit', desc)
        if un:
            info["total_units"] = int(un.group(1))

    # Lat/lng
    m = re.search(r'<meta\s+name="ICBM"\s+content="([^"]*)"', html)
    if m:
        parts = m.group(1).split(";")
        if len(parts) == 2:
            info["lat"] = float(parts[0].strip())
            info["lng"] = float(parts[1].strip())

    # RSC data
    rsc = extract_rsc_data(html)
    if rsc:
        # Amenities
        amen_match = re.search(r'"amenities":\{"list":\[([^\]]*)\]', rsc)
        if amen_match:
            try:
                items = json.loads("[" + amen_match.group(1) + "]")
                info["amenities_raw"] = items
            except:
                pass

        # Amenities with names (the display version)
        amen_display = re.search(r'"amenities":\{("Services[^}]+\}[^}]+\}[^}]+\})', rsc)
        if amen_display:
            try:
                obj = json.loads("{" + amen_display.group(1) + "}")
                all_amenities = []
                for category, items in obj.items():
                    if isinstance(items, list):
                        for item in items:
                            if isinstance(item, dict) and "name" in item:
                                all_amenities.append(item["name"])
                            elif isinstance(item, str):
                                all_amenities.append(item)
                if all_amenities:
                    info["amenities_display"] = all_amenities
            except:
                pass

        # Unit features
        feat_match = re.search(r'"unitFeatures":\[([^\]]*)\]', rsc)
        if feat_match:
            try:
                features = json.loads("[" + feat_match.group(1) + "]")
                info["unit_features"] = [f.get("name", f.get("description", "")) for f in features if isinstance(f, dict)]
            except:
                pass

        # Owner/developer details
        owner_match = re.search(r'\{"owner":"([^"]*)"[^}]*"developer":"([^"]*)"[^}]*\}', rsc)
        if owner_match:
            info["owner"] = owner_match.group(1) if owner_match.group(1) != "UNAVAILABLE OWNER" else None
            info["developer"] = owner_match.group(2) or None

        # Address details
        addr_match = re.search(r'"address":\{"street":"([^"]*)","city":"([^"]*)","state":"([^"]*)","zipCode":"([^"]*)"', rsc)
        if addr_match:
            info["street"] = addr_match.group(1)
            info["city"] = addr_match.group(2)
            info["state"] = addr_match.group(3)
            info["zipcode"] = addr_match.group(4)

        # Neighborhood from RSC (more reliable)
        n_match = re.search(r'"neighborhood":"([^"]+)"', rsc)
        if n_match:
            info["neighborhood"] = n_match.group(1)

        # Building ID
        id_match = re.search(r'"buildingId":"(\d+)"', rsc)
        if id_match:
            info["building_id"] = id_match.group(1)

        # Pet policy from amenities context
        pet_match = re.search(r'"policies":\[([^\]]*)\]', rsc)
        if pet_match and pet_match.group(1):
            try:
                policies = json.loads("[" + pet_match.group(1) + "]")
                pet_items = [p for p in policies if isinstance(p, dict) and "pet" in str(p).lower()]
                if pet_items:
                    info["pet_policy"] = str(pet_items[0])
            except:
                pass

        # Sale/rental inventory summary
        for inv_type in ["saleInventorySummary", "rentalInventorySummary"]:
            # These contain bedroom breakdowns with available/unavailable counts
            pattern = f'"{inv_type}":'
            idx = rsc.find(pattern)
            if idx >= 0:
                # Look for the aggregate objects nearby
                region = rsc[idx:idx+2000]
                agg_objects = re.findall(r'\{[^{}]*"bedroomTitle":"[^"]*"[^{}]*\}', region)
                summaries = []
                for ao in agg_objects:
                    try:
                        summaries.append(json.loads(ao))
                    except:
                        pass
                if summaries:
                    info[inv_type] = summaries

        # Description from RSC
        desc_match = re.search(r'"description":"((?:[^"\\]|\\.)*)","', rsc)
        if desc_match:
            desc_text = desc_match.group(1)
            # Unescape HTML entities
            desc_text = desc_text.replace("\\u003cp\\u003e", "").replace("\\u003c/p\\u003e", " ")
            desc_text = desc_text.replace("\\u003cp>", "").replace("</p>", " ")
            desc_text = re.sub(r'<[^>]+>', '', desc_text).strip()
            if desc_text and len(desc_text) > 20:
                info["description"] = desc_text

    return info


# ── Building scraper ─────────────────────────────────────────

def scrape_building(session, conn, slug):
    """Fetch and parse a single building page."""
    url = f"https://streeteasy.com/building/{slug}"
    t0 = time.time()

    try:
        resp = session.get(url)
    except Exception as e:
        return "error", str(e)

    elapsed_ms = int((time.time() - t0) * 1000)

    if resp.status_code == 403:
        # Store the block so we skip it next time
        conn.execute("""
            INSERT OR REPLACE INTO buildings (slug, url, scraped_at, scrape_duration_ms, status)
            VALUES (?, ?, datetime('now'), ?, 'blocked')
        """, (slug, url, elapsed_ms))
        conn.commit()
        return "blocked", None

    if resp.status_code in (404, 308):
        conn.execute("""
            INSERT OR REPLACE INTO buildings (slug, url, scraped_at, scrape_duration_ms, status)
            VALUES (?, ?, datetime('now'), ?, ?)
        """, (slug, url, elapsed_ms, "not_found" if resp.status_code == 404 else "redirect"))
        conn.commit()
        return "not_found", None

    if resp.status_code != 200:
        return f"http_{resp.status_code}", None

    html = resp.text

    # Check for PX challenge (short page with only captcha)
    if "px-captcha" in html and len(html) < 5000:
        return "px_challenge", None

    info = parse_building_page(html, slug)
    if not info.get("title") or "StreetEasy" not in info.get("title", ""):
        return "parse_error", None

    # Build amenities JSON
    amenities = info.get("amenities_display") or info.get("amenities_raw")
    amenities_json = json.dumps(amenities) if amenities else None

    # Build full raw_info
    raw_json = json.dumps(info, default=str)

    # Store building
    conn.execute("""
        INSERT OR REPLACE INTO buildings
        (slug, url, address, neighborhood, title, building_type, total_units, stories,
         year_built, pet_policy, amenities, description,
         scraped_at, scrape_duration_ms, status, raw_info)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?, 'ok', ?)
    """, (slug, url, info.get("address"), info.get("neighborhood"), info.get("title"),
          info.get("building_type"), info.get("total_units"), info.get("stories"),
          info.get("year_built"), info.get("pet_policy"), amenities_json,
          info.get("description"), elapsed_ms, raw_json))

    conn.commit()

    log(conn, "building_ok", url, slug, duration_ms=elapsed_ms,
        detail=f"type={info.get('building_type')} units={info.get('total_units')} yr={info.get('year_built')}")

    return "ok", info


def log(conn, action, url, slug, unit=None, duration_ms=None, status=None, detail=None):
    conn.execute("""
        INSERT INTO scrape_log (action, url, slug, unit, duration_ms, status, detail)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (action, url, slug, unit, duration_ms, status, detail))
    conn.commit()


# ── Building sources ─────────────────────────────────────────

def get_slugs_from_sitemap(limit=None):
    slugs_file = SITEMAP_DIR / "all_buildings.txt"
    if not slugs_file.exists():
        print(f"Sitemap not found: {slugs_file}")
        return []
    with open(slugs_file) as f:
        slugs = [line.strip() for line in f if line.strip()]
    return slugs[:limit] if limit else slugs


# ── Main ─────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="SE high-speed scraper")
    parser.add_argument("--extract-cookies", action="store_true")
    parser.add_argument("--building", "-b", help="Single building slug")
    parser.add_argument("--file", "-f", help="File with building slugs")
    parser.add_argument("--from-sitemap", action="store_true")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--delay", type=float, default=1.0, help="Min delay between requests (seconds)")
    parser.add_argument("--batch-size", type=int, default=100, help="Commit every N buildings")
    args = parser.parse_args()

    if args.extract_cookies:
        extract_cookies_from_chrome()
        return

    cookie_data = load_cookies()
    if not cookie_data:
        return

    session = create_session(cookie_data)
    conn = init_db()

    # Determine building list
    if args.building:
        slugs = [args.building]
    elif args.file:
        with open(args.file) as f:
            slugs = [l.strip() for l in f if l.strip() and not l.startswith("#")]
    elif args.from_sitemap:
        slugs = get_slugs_from_sitemap(args.limit)
    else:
        parser.print_help()
        return

    # Filter out already-scraped
    already = set(r[0] for r in conn.execute(
        "SELECT slug FROM buildings WHERE status IN ('ok', 'not_found', 'blocked', 'redirect')"
    ).fetchall())
    to_scrape = [s for s in slugs if s not in already]
    skipped = len(slugs) - len(to_scrape)

    print(f"{'='*60}")
    print(f"StreetEasy Fast Scraper (tls-client)")
    print(f"{'='*60}")
    print(f"Target:  {len(to_scrape)} buildings ({skipped} cached)")
    print(f"Delay:   {args.delay}s")
    print(f"DB:      {SE_DB_PATH}")
    print()

    stats = {"ok": 0, "not_found": 0, "blocked": 0, "error": 0, "other": 0}
    consecutive_blocks = 0
    t_start = time.time()

    for i, slug in enumerate(to_scrape):
        status, info = scrape_building(session, conn, slug)

        # Progress
        elapsed_total = time.time() - t_start
        rate = (i + 1) / elapsed_total if elapsed_total > 0 else 0

        if status == "ok":
            stats["ok"] += 1
            consecutive_blocks = 0
            btype = info.get("building_type", "?") if info else "?"
            units = info.get("total_units", "?") if info else "?"
            nbhd = info.get("neighborhood", "") if info else ""
            print(f"  [{i+1}/{len(to_scrape)}] {slug} — {btype}, {units} units, {nbhd} ({rate:.1f}/s)")
        elif status == "not_found":
            stats["not_found"] += 1
            consecutive_blocks = 0
            if (i + 1) % 50 == 0:  # Only print every 50th 404
                print(f"  [{i+1}/{len(to_scrape)}] ... {stats['not_found']} not found so far ({rate:.1f}/s)")
        elif status == "blocked" or status == "px_challenge":
            stats["blocked"] += 1
            consecutive_blocks += 1
            print(f"  [{i+1}/{len(to_scrape)}] {slug} — BLOCKED ({consecutive_blocks} in a row)")

            if consecutive_blocks >= 3:
                print(f"\n  3 consecutive blocks — refreshing cookies...")
                new_cookies = extract_cookies_from_chrome()
                if new_cookies:
                    session = create_session(new_cookies)
                    consecutive_blocks = 0
                    print(f"  Cookies refreshed. Resuming in 5s...")
                    time.sleep(5)
                else:
                    print(f"  Cookie refresh failed. Stopping.")
                    break
        else:
            stats["other"] += 1
            consecutive_blocks = 0
            print(f"  [{i+1}/{len(to_scrape)}] {slug} — {status}")

        time.sleep(args.delay + random.uniform(0, args.delay * 0.5))

    # Summary
    elapsed = time.time() - t_start
    total_done = sum(stats.values())
    print(f"\n{'='*60}")
    print(f"Done in {elapsed:.0f}s ({total_done/elapsed:.1f} buildings/s)")
    print(f"  OK:        {stats['ok']}")
    print(f"  Not found: {stats['not_found']}")
    print(f"  Blocked:   {stats['blocked']}")
    print(f"  Other:     {stats['other']}")

    b_count = conn.execute("SELECT COUNT(*) FROM buildings WHERE status='ok'").fetchone()[0]
    print(f"\nTotal buildings in DB: {b_count}")
    conn.close()


if __name__ == "__main__":
    main()

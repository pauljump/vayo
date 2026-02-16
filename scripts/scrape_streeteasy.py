#!/usr/bin/env python3
"""
StreetEasy Building Scraper — Two-Pass Edition

Pass 1: Building pages → modal → discover all units + summary
Pass 2: Unit pages → full price history timeline

Stores everything in se_listings.db with full traceability.

Setup:
  1. Open Chrome (normal, no special flags)
  2. In Chrome: View > Developer > Allow JavaScript from Apple Events
  3. Visit streeteasy.com once in Chrome

Usage:
  python3 scripts/scrape_streeteasy.py --from-db --limit 50
  python3 scripts/scrape_streeteasy.py --building 710-broadway-new_york
  python3 scripts/scrape_streeteasy.py --from-sitemap --limit 1000
  python3 scripts/scrape_streeteasy.py --pass2-only  # skip pass 1, do deep scrape on discovered units
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

DB_PATH = Path(__file__).parent.parent / "vayo_clean.db"
SE_DB_PATH = Path(__file__).parent.parent / "se_listings.db"
SITEMAP_DIR = Path(__file__).parent.parent / "se_sitemaps"


# ── Database setup ───────────────────────────────────────────

def init_db():
    """Create se_listings.db with full schema."""
    conn = sqlite3.connect(str(SE_DB_PATH))
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS buildings (
            slug TEXT PRIMARY KEY,
            url TEXT NOT NULL,
            address TEXT,
            neighborhood TEXT,
            title TEXT,
            building_type TEXT,      -- condo, co-op, condop, rental, townhouse, etc
            total_units INTEGER,
            stories INTEGER,
            year_built INTEGER,
            pet_policy TEXT,
            amenities TEXT,          -- JSON array of amenity strings
            description TEXT,        -- building overview/description text
            scraped_at TEXT,
            scrape_duration_ms INTEGER,
            status TEXT,             -- ok, blocked, not_found, no_modal, error
            raw_info TEXT            -- full JSON of everything extracted
        );

        CREATE TABLE IF NOT EXISTS unit_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            building_slug TEXT NOT NULL REFERENCES buildings(slug),
            unit TEXT NOT NULL,
            listing_type TEXT,       -- sale, rental
            availability TEXT,       -- available, unavailable
            date TEXT,
            price TEXT,
            price_numeric REAL,
            status TEXT,             -- "Sold (asking: $X; -Y%)", "Recorded closing", etc
            beds TEXT,
            baths TEXT,
            sqft TEXT,
            sqft_numeric INTEGER,
            asking_price REAL,       -- parsed from status if available
            discount_pct REAL,       -- parsed from status if available
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
            status TEXT,             -- ok, blocked, not_found, empty
            record_count INTEGER
        );

        CREATE TABLE IF NOT EXISTS price_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            building_slug TEXT NOT NULL,
            unit TEXT NOT NULL,
            date TEXT,
            price TEXT,
            price_numeric REAL,
            event TEXT,              -- "Listed by Compass", "Closing record", "Price decreased by 4%", etc
            broker TEXT,             -- parsed from event if available
            event_type TEXT,         -- listed, delisted, price_change, closing, rented, in_contract, etc
            scraped_at TEXT
        );

        CREATE TABLE IF NOT EXISTS scrape_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT DEFAULT (datetime('now')),
            action TEXT,             -- navigate, click_modal, extract_table, extract_history
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

    # Migrate: add availability column if missing
    cols = [r[1] for r in conn.execute("PRAGMA table_info(unit_summary)").fetchall()]
    if "availability" not in cols:
        conn.execute("ALTER TABLE unit_summary ADD COLUMN availability TEXT DEFAULT 'unavailable'")
        conn.commit()

    # Migrate: add new building columns if missing
    bcols = [r[1] for r in conn.execute("PRAGMA table_info(buildings)").fetchall()]
    for col, typ in [("neighborhood", "TEXT"), ("building_type", "TEXT"),
                     ("pet_policy", "TEXT"), ("amenities", "TEXT"), ("description", "TEXT")]:
        if col not in bcols:
            conn.execute(f"ALTER TABLE buildings ADD COLUMN {col} {typ}")
    conn.commit()

    return conn


# ── AppleScript Chrome control ───────────────────────────────

def chrome_js(code):
    escaped = code.replace("\\", "\\\\").replace('"', '\\"')
    script = f'tell application "Google Chrome" to execute active tab of first window javascript "{escaped}"'
    r = subprocess.run(["osascript", "-e", script], capture_output=True, text=True, timeout=30)
    if r.returncode != 0:
        raise Exception(r.stderr.strip()[:200])
    return r.stdout.strip()


def chrome_navigate(url):
    escaped = url.replace('"', '\\"')
    script = f'tell application "Google Chrome" to set URL of active tab of first window to "{escaped}"'
    subprocess.run(["osascript", "-e", script], capture_output=True, text=True, timeout=10)


def chrome_title():
    r = subprocess.run(
        ["osascript", "-e", 'tell application "Google Chrome" to get title of active tab of first window'],
        capture_output=True, text=True, timeout=10
    )
    return r.stdout.strip()


def chrome_url():
    r = subprocess.run(
        ["osascript", "-e", 'tell application "Google Chrome" to get URL of active tab of first window'],
        capture_output=True, text=True, timeout=10
    )
    return r.stdout.strip()


def chrome_ensure_window():
    subprocess.run(["osascript", "-e", '''
        tell application "Google Chrome"
            if (count of windows) = 0 then make new window
            activate
        end tell
    '''], capture_output=True, text=True, timeout=10)


def wait_for_load(timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if chrome_js("document.readyState") == "complete":
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def is_blocked():
    try:
        title = chrome_title()
        return "denied" in title.lower() or "captcha" in title.lower()
    except Exception:
        return False


# ── Parsing helpers ──────────────────────────────────────────

def parse_price(price_str):
    """'$3,450,000' → 3450000.0"""
    if not price_str:
        return None
    cleaned = re.sub(r'[^\d.]', '', price_str.split('\n')[0].split('↓')[0].split('↑')[0])
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def parse_sqft(sqft_str):
    """'3,650 ft²' → 3650"""
    if not sqft_str or sqft_str.strip() in ('-', '- ft²', ''):
        return None
    cleaned = re.sub(r'[^\d]', '', sqft_str)
    try:
        return int(cleaned) if cleaned else None
    except ValueError:
        return None


def parse_status(status_str):
    """Extract asking price and discount from status like 'Sold (asking: $3,750,000; -8.0%)'"""
    asking = None
    discount = None
    m = re.search(r'asking:\s*\$([\d,]+)', status_str or '')
    if m:
        asking = float(m.group(1).replace(',', ''))
    m = re.search(r'([+-]?\d+\.?\d*)%', status_str or '')
    if m:
        discount = float(m.group(1))
    return asking, discount


def parse_event(event_str):
    """Classify event and extract broker."""
    event = (event_str or '').strip()
    broker = None
    event_type = "other"

    if "Listed by" in event:
        event_type = "listed"
        broker = event.replace("Listed by", "").strip()
    elif "Delisted by" in event:
        event_type = "delisted"
        broker = event.replace("Delisted by", "").strip()
    elif "Rented by" in event:
        event_type = "rented"
        broker = event.replace("Rented by", "").strip()
    elif "Sold by" in event:
        event_type = "sold"
        broker = event.split("Sold by")[-1].split("Closing")[0].strip()
    elif "Closing record" in event:
        event_type = "closing"
        if "Sold by" in event:
            broker = event.split("Sold by")[-1].split("Closing")[0].strip()
    elif "Price decreased" in event:
        event_type = "price_decrease"
    elif "Price increased" in event:
        event_type = "price_increase"
    elif "No longer available" in event:
        event_type = "no_longer_available"
    elif "Temporarily off market" in event:
        event_type = "off_market"
    elif "In contract" in event:
        event_type = "in_contract"
    elif "Recorded closing" in event:
        event_type = "closing"

    return event_type, broker


# ── Pass 1: Building modal scraper ───────────────────────────

def scrape_building_modal(conn, building_slug):
    """Pass 1: Scrape building page modal for unit summary."""
    url = f"https://streeteasy.com/building/{building_slug}"
    t0 = time.time()

    chrome_navigate(url)
    time.sleep(random.uniform(3, 5))
    wait_for_load()

    actual_url = chrome_url()
    title = chrome_title()

    if is_blocked():
        log(conn, "navigate", url, building_slug, status="blocked")
        return "blocked"

    if "StreetEasy" not in title:
        log(conn, "navigate", url, building_slug, status="not_found")
        return "not_found"

    # Extract comprehensive building info
    info_raw = chrome_js(
        "JSON.stringify((function() {"
        "  var r = {title: document.title, url: location.href};"
        ""
        "  // Neighborhood from breadcrumb or title"
        "  var bc = document.querySelector('[class*=breadcrumb], [class*=Breadcrumb], nav[aria-label]');"
        "  if (bc) r.breadcrumb = bc.innerText.trim().substring(0, 300);"
        "  var tm = document.title.match(/in\\s+([^:]+)\\s*:/);"
        "  if (tm) r.neighborhood = tm[1].trim();"
        ""
        "  // Building type (Condo, Co-op, Rental, Condop, Townhouse)"
        "  var bodyText = document.body.innerText.substring(0, 5000);"
        "  var btm = bodyText.match(/(Condo|Co-op|Condop|Rental|Townhouse|Multi-Family)\\s*(Building|building|\\b)/i);"
        "  if (btm) r.building_type = btm[1];"
        ""
        "  // Header/info section — units, stories, year built"
        "  var hdr = document.querySelector('[class*=BuildingInfo], [class*=buildingInfo], [class*=building-info], header, [class*=Header]');"
        "  r.header_text = hdr ? hdr.innerText.substring(0, 500) : bodyText.substring(0, 1000);"
        ""
        "  // Overview/description"
        "  var desc = document.querySelector('[class*=description], [class*=Description], [class*=overview], [class*=Overview]');"
        "  if (desc) r.description = desc.innerText.trim().substring(0, 2000);"
        ""
        "  // Amenities — look for amenity list, chips, or tags"
        "  var amenities = [];"
        "  var amenEls = document.querySelectorAll('[class*=amenity] li, [class*=Amenity] li, [class*=amenities] li, [class*=Amenities] li, [class*=amenity] span, [class*=Amenity] span');"
        "  for (var i = 0; i < amenEls.length; i++) {"
        "    var t = amenEls[i].textContent.trim();"
        "    if (t && t.length < 80) amenities.push(t);"
        "  }"
        "  if (amenities.length === 0) {"
        "    var sections = document.querySelectorAll('h2, h3');"
        "    for (var i = 0; i < sections.length; i++) {"
        "      if (/amenit/i.test(sections[i].textContent)) {"
        "        var sib = sections[i].nextElementSibling;"
        "        if (sib) {"
        "          var items = sib.querySelectorAll('li, span, div');"
        "          for (var j = 0; j < items.length && j < 50; j++) {"
        "            var t = items[j].textContent.trim();"
        "            if (t && t.length < 80 && t.length > 1) amenities.push(t);"
        "          }"
        "        }"
        "        break;"
        "      }"
        "    }"
        "  }"
        "  r.amenities = amenities;"
        ""
        "  // Pet policy"
        "  var petm = bodyText.match(/(Pets[^.]*(?:allowed|not allowed|case by case|no pets|dogs? allowed|cats? allowed)[^.]*\\.?)/i);"
        "  if (petm) r.pet_policy = petm[1].trim();"
        "  if (!r.pet_policy) {"
        "    for (var i = 0; i < amenities.length; i++) {"
        "      if (/pet|dog|cat/i.test(amenities[i])) { r.pet_policy = amenities[i]; break; }"
        "    }"
        "  }"
        ""
        "  // Key facts section"
        "  var facts = document.querySelectorAll('[class*=detail] dt, [class*=detail] dd, [class*=fact] dt, [class*=fact] dd, [class*=Detail] dt, [class*=Detail] dd');"
        "  if (facts.length > 0) {"
        "    r.facts = {};"
        "    for (var i = 0; i < facts.length - 1; i += 2) {"
        "      r.facts[facts[i].textContent.trim()] = facts[i+1].textContent.trim();"
        "    }"
        "  }"
        ""
        "  return r;"
        "})())"
    )
    info_parsed = json.loads(info_raw) if info_raw else {}
    info_text = info_parsed.get("header_text", "")

    # Parse building details from header text
    units_match = re.search(r'(\d+)\s*[Uu]nit', info_text)
    stories_match = re.search(r'(\d+)\s*[Ss]tor', info_text)
    built_match = re.search(r'(\d{4})\s*[Bb]uilt', info_text)

    total_units = int(units_match.group(1)) if units_match else None
    stories = int(stories_match.group(1)) if stories_match else None
    year_built = int(built_match.group(1)) if built_match else None

    # Extract parsed fields
    neighborhood = info_parsed.get("neighborhood")
    building_type = info_parsed.get("building_type")
    pet_policy = info_parsed.get("pet_policy")
    amenities_list = info_parsed.get("amenities", [])
    amenities_json = json.dumps(amenities_list) if amenities_list else None
    description = info_parsed.get("description")

    # Extract address from title: "710 Broadway in Noho : Sales..." → "710 Broadway"
    address = title.split(" in ")[0].strip() if " in " in title else title.split("|")[0].strip()

    # Scroll to units section
    chrome_js(
        "var els = document.querySelectorAll('h2');"
        "for (var i = 0; i < els.length; i++) {"
        "  if (els[i].textContent.includes('Available')) {"
        "    els[i].scrollIntoView(); break;"
        "  }"
        "}"
    )
    time.sleep(0.5)

    # Extract available units from the page (before opening unavailable modal)
    available_data = chrome_js(
        "var results = [];"
        "var cards = document.querySelectorAll('[class*=listing], [class*=Listing], [data-testid*=listing]');"
        "for (var i = 0; i < cards.length; i++) {"
        "  var c = cards[i];"
        "  var link = c.querySelector('a[href*=\"/building/\"]');"
        "  var unitEl = c.querySelector('[class*=unit], [class*=Unit], a');"
        "  var priceEl = c.querySelector('[class*=price], [class*=Price]');"
        "  var unit = '';"
        "  if (link) {"
        "    var m = link.href.match(/\\/building\\/[^/]+\\/([^?]+)/);"
        "    if (m) unit = m[1];"
        "  }"
        "  if (!unit && unitEl) unit = unitEl.textContent.trim().replace('#','');"
        "  var price = priceEl ? priceEl.textContent.trim() : '';"
        "  var text = c.innerText;"
        "  var beds = '';"
        "  var baths = '';"
        "  var sqft = '';"
        "  var bm = text.match(/([0-9]+)\\s*(?:bed|bd)/i);"
        "  if (bm) beds = bm[1] + ' beds';"
        "  var btm = text.match(/([0-9.]+)\\s*(?:bath|ba)/i);"
        "  if (btm) baths = btm[1] + ' baths';"
        "  var sm = text.match(/([0-9,]+)\\s*(?:ft|sq)/i);"
        "  if (sm) sqft = sm[1] + ' ft\\u00b2';"
        "  var type = 'sale';"
        "  if (/rent|no.fee|\\/(month|mo)/i.test(text)) type = 'rental';"
        "  if (unit) results.push({unit:unit,price:price,beds:beds,baths:baths,sqft:sqft,type:type});"
        "}"
        "JSON.stringify(results)"
    )
    available_rows = json.loads(available_data) if available_data else []

    # Click "View unavailable units"
    clicked = chrome_js(
        "var btns = document.querySelectorAll('button');"
        "var found = false;"
        "for (var i = 0; i < btns.length; i++) {"
        "  if (btns[i].textContent.trim().toLowerCase().includes('unavailable')) {"
        "    btns[i].click(); found = true; break;"
        "  }"
        "}"
        "found ? 'yes' : 'no'"
    )

    sales_rows = []
    rental_rows = []

    if clicked == "yes":
        time.sleep(3)
        has_modal = chrome_js(
            "document.querySelector('.ReactModal__Content--after-open') ? 'yes' : 'no'"
        )
        if has_modal != "yes":
            time.sleep(2)
            has_modal = chrome_js(
                "document.querySelector('.ReactModal__Content--after-open') ? 'yes' : 'no'"
            )

        if has_modal == "yes":
            log(conn, "click_modal", url, building_slug, status="opened")

            # Extract FOR SALE
            sales_rows = extract_modal_table()

            # Switch to FOR RENT
            chrome_js(
                "var m = document.querySelector('.ReactModal__Content--after-open');"
                "if (m) {"
                "  var tabs = m.querySelectorAll('button, [role=tab]');"
                "  for (var i = 0; i < tabs.length; i++) {"
                "    var t = tabs[i].textContent.trim();"
                "    if (t === 'For Rent' || t === 'FOR RENT') {"
                "      tabs[i].click(); break;"
                "    }"
                "  }"
                "}"
            )
            time.sleep(1.5)
            rental_rows = extract_modal_table()

            # Deduplicate
            if rental_rows == sales_rows:
                rental_rows = []

            # Close modal
            chrome_js(
                "var m = document.querySelector('.ReactModal__Content--after-open');"
                "if (m) { var c = m.querySelector('button'); if (c) c.click(); }"
            )
        else:
            log(conn, "click_modal", url, building_slug, status="modal_failed")

    duration_ms = int((time.time() - t0) * 1000)

    # Store building
    conn.execute("""
        INSERT OR REPLACE INTO buildings
        (slug, url, address, neighborhood, title, building_type, total_units, stories, year_built,
         pet_policy, amenities, description, scraped_at, scrape_duration_ms, status, raw_info)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?, 'ok', ?)
    """, (building_slug, actual_url, address, neighborhood, title, building_type,
          total_units, stories, year_built, pet_policy, amenities_json, description,
          duration_ms, info_raw))

    now = time.strftime("%Y-%m-%d %H:%M:%S")

    # Store available units
    for row in available_rows:
        asking, discount = parse_status(row.get("status", ""))
        conn.execute("""
            INSERT INTO unit_summary
            (building_slug, unit, listing_type, availability, date, price, price_numeric, status, beds, baths, sqft, sqft_numeric, asking_price, discount_pct, scraped_at)
            VALUES (?, ?, ?, 'available', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (building_slug, row["unit"], row.get("type", "sale"), row.get("date", ""),
              row.get("price", ""), parse_price(row.get("price", "")),
              row.get("status", ""), row.get("beds", ""), row.get("baths", ""),
              row.get("sqft", ""), parse_sqft(row.get("sqft", "")),
              asking, discount, now))

    # Store unavailable unit summaries
    for row in sales_rows:
        asking, discount = parse_status(row.get("status", ""))
        conn.execute("""
            INSERT INTO unit_summary
            (building_slug, unit, listing_type, availability, date, price, price_numeric, status, beds, baths, sqft, sqft_numeric, asking_price, discount_pct, scraped_at)
            VALUES (?, ?, 'sale', 'unavailable', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (building_slug, row["unit"], row["date"], row["price"], parse_price(row["price"]),
              row["status"], row["beds"], row["baths"], row["sqft"], parse_sqft(row["sqft"]),
              asking, discount, now))

    for row in rental_rows:
        asking, discount = parse_status(row.get("status", ""))
        conn.execute("""
            INSERT INTO unit_summary
            (building_slug, unit, listing_type, availability, date, price, price_numeric, status, beds, baths, sqft, sqft_numeric, asking_price, discount_pct, scraped_at)
            VALUES (?, ?, 'rental', 'unavailable', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (building_slug, row["unit"], row["date"], row["price"], parse_price(row["price"]),
              row["status"], row["beds"], row["baths"], row["sqft"], parse_sqft(row["sqft"]),
              asking, discount, now))

    conn.commit()
    log(conn, "building_done", url, building_slug, duration_ms=duration_ms,
        detail=f"available={len(available_rows)} sales={len(sales_rows)} rentals={len(rental_rows)}")

    return "ok", len(available_rows), len(sales_rows), len(rental_rows), total_units


def extract_modal_table():
    data = chrome_js(
        "var m = document.querySelector('.ReactModal__Content--after-open');"
        "if (!m) { JSON.stringify([]); } else {"
        "  var trs = m.querySelectorAll('tr');"
        "  var out = [];"
        "  for (var i = 0; i < trs.length; i++) {"
        "    var tds = trs[i].querySelectorAll('td');"
        "    if (tds.length >= 4) {"
        "      out.push({"
        "        date: tds[0].innerText.trim(),"
        "        unit: tds[1].innerText.trim().replace('#',''),"
        "        price: tds[2].innerText.trim(),"
        "        status: tds[3].innerText.trim(),"
        "        beds: tds.length > 4 ? tds[4].innerText.trim() : '',"
        "        baths: tds.length > 5 ? tds[5].innerText.trim() : '',"
        "        sqft: tds.length > 6 ? tds[6].innerText.trim() : ''"
        "      });"
        "    }"
        "  }"
        "  JSON.stringify(out);"
        "}"
    )
    return json.loads(data) if data else []


# ── Pass 2: Unit page deep scraper ───────────────────────────

def scrape_unit_deep(conn, building_slug, unit_id):
    """Pass 2: Scrape individual unit page for full price history."""
    url = f"https://streeteasy.com/building/{building_slug}/{unit_id}"
    t0 = time.time()

    chrome_navigate(url)
    time.sleep(random.uniform(0.6, 1.0))
    wait_for_load()

    actual_url = chrome_url()
    title = chrome_title()

    if is_blocked():
        log(conn, "unit_navigate", url, building_slug, unit=unit_id, status="blocked")
        return "blocked", 0

    if "#" not in title or "StreetEasy" not in title:
        duration_ms = int((time.time() - t0) * 1000)
        conn.execute("""
            INSERT INTO unit_pages (building_slug, unit, url, title, scraped_at, scrape_duration_ms, status, record_count)
            VALUES (?, ?, ?, ?, datetime('now'), ?, 'not_found', 0)
        """, (building_slug, unit_id, actual_url, title, duration_ms))
        conn.commit()
        return "not_found", 0

    # Click "Price history" tab
    chrome_js(
        "var els = document.querySelectorAll('button, a, span, [role=tab]');"
        "for (var i = 0; i < els.length; i++) {"
        "  var t = els[i].textContent.trim().toLowerCase();"
        "  if (t === 'price history' || t === 'listing history') {"
        "    els[i].click(); break;"
        "  }"
        "}"
    )
    time.sleep(0.2)

    # Click "Show more"
    chrome_js(
        "var btns = document.querySelectorAll('button, a, span');"
        "for (var i = 0; i < btns.length; i++) {"
        "  var t = btns[i].textContent.trim().toLowerCase();"
        "  if (t.includes('show more')) { btns[i].click(); break; }"
        "}"
    )
    time.sleep(0.4)

    # Extract price history
    data = chrome_js(
        "var results = [];"
        "var rows = document.querySelectorAll('table tr');"
        "for (var r = 0; r < rows.length; r++) {"
        "  var cells = rows[r].querySelectorAll('td');"
        "  if (cells.length >= 3) {"
        "    var d = cells[0].textContent.trim();"
        "    var p = cells[1].textContent.trim();"
        "    var e = cells[2].textContent.trim();"
        "    if (/[0-9]+\\/[0-9]+\\/[0-9]{4}/.test(d)) {"
        "      results.push({date: d, price: p, event: e});"
        "    }"
        "  }"
        "}"
        "JSON.stringify(results)"
    )

    records = json.loads(data) if data else []
    duration_ms = int((time.time() - t0) * 1000)
    now = time.strftime("%Y-%m-%d %H:%M:%S")

    # Store unit page record
    conn.execute("""
        INSERT INTO unit_pages (building_slug, unit, url, title, scraped_at, scrape_duration_ms, status, record_count)
        VALUES (?, ?, ?, ?, ?, ?, 'ok', ?)
    """, (building_slug, unit_id, actual_url, title, now, duration_ms, len(records)))

    # Store each price history record
    for rec in records:
        event_type, broker = parse_event(rec["event"])
        conn.execute("""
            INSERT INTO price_history
            (building_slug, unit, date, price, price_numeric, event, broker, event_type, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (building_slug, unit_id, rec["date"], rec["price"], parse_price(rec["price"]),
              rec["event"], broker, event_type, now))

    conn.commit()
    log(conn, "unit_done", url, building_slug, unit=unit_id, duration_ms=duration_ms,
        detail=f"records={len(records)}")

    return "ok", len(records)


# ── Logging ──────────────────────────────────────────────────

def log(conn, action, url, slug, unit=None, duration_ms=None, status=None, detail=None):
    conn.execute("""
        INSERT INTO scrape_log (action, url, slug, unit, duration_ms, status, detail)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (action, url, slug, unit, duration_ms, status, detail))
    conn.commit()


# ── Building sources ─────────────────────────────────────────

def get_buildings_from_db(limit=50):
    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}")
        return []
    conn = sqlite3.connect(str(DB_PATH))
    rows = conn.execute("""
        SELECT b.address
        FROM buildings b
        WHERE b.units_residential BETWEEN 4 AND 100
          AND b.avg_unit_sqft > 600
          AND b.borough IN ('MANHATTAN', 'MN', '1')
        ORDER BY b.avg_unit_sqft DESC
        LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [address_to_slug(addr) for addr, in rows if addr]


def get_buildings_from_sitemap(limit=1000):
    slugs_file = SITEMAP_DIR / "all_buildings.txt"
    if not slugs_file.exists():
        print(f"Sitemap not found: {slugs_file}")
        return []
    with open(slugs_file) as f:
        slugs = [line.strip() for line in f if line.strip()]
    return slugs[:limit] if limit else slugs


def address_to_slug(address):
    slug = address.lower().strip()
    slug = re.sub(r'[^a-z0-9]+', '-', slug).strip('-')
    return f"{slug}-new_york"


# ── Main ─────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="StreetEasy two-pass scraper")
    parser.add_argument("--building", "-b", help="Single building slug")
    parser.add_argument("--file", "-f", help="File with building slugs")
    parser.add_argument("--from-db", action="store_true", help="Top gems from database")
    parser.add_argument("--from-sitemap", action="store_true", help="Buildings from sitemap")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--pass1-only", action="store_true", help="Only do pass 1 (building modals)")
    parser.add_argument("--pass2-only", action="store_true", help="Only do pass 2 (unit deep scrape)")
    parser.add_argument("--delay-min", type=float, default=4)
    parser.add_argument("--delay-max", type=float, default=8)
    args = parser.parse_args()

    conn = init_db()

    # Determine building list
    buildings = []
    if not args.pass2_only:
        if args.building:
            buildings = [args.building]
        elif args.file:
            with open(args.file) as f:
                buildings = [l.strip() for l in f if l.strip() and not l.startswith("#")]
        elif args.from_db:
            buildings = get_buildings_from_db(args.limit)
        elif args.from_sitemap:
            buildings = get_buildings_from_sitemap(args.limit)
        else:
            parser.print_help()
            return

    chrome_ensure_window()
    time.sleep(1)

    # ── PASS 1: Building modals ──────────────────────────────
    if not args.pass2_only:
        # Filter out already-scraped buildings
        already = set(r[0] for r in conn.execute("SELECT slug FROM buildings WHERE status='ok'").fetchall())
        to_scrape = [s for s in buildings if s not in already]
        skipped = len(buildings) - len(to_scrape)

        print(f"═══ PASS 1: Building Modals ═══")
        print(f"Target: {len(to_scrape)} buildings ({skipped} cached)")
        print(f"Database: {SE_DB_PATH}")
        print()

        stats1 = {"ok": 0, "blocked": 0, "not_found": 0, "error": 0}

        for i, slug in enumerate(to_scrape):
            print(f"  [{i+1}/{len(to_scrape)}] {slug}...", end=" ", flush=True)

            try:
                result = scrape_building_modal(conn, slug)
            except Exception as e:
                print(f"ERROR: {e}")
                stats1["error"] += 1
                time.sleep(5)
                continue

            if isinstance(result, str):
                # blocked or not_found
                print(result)
                stats1[result] += 1
                if result == "blocked":
                    print("    Solve captcha in Chrome, then press Enter...")
                    input()
                continue

            status, n_avail, n_sales, n_rentals, total_units = result
            print(f"{total_units or '?'} units — {n_avail} available, {n_sales} sales, {n_rentals} rentals")
            stats1["ok"] += 1

            if i < len(to_scrape) - 1:
                time.sleep(random.uniform(args.delay_min, args.delay_max))

        print(f"\nPass 1 done: {stats1}")

    # ── PASS 2: Unit deep scrape ─────────────────────────────
    if not args.pass1_only:
        # Get all units discovered in pass 1 that haven't been deep-scraped yet
        discovered = conn.execute("""
            SELECT DISTINCT us.building_slug, us.unit
            FROM unit_summary us
            WHERE NOT EXISTS (
                SELECT 1 FROM unit_pages up
                WHERE up.building_slug = us.building_slug AND up.unit = us.unit
            )
            ORDER BY us.building_slug, us.unit
        """).fetchall()

        print(f"\n═══ PASS 2: Unit Deep Scrape ═══")
        print(f"Target: {len(discovered)} units across {len(set(r[0] for r in discovered))} buildings")
        print()

        stats2 = {"ok": 0, "blocked": 0, "not_found": 0, "error": 0}
        current_building = None

        for i, (slug, unit) in enumerate(discovered):
            if slug != current_building:
                current_building = slug
                print(f"\n  Building: {slug}")

            print(f"    [{i+1}/{len(discovered)}] Unit {unit}...", end=" ", flush=True)

            try:
                status, n_records = scrape_unit_deep(conn, slug, unit)
            except Exception as e:
                print(f"ERROR: {e}")
                stats2["error"] += 1
                time.sleep(5)
                continue

            if status == "blocked":
                print("BLOCKED!")
                stats2["blocked"] += 1
                print("    Solve captcha in Chrome, then press Enter...")
                input()
                # Retry
                try:
                    status, n_records = scrape_unit_deep(conn, slug, unit)
                except Exception:
                    continue

            if status == "not_found":
                print("not found")
                stats2["not_found"] += 1
            elif status == "ok":
                print(f"{n_records} records")
                stats2["ok"] += 1
            else:
                print(status)

            if i < len(discovered) - 1:
                time.sleep(random.uniform(args.delay_min, args.delay_max))

        print(f"\nPass 2 done: {stats2}")

    # Summary
    b_count = conn.execute("SELECT COUNT(*) FROM buildings WHERE status='ok'").fetchone()[0]
    u_count = conn.execute("SELECT COUNT(*) FROM unit_summary").fetchone()[0]
    p_count = conn.execute("SELECT COUNT(*) FROM price_history").fetchone()[0]
    up_count = conn.execute("SELECT COUNT(*) FROM unit_pages WHERE status='ok'").fetchone()[0]
    print(f"\n═══ Database Summary ═══")
    print(f"  Buildings:      {b_count}")
    print(f"  Unit summaries: {u_count}")
    print(f"  Units scraped:  {up_count}")
    print(f"  Price records:  {p_count}")
    print(f"  Database: {SE_DB_PATH}")

    conn.close()


if __name__ == "__main__":
    main()

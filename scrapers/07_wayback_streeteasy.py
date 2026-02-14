#!/usr/bin/env python3
"""
Scrape historical StreetEasy listings from Wayback Machine
Discover units that were listed in the past but not currently
"""

import requests
import sqlite3
import time
import re
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Dict, Set

DB_PATH = "/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"

# Wayback Machine CDX API
CDX_API = "http://web.archive.org/cdx/search/cdx"
WAYBACK_URL = "http://web.archive.org/web"

def get_streeteasy_snapshots(url_pattern: str, limit: int = 1000) -> List[Dict]:
    """Get Wayback Machine snapshots for a URL pattern"""
    print(f"Fetching Wayback snapshots for: {url_pattern}")

    params = {
        'url': url_pattern,
        'output': 'json',
        'limit': limit,
        'filter': 'statuscode:200',  # Only successful snapshots
        'collapse': 'timestamp:8'  # One per day
    }

    try:
        response = requests.get(CDX_API, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        if not data:
            return []

        # First row is headers
        headers = data[0]
        snapshots = []

        for row in data[1:]:
            snapshot = dict(zip(headers, row))
            snapshots.append({
                'timestamp': snapshot['timestamp'],
                'original_url': snapshot['original'],
                'mimetype': snapshot['mimetype'],
                'statuscode': snapshot['statuscode']
            })

        print(f"  Found {len(snapshots)} snapshots")
        return snapshots

    except Exception as e:
        print(f"  Error fetching snapshots: {e}")
        return []

def scrape_wayback_listing(timestamp: str, url: str) -> Dict:
    """Scrape a single listing from Wayback Machine"""
    wayback_url = f"{WAYBACK_URL}/{timestamp}id_/{url}"

    try:
        response = requests.get(wayback_url, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract listing details
        # (These selectors may need adjustment based on StreetEasy's historical HTML)
        listing_data = {}

        # Try to find address
        address_elem = soup.find('h1', class_=lambda x: x and 'address' in x.lower())
        if address_elem:
            listing_data['address'] = address_elem.get_text(strip=True)

        # Try to find unit
        unit_elem = soup.find(string=re.compile(r'(?:Apt|Unit|#)\s*[A-Z0-9]+', re.I))
        if unit_elem:
            match = re.search(r'(?:Apt|Unit|#)\s*([A-Z0-9]+)', unit_elem, re.I)
            if match:
                listing_data['unit'] = match.group(1)

        # Try to find price
        price_elem = soup.find(class_=lambda x: x and 'price' in x.lower())
        if price_elem:
            price_text = price_elem.get_text()
            price_match = re.search(r'\$([0-9,]+)', price_text.replace(',', ''))
            if price_match:
                listing_data['price'] = int(price_match.group(1))

        # Try to find bedrooms
        beds_elem = soup.find(string=re.compile(r'(\d+)\s*bed', re.I))
        if beds_elem:
            beds_match = re.search(r'(\d+)\s*bed', beds_elem, re.I)
            if beds_match:
                listing_data['bedrooms'] = int(beds_match.group(1))

        # Store snapshot info
        listing_data['snapshot_timestamp'] = timestamp
        listing_data['snapshot_url'] = wayback_url
        listing_data['original_url'] = url

        return listing_data

    except Exception as e:
        return {}

def scrape_building_page(timestamp: str, url: str) -> List[Dict]:
    """Scrape a building page which may list multiple units"""
    wayback_url = f"{WAYBACK_URL}/{timestamp}id_/{url}"

    listings = []

    try:
        response = requests.get(wayback_url, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        # Find all unit listings on the building page
        # Common patterns in StreetEasy building pages
        unit_cards = soup.find_all(class_=lambda x: x and ('unit' in x.lower() or 'listing' in x.lower()))

        for card in unit_cards:
            listing = {}

            # Extract unit number
            unit_elem = card.find(string=re.compile(r'(?:Unit|Apt|#)\s*[A-Z0-9]+', re.I))
            if unit_elem:
                match = re.search(r'(?:Unit|Apt|#)\s*([A-Z0-9]+)', unit_elem, re.I)
                if match:
                    listing['unit'] = match.group(1)

            # Extract price
            price_elem = card.find(class_=lambda x: x and 'price' in x.lower())
            if price_elem:
                price_text = price_elem.get_text()
                price_match = re.search(r'\$([0-9,]+)', price_text.replace(',', ''))
                if price_match:
                    listing['price'] = int(price_match.group(1))

            # Extract beds
            beds_elem = card.find(string=re.compile(r'(\d+)\s*bed', re.I))
            if beds_elem:
                beds_match = re.search(r'(\d+)\s*bed', beds_elem, re.I)
                if beds_match:
                    listing['bedrooms'] = int(beds_match.group(1))

            if listing.get('unit'):
                listing['snapshot_timestamp'] = timestamp
                listing['snapshot_url'] = wayback_url
                listing['original_url'] = url
                listings.append(listing)

        return listings

    except Exception as e:
        return []

def save_wayback_listings(conn: sqlite3.Connection, listings: List[Dict]):
    """Save historical listings to database"""
    cursor = conn.cursor()

    # Create table for wayback listings
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS wayback_listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT DEFAULT 'WAYBACK_STREETEASY',
            address TEXT,
            unit_number TEXT,
            price INTEGER,
            bedrooms INTEGER,
            bathrooms REAL,
            snapshot_timestamp TEXT,
            snapshot_url TEXT,
            original_url TEXT,
            discovered_at DATETIME DEFAULT CURRENT_TIMESTAMP,

            UNIQUE(original_url, snapshot_timestamp, unit_number)
        )
    """)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wayback_address ON wayback_listings(address)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wayback_unit ON wayback_listings(unit_number)")

    inserted = 0
    for listing in listings:
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO wayback_listings
                (address, unit_number, price, bedrooms, snapshot_timestamp, snapshot_url, original_url)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                listing.get('address'),
                listing.get('unit'),
                listing.get('price'),
                listing.get('bedrooms'),
                listing.get('snapshot_timestamp'),
                listing.get('snapshot_url'),
                listing.get('original_url')
            ))

            if cursor.rowcount > 0:
                inserted += 1

        except Exception as e:
            continue

    conn.commit()
    return inserted

def main():
    print("="*60)
    print("WAYBACK MACHINE HISTORICAL LISTING SCRAPER")
    print("="*60)
    print(f"Started: {datetime.now()}")
    print()

    conn = sqlite3.connect(DB_PATH)

    # Sample buildings to scrape from Wayback
    # Start with major apartment buildings in NYC
    building_urls = [
        "streeteasy.com/building/stuy-town",
        "streeteasy.com/building/peter-cooper-village",
        "streeteasy.com/building/the-orion",
        "streeteasy.com/building/tribeca-green",
        # Add more buildings here
    ]

    total_listings = []

    for building_url in building_urls[:5]:  # Start with first 5 as test
        print(f"\n--- Scraping: {building_url} ---")

        # Get snapshots for this building
        snapshots = get_streeteasy_snapshots(building_url, limit=100)

        # Sample every 10th snapshot to avoid overwhelming
        sampled_snapshots = snapshots[::10]
        print(f"Sampling {len(sampled_snapshots)} snapshots")

        for i, snapshot in enumerate(sampled_snapshots[:20], 1):  # Limit to 20 snapshots per building for now
            print(f"  Snapshot {i}/20: {snapshot['timestamp']}")

            listings = scrape_building_page(snapshot['timestamp'], snapshot['original_url'])

            if listings:
                print(f"    Found {len(listings)} units")
                total_listings.extend(listings)
            else:
                print(f"    No units found")

            time.sleep(2)  # Be nice to Wayback Machine

    # Save all listings
    if total_listings:
        print(f"\nSaving {len(total_listings)} total listings...")
        inserted = save_wayback_listings(conn, total_listings)
        print(f"Inserted {inserted} new unique listings")

    conn.close()

    print()
    print("="*60)
    print("COMPLETE")
    print("="*60)
    print(f"Finished: {datetime.now()}")

if __name__ == "__main__":
    main()

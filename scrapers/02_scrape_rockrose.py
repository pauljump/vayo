#!/usr/bin/env python3
"""
Scrape Rockrose apartment availability
Rockrose has a clean /availabilities/ page with structured data
"""

import requests
import sqlite3
import json
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Dict, Any

ROCKROSE_URL = "https://rockrose.com/availabilities/"
DB_PATH = "/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"
USER_AGENT = "Vayo/1.0 (NYC Housing Research; hello@vayo.com)"

def create_listings_table(conn: sqlite3.Connection):
    """Create listings table if it doesn't exist"""
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS landlord_listings (
            listing_id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            url TEXT,

            -- Property identification
            building_name TEXT,
            address TEXT,
            neighborhood TEXT,
            unit_number TEXT,

            -- Unit details
            bedrooms INTEGER,
            bathrooms REAL,
            square_feet INTEGER,
            floor INTEGER,

            -- Pricing
            rent_price INTEGER,
            availability_date DATE,

            -- Amenities (JSON)
            building_amenities TEXT,
            unit_amenities TEXT,

            -- Contact
            contact_phone TEXT,
            contact_email TEXT,

            -- Metadata
            first_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
            last_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'active',
            raw_data TEXT
        )
    """)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_landlord_listings_source ON landlord_listings(source)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_landlord_listings_address ON landlord_listings(address)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_landlord_listings_status ON landlord_listings(status)")

    conn.commit()

def scrape_rockrose() -> List[Dict[str, Any]]:
    """Scrape Rockrose availabilities"""
    print(f"Fetching {ROCKROSE_URL}...")

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    }

    try:
        response = requests.get(ROCKROSE_URL, headers=headers, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        # Look for availability listings
        # This is site-specific - we'll need to inspect the HTML structure
        listings = []

        # Try to find common patterns:
        # 1. JSON-LD structured data
        json_ld = soup.find_all('script', type='application/ld+json')
        for script in json_ld:
            try:
                data = json.loads(script.string)
                if data.get('@type') == 'Apartment' or data.get('@type') == 'RealEstateListing':
                    listings.append(data)
            except:
                pass

        # 2. Look for unit cards/items
        unit_cards = soup.find_all(class_=lambda x: x and ('unit' in x.lower() or 'apartment' in x.lower() or 'listing' in x.lower()))

        for card in unit_cards[:50]:  # Limit to first 50 to avoid overload
            listing = extract_listing_from_card(card)
            if listing:
                listings.append(listing)

        print(f"Found {len(listings)} listings")
        return listings

    except Exception as e:
        print(f"Error scraping Rockrose: {e}")
        return []

def extract_listing_from_card(card) -> Dict[str, Any] | None:
    """Extract listing data from a unit card element"""
    try:
        listing = {}

        # Try to extract common fields
        # (This is a template - needs customization based on actual HTML structure)

        # Address/building
        address_elem = card.find(class_=lambda x: x and 'address' in x.lower())
        if address_elem:
            listing['address'] = address_elem.get_text(strip=True)

        # Unit number
        unit_elem = card.find(class_=lambda x: x and 'unit' in x.lower())
        if unit_elem:
            listing['unit_number'] = unit_elem.get_text(strip=True)

        # Bedrooms
        beds_elem = card.find(string=lambda x: x and 'bed' in x.lower())
        if beds_elem:
            import re
            beds_match = re.search(r'(\d+)\s*bed', beds_elem.lower())
            if beds_match:
                listing['bedrooms'] = int(beds_match.group(1))

        # Rent
        price_elem = card.find(class_=lambda x: x and ('price' in x.lower() or 'rent' in x.lower()))
        if price_elem:
            import re
            price_text = price_elem.get_text(strip=True)
            price_match = re.search(r'\$?([\d,]+)', price_text.replace(',', ''))
            if price_match:
                listing['rent_price'] = int(price_match.group(1))

        return listing if listing else None

    except Exception as e:
        print(f"Error extracting listing: {e}")
        return None

def save_listings(conn: sqlite3.Connection, listings: List[Dict[str, Any]], source: str):
    """Save listings to database"""
    cursor = conn.cursor()
    now = datetime.now().isoformat()

    for listing in listings:
        # Generate listing ID
        listing_id = f"{source}_{listing.get('address', '')}_{listing.get('unit_number', '')}".replace(' ', '_')

        try:
            cursor.execute("""
                INSERT INTO landlord_listings (
                    listing_id, source, address, unit_number,
                    bedrooms, bathrooms, square_feet,
                    rent_price, last_seen, raw_data
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(listing_id) DO UPDATE SET
                    last_seen = excluded.last_seen,
                    rent_price = excluded.rent_price,
                    raw_data = excluded.raw_data
            """, (
                listing_id,
                source,
                listing.get('address'),
                listing.get('unit_number'),
                listing.get('bedrooms'),
                listing.get('bathrooms'),
                listing.get('square_feet'),
                listing.get('rent_price'),
                now,
                json.dumps(listing)
            ))
        except Exception as e:
            print(f"Error saving listing {listing_id}: {e}")
            continue

    conn.commit()
    print(f"Saved {len(listings)} listings from {source}")

def main():
    print("=" * 60)
    print("Rockrose Availability Scraper")
    print("=" * 60)
    print()

    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    create_listings_table(conn)

    # Scrape listings
    listings = scrape_rockrose()

    # Save to database
    if listings:
        save_listings(conn, listings, "rockrose")

    conn.close()

    print()
    print("=" * 60)
    print("Complete!")
    print("=" * 60)

if __name__ == "__main__":
    main()

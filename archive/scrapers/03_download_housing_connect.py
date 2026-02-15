#!/usr/bin/env python3
"""
Download NYC Housing Connect affordable housing lottery data
From NYC Open Data official API
"""

import requests
import sqlite3
import time
from datetime import datetime
from typing import List, Dict, Any

# NYC Open Data Housing Connect endpoints
LOTTERY_BY_BUILDING_API = "https://data.cityofnewyork.us/resource/nibs-na6y.json"
LOTTERY_BY_LOTTERY_API = "https://data.cityofnewyork.us/resource/vy5i-a666.json"

DB_PATH = "/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"
BATCH_SIZE = 50000

def create_housing_connect_tables(conn: sqlite3.Connection):
    """Create Housing Connect tables"""
    cursor = conn.cursor()

    # Lotteries by building table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS housing_connect_buildings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lottery_id TEXT,

            -- Location
            address TEXT,
            bbl TEXT,
            borough TEXT,
            community_board TEXT,
            council_district TEXT,
            census_tract TEXT,
            bin TEXT,

            -- Units by income level
            extremely_low_income_units INTEGER,
            very_low_income_units INTEGER,
            low_income_units INTEGER,
            moderate_income_units INTEGER,
            middle_income_units INTEGER,
            other_income_units INTEGER,
            total_units INTEGER,

            -- Lottery details
            lottery_status TEXT,
            building_name TEXT,

            -- Metadata
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,

            UNIQUE(lottery_id, bbl)
        )
    """)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_hc_buildings_bbl ON housing_connect_buildings(bbl)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_hc_buildings_address ON housing_connect_buildings(address)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_hc_buildings_lottery_id ON housing_connect_buildings(lottery_id)")

    # Lotteries by lottery table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS housing_connect_lotteries (
            lottery_id TEXT PRIMARY KEY,

            -- Lottery details
            lottery_name TEXT,
            lottery_status TEXT,
            application_start_date TEXT,
            application_due_date TEXT,

            -- Preferences
            municipal_employees_pref REAL,
            nycha_residents_pref REAL,
            community_board_pref REAL,

            -- Contact
            contact_name TEXT,
            contact_email TEXT,
            contact_phone TEXT,

            -- Links
            lottery_url TEXT,

            -- Metadata
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    print("Created Housing Connect tables")

def fetch_housing_connect_data(endpoint: str, limit: int = BATCH_SIZE) -> List[Dict[str, Any]]:
    """Fetch data from Housing Connect API"""
    all_records = []
    offset = 0

    while True:
        url = f"{endpoint}?$limit={limit}&$offset={offset}"

        try:
            print(f"Fetching records {offset} to {offset + limit}...")
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            data = response.json()

            if not data:
                break

            all_records.extend(data)
            print(f"  Retrieved {len(data)} records (total: {len(all_records)})")

            if len(data) < limit:
                break

            offset += limit
            time.sleep(0.5)  # Rate limiting

        except Exception as e:
            print(f"Error fetching data: {e}")
            break

    return all_records

def insert_buildings(conn: sqlite3.Connection, buildings: List[Dict[str, Any]]):
    """Insert Housing Connect building data"""
    cursor = conn.cursor()

    for building in buildings:
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO housing_connect_buildings (
                    lottery_id, address, bbl, borough, community_board,
                    council_district, census_tract, bin,
                    extremely_low_income_units, very_low_income_units,
                    low_income_units, moderate_income_units,
                    middle_income_units, other_income_units, total_units,
                    lottery_status, building_name
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                building.get('lottery_id'),
                building.get('address'),
                building.get('bbl'),
                building.get('borough'),
                building.get('community_board'),
                building.get('council_district'),
                building.get('census_tract'),
                building.get('bin'),
                int(building.get('extremely_low_income_units', 0)) if building.get('extremely_low_income_units') else None,
                int(building.get('very_low_income_units', 0)) if building.get('very_low_income_units') else None,
                int(building.get('low_income_units', 0)) if building.get('low_income_units') else None,
                int(building.get('moderate_income_units', 0)) if building.get('moderate_income_units') else None,
                int(building.get('middle_income_units', 0)) if building.get('middle_income_units') else None,
                int(building.get('other_income_units', 0)) if building.get('other_income_units') else None,
                int(building.get('total_units', 0)) if building.get('total_units') else None,
                building.get('lottery_status'),
                building.get('building_name')
            ))
        except Exception as e:
            print(f"Error inserting building: {e}")
            continue

    conn.commit()

def insert_lotteries(conn: sqlite3.Connection, lotteries: List[Dict[str, Any]]):
    """Insert Housing Connect lottery data"""
    cursor = conn.cursor()

    for lottery in lotteries:
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO housing_connect_lotteries (
                    lottery_id, lottery_name, lottery_status,
                    application_start_date, application_due_date,
                    municipal_employees_pref, nycha_residents_pref,
                    community_board_pref,
                    contact_name, contact_email, contact_phone,
                    lottery_url
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                lottery.get('lottery_id'),
                lottery.get('lottery_name'),
                lottery.get('lottery_status'),
                lottery.get('application_start_date'),
                lottery.get('application_due_date'),
                float(lottery.get('municipal_employees_preference', 0)) if lottery.get('municipal_employees_preference') else None,
                float(lottery.get('nycha_residents_preference', 0)) if lottery.get('nycha_residents_preference') else None,
                float(lottery.get('community_board_preference', 0)) if lottery.get('community_board_preference') else None,
                lottery.get('contact_name'),
                lottery.get('contact_email'),
                lottery.get('contact_phone'),
                lottery.get('lottery_url')
            ))
        except Exception as e:
            print(f"Error inserting lottery: {e}")
            continue

    conn.commit()

def main():
    print("=" * 60)
    print("NYC Housing Connect Data Download")
    print("=" * 60)
    print()

    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    create_housing_connect_tables(conn)

    # Download buildings data
    print("Downloading Housing Connect buildings...")
    buildings = fetch_housing_connect_data(LOTTERY_BY_BUILDING_API)
    if buildings:
        insert_buildings(conn, buildings)
        print(f"Inserted {len(buildings)} building records")
    print()

    # Download lotteries data
    print("Downloading Housing Connect lotteries...")
    lotteries = fetch_housing_connect_data(LOTTERY_BY_LOTTERY_API)
    if lotteries:
        insert_lotteries(conn, lotteries)
        print(f"Inserted {len(lotteries)} lottery records")
    print()

    conn.close()

    print("=" * 60)
    print("Complete!")
    print("=" * 60)

if __name__ == "__main__":
    main()

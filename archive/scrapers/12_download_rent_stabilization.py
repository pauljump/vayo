#!/usr/bin/env python3
"""
Download Rent Stabilization Unit Counts
Buildings with rent-stabilized units
"""

import requests
import sqlite3
import time
from datetime import datetime

# NYC Rent Stabilization Counts by BBL
API_URL = "https://data.cityofnewyork.us/resource/uc8s-8pff.json"
DB_PATH = "/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"
BATCH_SIZE = 50000

def create_table(conn: sqlite3.Connection):
    """Create rent stabilization table"""
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS rent_stabilized_units")

    cursor.execute("""
        CREATE TABLE rent_stabilized_units (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bbl TEXT,
            borough TEXT,
            ucbbl TEXT,
            year TEXT,
            unitcount INTEGER,

            UNIQUE(bbl, year)
        )
    """)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_rentstab_bbl ON rent_stabilized_units(bbl)")

    conn.commit()
    print("Created rent_stabilized_units table")

def download_rentstab(conn: sqlite3.Connection):
    """Download rent stabilization data"""
    print("="*60)
    print("Downloading Rent Stabilization Unit Counts")
    print("="*60)
    print()

    cursor = conn.cursor()

    # Get total count
    print("Fetching total record count...")
    count_url = f"{API_URL}?$select=count(*)"
    try:
        response = requests.get(count_url, timeout=30)
        total_available = int(response.json()[0]['count'])
        print(f"Total records available: {total_available:,}")
        print()
    except Exception as e:
        print(f"Could not get count: {e}")
        total_available = 500000

    offset = 0
    total_inserted = 0
    batch_num = 0

    while offset < total_available:
        batch_num += 1
        print(f"Batch {batch_num}: Fetching records {offset:,} to {offset + BATCH_SIZE:,}...")

        url = f"{API_URL}?$limit={BATCH_SIZE}&$offset={offset}"

        try:
            response = requests.get(url, timeout=120)
            response.raise_for_status()
            records = response.json()

            if not records:
                break

            inserted = insert_rentstab(cursor, records)
            total_inserted += inserted

            print(f"  Retrieved {len(records)} records, inserted {inserted}")
            print(f"  Progress: {offset + len(records):,} / {total_available:,} ({100*(offset+len(records))/total_available:.1f}%)")
            print()

            if len(records) < BATCH_SIZE:
                break

            offset += len(records)
            conn.commit()
            time.sleep(1)

        except Exception as e:
            print(f"Error: {e}")
            time.sleep(10)
            continue

    conn.commit()

    print()
    print("="*60)
    print(f"Complete! Inserted {total_inserted:,} rent stabilization records")
    print("="*60)

def insert_rentstab(cursor, records):
    """Insert rent stabilization records"""
    inserted = 0

    for record in records:
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO rent_stabilized_units (
                    bbl, borough, ucbbl, year, unitcount
                ) VALUES (?, ?, ?, ?, ?)
            """, (
                record.get('bbl'),
                record.get('borough'),
                record.get('ucbbl'),
                record.get('year'),
                int(record.get('unitcount', 0)) if record.get('unitcount') else None
            ))

            if cursor.rowcount > 0:
                inserted += 1

        except Exception as e:
            continue

    return inserted

def main():
    print(f"Started: {datetime.now()}")
    print()

    conn = sqlite3.connect(DB_PATH)
    create_table(conn)
    download_rentstab(conn)
    conn.close()

    print()
    print(f"Finished: {datetime.now()}")

if __name__ == "__main__":
    main()

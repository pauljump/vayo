#!/usr/bin/env python3
"""
Download NYCHA Public Housing Development Data
Contains all NYC Housing Authority units
"""

import requests
import sqlite3
import time
from datetime import datetime

API_URL = "https://data.cityofnewyork.us/resource/evjd-dqpz.json"
DB_PATH = "/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"
BATCH_SIZE = 50000

def create_table(conn: sqlite3.Connection):
    """Create NYCHA housing table"""
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS nycha_developments")

    cursor.execute("""
        CREATE TABLE nycha_developments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            development TEXT,
            borough TEXT,
            program TEXT,
            tds TEXT,
            total_apartments INTEGER,
            current_apartments INTEGER,
            completion_date TEXT,

            UNIQUE(development, tds)
        )
    """)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_nycha_dev ON nycha_developments(development)")

    conn.commit()
    print("Created nycha_developments table")

def download_nycha(conn: sqlite3.Connection):
    """Download NYCHA developments"""
    print("="*60)
    print("Downloading NYCHA Public Housing Data")
    print("="*60)
    print()

    cursor = conn.cursor()

    # Get total count
    print("Fetching total record count...")
    count_url = f"{API_URL}?$select=count(*)"
    try:
        response = requests.get(count_url, timeout=30)
        total_available = int(response.json()[0]['count'])
        print(f"Total NYCHA developments available: {total_available:,}")
        print()
    except Exception as e:
        print(f"Could not get count: {e}")
        total_available = 10000

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

            inserted = insert_nycha(cursor, records)
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
    print(f"Complete! Inserted {total_inserted:,} NYCHA developments")
    print("="*60)

def insert_nycha(cursor, records):
    """Insert NYCHA records"""
    inserted = 0

    for record in records:
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO nycha_developments (
                    development, borough, program, tds,
                    total_apartments, current_apartments, completion_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                record.get('development'),
                record.get('borough'),
                record.get('program'),
                record.get('tds_'),
                int(record.get('total_number_of_apartments', 0)) if record.get('total_number_of_apartments') else None,
                int(record.get('number_of_current_apartments', 0)) if record.get('number_of_current_apartments') else None,
                record.get('completion_date')
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
    download_nycha(conn)
    conn.close()

    print()
    print(f"Finished: {datetime.now()}")

if __name__ == "__main__":
    main()

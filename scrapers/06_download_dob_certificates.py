#!/usr/bin/env python3
"""
Download DOB Certificates of Occupancy from NYC Open Data
These have official unit counts and sometimes enumerate individual units
"""

import requests
import sqlite3
import time
from datetime import datetime

API_URL = "https://data.cityofnewyork.us/resource/bs8b-p36w.json"
DB_PATH = "/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"
BATCH_SIZE = 50000

def create_table(conn: sqlite3.Connection):
    """Ensure certificates_of_occupancy table has correct schema"""
    cursor = conn.cursor()

    # Drop and recreate to ensure schema
    cursor.execute("DROP TABLE IF EXISTS certificates_of_occupancy_new")

    cursor.execute("""
        CREATE TABLE certificates_of_occupancy_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bin TEXT,
            job_number TEXT,
            co_issue_date TEXT,
            co_type TEXT,
            street_name TEXT,
            house_number TEXT,
            borough TEXT,
            block TEXT,
            lot TEXT,
            existing_occupancy TEXT,
            proposed_occupancy TEXT,
            existing_dwelling_units TEXT,
            proposed_dwelling_units TEXT,
            existing_stories TEXT,
            proposed_stories TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,

            UNIQUE(job_number, co_issue_date)
        )
    """)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_co_new_bin ON certificates_of_occupancy_new(bin)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_co_new_date ON certificates_of_occupancy_new(co_issue_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_co_new_borough ON certificates_of_occupancy_new(borough)")

    conn.commit()
    print("Created certificates_of_occupancy_new table")

def download_certificates(conn: sqlite3.Connection):
    """Download all DOB Certificates of Occupancy"""
    print("="*60)
    print("Downloading DOB Certificates of Occupancy")
    print("="*60)
    print()

    cursor = conn.cursor()

    # Get total count
    print("Fetching total record count...")
    count_url = f"{API_URL}?$select=count(*)"
    try:
        response = requests.get(count_url, timeout=30)
        total_available = int(response.json()[0]['count'])
        print(f"Total certificates available: {total_available:,}")
        print()
    except Exception as e:
        print(f"Could not get count: {e}")
        total_available = 1_000_000

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

            inserted = insert_certificates(cursor, records)
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
    print(f"Complete! Inserted {total_inserted:,} certificates")
    print("="*60)

def insert_certificates(cursor, records):
    """Insert certificate records"""
    inserted = 0

    for record in records:
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO certificates_of_occupancy_new (
                    bin, job_number, co_issue_date, co_type,
                    street_name, house_number, borough, block, lot,
                    existing_occupancy, proposed_occupancy,
                    existing_dwelling_units, proposed_dwelling_units,
                    existing_stories, proposed_stories
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                record.get('bin'),
                record.get('job__'),
                record.get('co_issue_date'),
                record.get('co_type'),
                record.get('street_name'),
                record.get('house__'),
                record.get('borough'),
                record.get('block'),
                record.get('lot'),
                record.get('existing_occupancy_classification'),
                record.get('proposed_occupancy_classification'),
                record.get('existing_no_of_dwelling_units'),
                record.get('proposed_no_of_dwelling_units'),
                record.get('existing_no_of_stories'),
                record.get('proposed_no_of_stories')
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
    download_certificates(conn)
    conn.close()

    print()
    print(f"Finished: {datetime.now()}")

if __name__ == "__main__":
    main()

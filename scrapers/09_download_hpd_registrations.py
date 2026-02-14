#!/usr/bin/env python3
"""
Download HPD Multiple Dwelling Registrations
Landlords must register all rental buildings with unit counts
"""

import requests
import sqlite3
import time
from datetime import datetime

API_URL = "https://data.cityofnewyork.us/resource/tesw-yqqr.json"
DB_PATH = "/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"
BATCH_SIZE = 50000

def create_table(conn: sqlite3.Connection):
    """Create HPD registrations table"""
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS hpd_registrations")

    cursor.execute("""
        CREATE TABLE hpd_registrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            registrationid TEXT,
            buildingid TEXT,
            boroid TEXT,
            bin TEXT,
            housenumber TEXT,
            streetname TEXT,
            zip TEXT,
            block TEXT,
            lot TEXT,
            communityboard TEXT,
            lastregistrationdate TEXT,
            registrationenddate TEXT,

            UNIQUE(registrationid)
        )
    """)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_hpd_reg_bin ON hpd_registrations(bin)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_hpd_reg_buildingid ON hpd_registrations(buildingid)")

    conn.commit()
    print("Created hpd_registrations table")

def download_registrations(conn: sqlite3.Connection):
    """Download HPD registrations"""
    print("="*60)
    print("Downloading HPD Multiple Dwelling Registrations")
    print("="*60)
    print()

    cursor = conn.cursor()

    # Get total count
    print("Fetching total record count...")
    count_url = f"{API_URL}?$select=count(*)"
    try:
        response = requests.get(count_url, timeout=30)
        total_available = int(response.json()[0]['count'])
        print(f"Total registrations available: {total_available:,}")
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

            inserted = insert_registrations(cursor, records)
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
    print(f"Complete! Inserted {total_inserted:,} registrations")
    print("="*60)

def insert_registrations(cursor, records):
    """Insert registration records"""
    inserted = 0

    for record in records:
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO hpd_registrations (
                    registrationid, buildingid, boroid, bin,
                    housenumber, streetname, zip, block, lot,
                    communityboard, lastregistrationdate, registrationenddate
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                record.get('registrationid'),
                record.get('buildingid'),
                record.get('boroid'),
                record.get('bin'),
                record.get('housenumber'),
                record.get('streetname'),
                record.get('zip'),
                record.get('block'),
                record.get('lot'),
                record.get('communityboard'),
                record.get('lastregistrationdate'),
                record.get('registrationenddate')
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
    download_registrations(conn)
    conn.close()

    print()
    print(f"Finished: {datetime.now()}")

if __name__ == "__main__":
    main()

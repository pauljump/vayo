#!/usr/bin/env python3
"""
Download HPD Registration Contacts
Contains unit count details for registered buildings
"""

import requests
import sqlite3
import time
from datetime import datetime

API_URL = "https://data.cityofnewyork.us/resource/feu5-w2e2.json"
DB_PATH = "/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"
BATCH_SIZE = 50000

def create_table(conn: sqlite3.Connection):
    """Create HPD contacts table"""
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS hpd_contacts")

    cursor.execute("""
        CREATE TABLE hpd_contacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            registrationcontactid TEXT,
            registrationid TEXT,
            buildingid TEXT,
            contacttype TEXT,
            corporationname TEXT,
            title TEXT,
            firstname TEXT,
            lastname TEXT,

            UNIQUE(registrationcontactid)
        )
    """)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_hpd_contact_regid ON hpd_contacts(registrationid)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_hpd_contact_buildingid ON hpd_contacts(buildingid)")

    conn.commit()
    print("Created hpd_contacts table")

def download_contacts(conn: sqlite3.Connection):
    """Download HPD contacts"""
    print("="*60)
    print("Downloading HPD Registration Contacts")
    print("="*60)
    print()

    cursor = conn.cursor()

    # Get total count
    print("Fetching total record count...")
    count_url = f"{API_URL}?$select=count(*)"
    try:
        response = requests.get(count_url, timeout=30)
        total_available = int(response.json()[0]['count'])
        print(f"Total contacts available: {total_available:,}")
        print()
    except Exception as e:
        print(f"Could not get count: {e}")
        total_available = 5_000_000

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

            inserted = insert_contacts(cursor, records)
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
    print(f"Complete! Inserted {total_inserted:,} contacts")
    print("="*60)

def insert_contacts(cursor, records):
    """Insert contact records"""
    inserted = 0

    for record in records:
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO hpd_contacts (
                    registrationcontactid, registrationid, buildingid,
                    contacttype, corporationname, title, firstname, lastname
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                record.get('registrationcontactid'),
                record.get('registrationid'),
                record.get('buildingid'),
                record.get('type'),
                record.get('corporationname'),
                record.get('title'),
                record.get('firstname'),
                record.get('lastname')
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
    download_contacts(conn)
    conn.close()

    print()
    print(f"Finished: {datetime.now()}")

if __name__ == "__main__":
    main()

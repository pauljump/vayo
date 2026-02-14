#!/usr/bin/env python3
"""
Download FULL 311 Service Requests dataset from NYC Open Data
Current database has only 150K - full dataset has 30M+
"""

import requests
import sqlite3
import time
from datetime import datetime

# NYC Open Data 311 endpoint
API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
DB_PATH = "/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"
BATCH_SIZE = 50000

def download_311_full(conn: sqlite3.Connection):
    """Download full 311 dataset"""
    print("="*60)
    print("Downloading Full 311 Service Requests")
    print("="*60)
    print()

    cursor = conn.cursor()

    # Get current count
    cursor.execute("SELECT COUNT(*) FROM service_requests_311")
    current_count = cursor.fetchone()[0]
    print(f"Current 311 records in database: {current_count:,}")
    print()

    # Get total available
    print("Fetching total record count...")
    count_url = f"{API_URL}?$select=count(*)"
    try:
        response = requests.get(count_url, timeout=30)
        total_available = int(response.json()[0]['count'])
        print(f"Total 311 records available: {total_available:,}")
        print(f"Records to download: {total_available - current_count:,}")
        print()
    except Exception as e:
        print(f"Could not get total count: {e}")
        print("Proceeding with download anyway...")
        total_available = 50_000_000  # Assume huge number

    # Download in batches
    offset = current_count  # Start from where we left off
    total_inserted = 0
    batch_num = 0

    while offset < total_available:
        batch_num += 1
        print(f"Batch {batch_num}: Fetching records {offset:,} to {offset + BATCH_SIZE:,}...")

        url = f"{API_URL}?$limit={BATCH_SIZE}&$offset={offset}&$order=unique_key"

        try:
            response = requests.get(url, timeout=120)
            response.raise_for_status()
            records = response.json()

            if not records:
                print("No more records available")
                break

            # Insert records
            inserted = insert_311_records(cursor, records)
            total_inserted += inserted

            print(f"  Retrieved {len(records)} records, inserted {inserted} new")
            print(f"  Progress: {offset + len(records):,} / {total_available:,} ({100*(offset + len(records))/total_available:.1f}%)")
            print()

            if len(records) < BATCH_SIZE:
                print("Received partial batch - reached end")
                break

            offset += len(records)

            # Commit every batch
            conn.commit()

            # Rate limiting
            time.sleep(1)

        except Exception as e:
            print(f"Error downloading batch: {e}")
            print("Waiting 10 seconds before retry...")
            time.sleep(10)
            continue

    conn.commit()

    print()
    print("="*60)
    print(f"Download complete! Inserted {total_inserted:,} new 311 records")
    print("="*60)

def insert_311_records(cursor, records):
    """Insert 311 records into database"""
    inserted = 0

    for record in records:
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO service_requests_311 (
                    unique_key, created_date, closed_date,
                    agency, agency_name, complaint_type, descriptor,
                    incident_address, street_name,
                    city, borough, bbl,
                    latitude, longitude, status,
                    resolution_description
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                record.get('unique_key'),
                record.get('created_date'),
                record.get('closed_date'),
                record.get('agency'),
                record.get('agency_name'),
                record.get('complaint_type'),
                record.get('descriptor'),
                record.get('incident_address'),
                record.get('street_name'),
                record.get('city'),
                record.get('borough'),
                record.get('bbl'),
                float(record.get('latitude')) if record.get('latitude') else None,
                float(record.get('longitude')) if record.get('longitude') else None,
                record.get('status'),
                record.get('resolution_description')
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
    download_311_full(conn)
    conn.close()

    print()
    print(f"Finished: {datetime.now()}")

if __name__ == "__main__":
    main()

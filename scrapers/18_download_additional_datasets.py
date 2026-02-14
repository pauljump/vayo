#!/usr/bin/env python3
"""
Download additional FREE NYC Open Data sources that might have apartment numbers
"""

import requests
import sqlite3
import time
from datetime import datetime

DB_PATH = "/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"

# Additional datasets to try
DATASETS = {
    'dob_job_applications': {
        'id': 'ic3t-wcy2',
        'name': 'DOB Job Application Filings',
        'fields': ['bbl', 'bin', 'house__', 'street_name', 'work_type', 'existing_dwelling_units', 'proposed_dwelling_units'],
        'text_fields': ['work_type']
    },
    'dob_safety_violations': {
        'id': 'w7w3-xahh',
        'name': 'DOB Elevator Device Violations',
        'fields': ['bin', 'house_number', 'street_name', 'violation_type'],
        'text_fields': ['violation_type']
    },
    'oath_hearings': {
        'id': 'jz4z-kudi',
        'name': 'OATH Hearings',
        'fields': ['respondent_name', 'violation_location', 'violation_details'],
        'text_fields': ['violation_location', 'violation_details']
    },
}

def download_dataset(dataset_id, dataset_name, fields, text_fields):
    """Download a dataset from NYC Open Data"""

    print(f"\n{'='*60}")
    print(f"Downloading: {dataset_name}")
    print(f"Dataset ID: {dataset_id}")
    print(f"{'='*60}")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Create table
    table_name = dataset_id.replace('-', '_')

    # Drop and recreate
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    # Create columns dynamically
    col_defs = ", ".join([f"{field.replace('__', '_')} TEXT" for field in fields])
    cursor.execute(f"""
        CREATE TABLE {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            {col_defs},
            raw_text TEXT
        )
    """)

    api_base = f"https://data.cityofnewyork.us/resource/{dataset_id}.json"

    offset = 0
    limit = 50000
    total_inserted = 0

    while True:
        url = f"{api_base}?$limit={limit}&$offset={offset}"

        try:
            response = requests.get(url, timeout=300)
            response.raise_for_status()
            records = response.json()

            if not records:
                break

            for record in records:
                try:
                    # Extract values for defined fields
                    values = [record.get(field, '') for field in fields]

                    # Combine text fields for mining
                    raw_text = ' '.join([str(record.get(field, '')) for field in text_fields])

                    values.append(raw_text)

                    placeholders = ', '.join(['?' for _ in range(len(fields) + 1)])
                    cursor.execute(f"""
                        INSERT INTO {table_name}
                        ({', '.join([f.replace('__', '_') for f in fields])}, raw_text)
                        VALUES ({placeholders})
                    """, values)

                except Exception as e:
                    print(f"Error inserting record: {e}")
                    continue

            conn.commit()
            total_inserted += len(records)
            print(f"  Downloaded {total_inserted:,} records...")

            offset += limit
            time.sleep(0.5)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print(f"  Dataset not found or unavailable")
                break
            else:
                print(f"  HTTP error: {e}")
                break
        except Exception as e:
            print(f"  Error at offset {offset}: {e}")
            time.sleep(5)
            continue

    conn.close()
    print(f"  Complete: {total_inserted:,} total records")
    return total_inserted

def main():
    print("="*60)
    print("DOWNLOADING ADDITIONAL NYC OPEN DATA SOURCES")
    print(f"Started: {datetime.now()}")
    print("="*60)

    results = {}

    for key, dataset in DATASETS.items():
        try:
            count = download_dataset(
                dataset['id'],
                dataset['name'],
                dataset['fields'],
                dataset['text_fields']
            )
            results[dataset['name']] = count
        except Exception as e:
            print(f"Failed to download {dataset['name']}: {e}")
            results[dataset['name']] = 0

    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    for name, count in results.items():
        print(f"{name}: {count:,} records")

    print(f"\nFinished: {datetime.now()}")

if __name__ == "__main__":
    main()

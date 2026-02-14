#!/usr/bin/env python3
"""
Download NYC PLUTO data from Open Data API
Provides comprehensive building and lot data for all of NYC
"""

import requests
import sqlite3
import time
from typing import Iterator, Dict, Any

# NYC Open Data PLUTO endpoint
PLUTO_ENDPOINT = "https://data.cityofnewyork.us/resource/64uk-42ks.json"
DB_PATH = "/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"

# Batch size for API requests
BATCH_SIZE = 50000
MAX_RETRIES = 3

def fetch_pluto_batch(offset: int, limit: int) -> Iterator[Dict[str, Any]]:
    """Fetch a batch of PLUTO records from the API"""
    url = f"{PLUTO_ENDPOINT}?$limit={limit}&$offset={offset}"

    for attempt in range(MAX_RETRIES):
        try:
            print(f"Fetching records {offset} to {offset + limit}... (attempt {attempt + 1})")
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            data = response.json()
            print(f"  Retrieved {len(data)} records")
            return data
        except Exception as e:
            print(f"  Error: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(5 * (attempt + 1))
            else:
                raise

    return []

def create_pluto_table(conn: sqlite3.Connection):
    """Create PLUTO table if it doesn't exist"""
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pluto (
            -- Identifiers
            bbl TEXT PRIMARY KEY,
            borough TEXT,
            block TEXT,
            lot TEXT,

            -- Address
            address TEXT,
            zipcode TEXT,

            -- Owner
            ownername TEXT,

            -- Building characteristics
            bldgclass TEXT,
            landuse TEXT,
            yearbuilt INTEGER,
            yearalter1 INTEGER,
            yearalter2 INTEGER,

            -- Units
            unitsres INTEGER,
            unitstotal INTEGER,
            numbldgs INTEGER,
            numfloors REAL,

            -- Areas
            lotarea INTEGER,
            bldgarea INTEGER,
            comarea INTEGER,
            resarea INTEGER,
            officearea INTEGER,
            retailarea INTEGER,

            -- Dimensions
            lotfront REAL,
            lotdepth REAL,
            bldgfront REAL,
            bldgdepth REAL,

            -- Zoning
            zonedist1 TEXT,
            splitzone BOOLEAN,

            -- Assessment
            assessland REAL,
            assesstot REAL,
            exempttot REAL,

            -- Geographic
            council TEXT,
            cd TEXT,
            ct2010 TEXT,
            schooldist TEXT,
            policeprct TEXT,
            firecomp TEXT,

            -- Coordinates
            latitude REAL,
            longitude REAL,
            xcoord INTEGER,
            ycoord INTEGER,

            -- Metadata
            version TEXT,
            appdate TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_pluto_borough ON pluto(borough)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_pluto_address ON pluto(address)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_pluto_zipcode ON pluto(zipcode)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_pluto_owner ON pluto(ownername)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_pluto_unitsres ON pluto(unitsres)")

    conn.commit()
    print("Created PLUTO table")

def insert_pluto_records(conn: sqlite3.Connection, records: list[Dict[str, Any]]):
    """Insert PLUTO records into database"""
    cursor = conn.cursor()

    for record in records:
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO pluto (
                    bbl, borough, block, lot, address, zipcode, ownername,
                    bldgclass, landuse, yearbuilt, yearalter1, yearalter2,
                    unitsres, unitstotal, numbldgs, numfloors,
                    lotarea, bldgarea, comarea, resarea, officearea, retailarea,
                    lotfront, lotdepth, bldgfront, bldgdepth,
                    zonedist1, splitzone,
                    assessland, assesstot, exempttot,
                    council, cd, ct2010, schooldist, policeprct, firecomp,
                    latitude, longitude, xcoord, ycoord,
                    version, appdate
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?,
                    ?, ?, ?,
                    ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?
                )
            """, (
                record.get('bbl'),
                record.get('borough'),
                record.get('block'),
                record.get('lot'),
                record.get('address'),
                record.get('zipcode'),
                record.get('ownername'),
                record.get('bldgclass'),
                record.get('landuse'),
                int(record.get('yearbuilt', 0)) if record.get('yearbuilt') else None,
                int(record.get('yearalter1', 0)) if record.get('yearalter1') else None,
                int(record.get('yearalter2', 0)) if record.get('yearalter2') else None,
                int(record.get('unitsres', 0)) if record.get('unitsres') else None,
                int(record.get('unitstotal', 0)) if record.get('unitstotal') else None,
                int(record.get('numbldgs', 0)) if record.get('numbldgs') else None,
                float(record.get('numfloors', 0)) if record.get('numfloors') else None,
                int(record.get('lotarea', 0)) if record.get('lotarea') else None,
                int(record.get('bldgarea', 0)) if record.get('bldgarea') else None,
                int(record.get('comarea', 0)) if record.get('comarea') else None,
                int(record.get('resarea', 0)) if record.get('resarea') else None,
                int(record.get('officearea', 0)) if record.get('officearea') else None,
                int(record.get('retailarea', 0)) if record.get('retailarea') else None,
                float(record.get('lotfront', 0)) if record.get('lotfront') else None,
                float(record.get('lotdepth', 0)) if record.get('lotdepth') else None,
                float(record.get('bldgfront', 0)) if record.get('bldgfront') else None,
                float(record.get('bldgdepth', 0)) if record.get('bldgdepth') else None,
                record.get('zonedist1'),
                record.get('splitzone') == 'true' if record.get('splitzone') else False,
                float(record.get('assessland', 0)) if record.get('assessland') else None,
                float(record.get('assesstot', 0)) if record.get('assesstot') else None,
                float(record.get('exempttot', 0)) if record.get('exempttot') else None,
                record.get('council'),
                record.get('cd'),
                record.get('ct2010'),
                record.get('schooldist'),
                record.get('policeprct'),
                record.get('firecomp'),
                float(record.get('latitude')) if record.get('latitude') else None,
                float(record.get('longitude')) if record.get('longitude') else None,
                int(record.get('xcoord')) if record.get('xcoord') else None,
                int(record.get('ycoord')) if record.get('ycoord') else None,
                record.get('version'),
                record.get('appdate')
            ))
        except Exception as e:
            print(f"Error inserting record {record.get('bbl')}: {e}")
            continue

    conn.commit()

def main():
    print("=" * 60)
    print("NYC PLUTO Data Download")
    print("=" * 60)
    print()

    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    create_pluto_table(conn)

    # Get total count estimate
    print("Fetching total record count...")
    count_url = f"{PLUTO_ENDPOINT}?$select=count(*)"
    count_resp = requests.get(count_url)
    total_records = int(count_resp.json()[0]['count'])
    print(f"Total PLUTO records available: {total_records:,}")
    print()

    # Download in batches
    offset = 0
    total_inserted = 0

    while offset < total_records:
        batch = fetch_pluto_batch(offset, BATCH_SIZE)

        if not batch:
            break

        insert_pluto_records(conn, batch)
        total_inserted += len(batch)

        print(f"Progress: {total_inserted:,} / {total_records:,} ({100*total_inserted/total_records:.1f}%)")
        print()

        offset += BATCH_SIZE

        # Rate limiting
        time.sleep(1)

    conn.close()

    print("=" * 60)
    print(f"Complete! Downloaded {total_inserted:,} PLUTO records")
    print("=" * 60)

if __name__ == "__main__":
    main()

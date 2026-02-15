#!/usr/bin/env python3
"""
Text mine unit numbers from existing data
Extract units from DOB permits, ECB violations, 311 requests, ACRIS parties
"""

import re
import sqlite3
from typing import Optional, Set, Tuple
from datetime import datetime

DB_PATH = "/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"

# Comprehensive unit number patterns
UNIT_PATTERNS = [
    # Standard formats
    r'(?:APT|APARTMENT|UNIT|SUITE|#|APT\.)\s*([0-9]{1,4}[A-Z]?)',
    r'(?:APT|APARTMENT|UNIT|SUITE)\s+([A-Z]-?[0-9]+)',

    # Letter-number combos
    r'\b([0-9]{1,2}[A-Z])\b',  # "3A", "12B"
    r'\b([A-Z][0-9]{1,3})\b',  # "A12", "B204"

    # Floor-unit combos
    r'(?:FLOOR|FL)\s*([0-9]+)\s*(?:APT|UNIT)\s*([A-Z0-9]+)',

    # Plural units
    r'UNITS?\s+([0-9]+[A-Z]?)\s+(?:AND|&)\s+([0-9]+[A-Z]?)',

    # Numbered only (be careful - lots of false positives)
    r'(?:APT|APARTMENT|UNIT)\s+([0-9]{1,4})\b',

    # PH (penthouse)
    r'\b(PH-?[A-Z0-9]+)\b',
    r'\b(PENTHOUSE)\b',
]

def extract_unit_numbers(text: str) -> Set[str]:
    """Extract all unit numbers from text"""
    if not text:
        return set()

    units = set()
    text_upper = text.upper()

    for pattern in UNIT_PATTERNS:
        matches = re.finditer(pattern, text_upper)
        for match in matches:
            # Get all captured groups
            for group in match.groups():
                if group and group not in ['AND', '&', 'FLOOR', 'FL']:
                    # Clean up the unit number
                    unit = group.strip().replace(' ', '').replace('-', '')

                    # Filter out obvious false positives
                    if is_valid_unit(unit):
                        units.add(unit)

    return units

def is_valid_unit(unit: str) -> bool:
    """Check if extracted string is likely a valid unit number"""
    if not unit or len(unit) > 10:
        return False

    # Skip common false positives
    false_positives = [
        'FLOOR', 'STORY', 'STORIES', 'FEET', 'INCH', 'YEAR',
        'ST', 'ND', 'RD', 'TH', 'STREET', 'AVENUE', 'AVE'
    ]

    if unit in false_positives:
        return False

    # Must have at least one digit or be "PH"/"PENTHOUSE"
    if not any(c.isdigit() for c in unit) and unit not in ['PH', 'PENTHOUSE']:
        return False

    return True

def extract_units_from_dob_permits(conn: sqlite3.Connection) -> int:
    """Extract units from DOB permit descriptions"""
    print("\n" + "="*60)
    print("Extracting units from DOB Permits")
    print("="*60)

    cursor = conn.cursor()

    # Get all permits with job descriptions
    cursor.execute("""
        SELECT permit_id, bin, block, lot, borough, job_description, house_number, street_name
        FROM dob_permits
        WHERE job_description IS NOT NULL
        AND job_description != ''
    """)

    units_found = []
    processed = 0

    for row in cursor.fetchall():
        permit_id, bin_id, block, lot, borough, description, house_num, street = row

        # Extract units
        unit_numbers = extract_unit_numbers(description)

        for unit_num in unit_numbers:
            # Try to construct address
            address = None
            if house_num and street:
                address = f"{house_num} {street}"

            # Construct BBL if we have all parts
            bbl = None
            if borough and block and lot:
                try:
                    bbl = f"{borough}{block.zfill(5)}{lot.zfill(4)}"
                except:
                    pass

            units_found.append({
                'source': 'DOB_PERMITS',
                'source_id': permit_id,
                'bin': bin_id,
                'bbl': bbl,
                'unit_number': unit_num,
                'address': address,
                'raw_text': description[:500]  # Store snippet for verification
            })

        processed += 1
        if processed % 10000 == 0:
            print(f"  Processed {processed:,} permits, found {len(units_found):,} units so far...")

    print(f"\nTotal processed: {processed:,}")
    print(f"Total units extracted: {len(units_found):,}")

    return save_discovered_units(conn, units_found)

def extract_units_from_ecb_violations(conn: sqlite3.Connection) -> int:
    """Extract units from ECB violation descriptions"""
    print("\n" + "="*60)
    print("Extracting units from ECB Violations")
    print("="*60)

    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, bin, block, lot, boro, violation_description, respondent_house, respondent_street
        FROM ecb_violations
        WHERE violation_description IS NOT NULL
        AND violation_description != ''
    """)

    units_found = []
    processed = 0

    for row in cursor.fetchall():
        violation_id, bin_id, block, lot, borough, description, house_num, street = row

        unit_numbers = extract_unit_numbers(description)

        for unit_num in unit_numbers:
            address = None
            if house_num and street:
                address = f"{house_num} {street}"

            bbl = None
            if borough and block and lot:
                try:
                    bbl = f"{borough}{block.zfill(5)}{lot.zfill(4)}"
                except:
                    pass

            units_found.append({
                'source': 'ECB_VIOLATIONS',
                'source_id': violation_id,
                'bin': bin_id,
                'bbl': bbl,
                'unit_number': unit_num,
                'address': address,
                'raw_text': description[:500]
            })

        processed += 1
        if processed % 10000 == 0:
            print(f"  Processed {processed:,} violations, found {len(units_found):,} units so far...")

    print(f"\nTotal processed: {processed:,}")
    print(f"Total units extracted: {len(units_found):,}")

    return save_discovered_units(conn, units_found)

def extract_units_from_311(conn: sqlite3.Connection) -> int:
    """Extract units from 311 service request addresses"""
    print("\n" + "="*60)
    print("Extracting units from 311 Service Requests")
    print("="*60)

    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, incident_address, bbl, borough
        FROM service_requests_311
        WHERE incident_address IS NOT NULL
        AND incident_address != ''
    """)

    units_found = []
    processed = 0

    for row in cursor.fetchall():
        request_id, address, bbl, borough = row

        unit_numbers = extract_unit_numbers(address)

        for unit_num in unit_numbers:
            units_found.append({
                'source': '311_REQUESTS',
                'source_id': request_id,
                'bin': None,
                'bbl': bbl,
                'unit_number': unit_num,
                'address': address,
                'raw_text': address
            })

        processed += 1
        if processed % 1000 == 0:
            print(f"  Processed {processed:,} requests, found {len(units_found):,} units so far...")

    print(f"\nTotal processed: {processed:,}")
    print(f"Total units extracted: {len(units_found):,}")

    return save_discovered_units(conn, units_found)

def save_discovered_units(conn: sqlite3.Connection, units_data: list) -> int:
    """Save discovered units to database"""
    if not units_data:
        return 0

    cursor = conn.cursor()

    # Create table for text-mined units
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS text_mined_units (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            source_id TEXT,
            bin TEXT,
            bbl TEXT,
            unit_number TEXT NOT NULL,
            address TEXT,
            raw_text TEXT,
            discovered_at DATETIME DEFAULT CURRENT_TIMESTAMP,

            UNIQUE(source, source_id, unit_number)
        )
    """)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_text_mined_bin ON text_mined_units(bin)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_text_mined_bbl ON text_mined_units(bbl)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_text_mined_unit ON text_mined_units(unit_number)")

    # Insert units
    inserted = 0
    for unit in units_data:
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO text_mined_units
                (source, source_id, bin, bbl, unit_number, address, raw_text)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                unit['source'],
                unit['source_id'],
                unit['bin'],
                unit['bbl'],
                unit['unit_number'],
                unit['address'],
                unit['raw_text']
            ))
            if cursor.rowcount > 0:
                inserted += 1
        except Exception as e:
            continue

    conn.commit()
    print(f"  Saved {inserted:,} new unique units to database")

    return inserted

def merge_to_canonical_units(conn: sqlite3.Connection):
    """Merge text-mined units into canonical_units table"""
    print("\n" + "="*60)
    print("Merging text-mined units into canonical_units")
    print("="*60)

    cursor = conn.cursor()

    # Insert units that don't exist yet
    cursor.execute("""
        INSERT OR IGNORE INTO canonical_units (
            unit_id, bbl, bin, unit_number, full_address,
            source_systems, confidence_score, verified
        )
        SELECT
            COALESCE(bbl, bin) || '-' || unit_number as unit_id,
            bbl,
            bin,
            unit_number,
            address,
            '["TEXT_MINED"]' as source_systems,
            0.6 as confidence_score,
            0 as verified
        FROM text_mined_units
        WHERE (bbl IS NOT NULL OR bin IS NOT NULL)
        AND NOT EXISTS (
            SELECT 1 FROM canonical_units c
            WHERE (c.bbl = text_mined_units.bbl OR c.bin = text_mined_units.bin)
            AND c.unit_number = text_mined_units.unit_number
        )
    """)

    new_units = cursor.rowcount
    print(f"Added {new_units:,} new units to canonical_units")

    conn.commit()

    return new_units

def main():
    print("="*60)
    print("TEXT MINING UNIT DISCOVERY")
    print("="*60)
    print(f"Started: {datetime.now()}")
    print()

    conn = sqlite3.connect(DB_PATH)

    total_new = 0

    # Extract from each source
    print("\n--- Phase 1: DOB Permits ---")
    dob_units = extract_units_from_dob_permits(conn)
    total_new += dob_units

    print("\n--- Phase 2: ECB Violations ---")
    ecb_units = extract_units_from_ecb_violations(conn)
    total_new += ecb_units

    print("\n--- Phase 3: 311 Service Requests ---")
    service_311_units = extract_units_from_311(conn)
    total_new += service_311_units

    # Merge into canonical units
    print("\n--- Phase 4: Merge to Canonical Units ---")
    merged = merge_to_canonical_units(conn)

    # Final stats
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM text_mined_units")
    total_mined = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM canonical_units")
    total_canonical = cursor.fetchone()[0]

    conn.close()

    print("\n" + "="*60)
    print("TEXT MINING COMPLETE")
    print("="*60)
    print(f"Total text-mined units: {total_mined:,}")
    print(f"New units added to canonical_units: {merged:,}")
    print(f"Total canonical units now: {total_canonical:,}")
    print(f"Finished: {datetime.now()}")
    print("="*60)

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Process Phase 2 data to extract additional units:
1. Full 311 dataset (30M records) - extract units from addresses
2. DOB Certificates of Occupancy - extract unit counts
"""

import sqlite3
import re
from datetime import datetime
from typing import List, Dict, Set

DB_PATH = "/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"

# Same unit extraction patterns from text mining
UNIT_PATTERNS = [
    r'(?:APT|APARTMENT|UNIT|SUITE|#|APT\.)\s*([0-9]{1,4}[A-Z]?)',
    r'(?:APT|APARTMENT|UNIT|SUITE)\s+([A-Z]-?[0-9]+)',
    r'\b([0-9]{1,2}[A-Z])\b',  # "3A", "12B"
    r'\b([A-Z][0-9]{1,3})\b',  # "A12", "B204"
    r'\b(PH-?[A-Z0-9]+)\b',     # Penthouse
    r'(?:FLOOR|FL|FLR)\s*([0-9]{1,2})',  # Floor numbers sometimes indicate units
    r'\b([0-9]{3,4})\b(?=\s*$)',  # 3-4 digit numbers at end of address
]

def extract_unit_numbers(text: str) -> Set[str]:
    """Extract unit numbers from text using regex patterns"""
    if not text:
        return set()

    units = set()
    text_upper = text.upper()

    for pattern in UNIT_PATTERNS:
        matches = re.findall(pattern, text_upper)
        for match in matches:
            # Clean up the unit number
            unit = match.strip()

            # Validate unit number (basic filtering)
            if len(unit) >= 1 and len(unit) <= 6:
                # Exclude common false positives
                if unit not in ['ST', 'AVE', 'ROAD', 'DR', 'PL', 'CT', 'LN', 'WAY']:
                    units.add(unit)

    return units

def extract_units_from_new_311(conn: sqlite3.Connection) -> int:
    """Extract units from newly downloaded 311 records"""
    print("="*60)
    print("Extracting units from NEW 311 Service Requests")
    print("="*60)

    cursor = conn.cursor()

    # Get count of new 311 records (those not already text-mined)
    cursor.execute("""
        SELECT COUNT(*)
        FROM service_requests_311
        WHERE incident_address IS NOT NULL
        AND incident_address != ''
        AND unique_key NOT IN (
            SELECT source_id FROM text_mined_units WHERE source = '311_REQUESTS'
        )
    """)
    total_new = cursor.fetchone()[0]
    print(f"New 311 records to process: {total_new:,}")
    print()

    # Extract units from new records
    cursor.execute("""
        SELECT unique_key, incident_address, bbl, borough
        FROM service_requests_311
        WHERE incident_address IS NOT NULL
        AND incident_address != ''
        AND unique_key NOT IN (
            SELECT source_id FROM text_mined_units WHERE source = '311_REQUESTS'
        )
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
        if processed % 10000 == 0:
            print(f"  Processed {processed:,} requests, found {len(units_found):,} units so far...")

    print(f"\nTotal processed: {processed:,}")
    print(f"Total units extracted: {len(units_found):,}")

    # Save to text_mined_units
    inserted = save_units(conn, units_found)
    return inserted

def extract_units_from_certificates(conn: sqlite3.Connection) -> int:
    """Extract units from DOB Certificates of Occupancy"""
    print("\n" + "="*60)
    print("Extracting units from DOB Certificates")
    print("="*60)

    cursor = conn.cursor()

    # Certificates with proposed dwelling units > 0
    cursor.execute("""
        SELECT bin, job_number,
               CAST(proposed_dwelling_units AS INTEGER) as units,
               street_name, house_number, borough
        FROM certificates_of_occupancy_new
        WHERE proposed_dwelling_units IS NOT NULL
        AND proposed_dwelling_units != ''
        AND CAST(proposed_dwelling_units AS INTEGER) > 0
        AND CAST(proposed_dwelling_units AS INTEGER) < 1000
    """)

    units_found = []
    buildings_processed = 0

    for row in cursor.fetchall():
        bin_id, job_number, num_units, street, house, borough = row

        # For buildings with unit counts, we'll create placeholder units
        # numbered 1 through N (we'll mark these as lower confidence)
        address = f"{house} {street}, {borough}" if house and street else None

        for unit_num in range(1, min(num_units + 1, 101)):  # Cap at 100 units per building
            units_found.append({
                'source': 'DOB_CERTIFICATES',
                'source_id': job_number,
                'bin': bin_id,
                'bbl': None,
                'unit_number': str(unit_num),
                'address': address,
                'raw_text': f"CO shows {num_units} proposed units"
            })

        buildings_processed += 1
        if buildings_processed % 1000 == 0:
            print(f"  Processed {buildings_processed:,} certificates, found {len(units_found):,} units so far...")

    print(f"\nTotal certificates processed: {buildings_processed:,}")
    print(f"Total units extracted: {len(units_found):,}")

    inserted = save_units(conn, units_found)
    return inserted

def save_units(conn: sqlite3.Connection, units_data: list) -> int:
    """Save units to text_mined_units table"""
    if not units_data:
        return 0

    cursor = conn.cursor()

    inserted = 0
    for unit in units_data:
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO text_mined_units (
                    source, source_id, bin, bbl, unit_number, address, raw_text
                )
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
    print(f"  Saved {inserted:,} new unique units to text_mined_units")

    return inserted

def merge_to_canonical(conn: sqlite3.Connection) -> int:
    """Merge new text-mined units into canonical_units"""
    print("\n" + "="*60)
    print("Merging new units into canonical_units")
    print("="*60)

    cursor = conn.cursor()

    # Use same optimized merge approach as before
    # BBL-based merge
    cursor.execute("""
        INSERT OR IGNORE INTO canonical_units (
            unit_id, bbl, bin, unit_number, full_address,
            source_systems, confidence_score, verified
        )
        SELECT
            COALESCE(t.bbl, t.bin) || '-' || t.unit_number as unit_id,
            t.bbl, t.bin, t.unit_number, t.address,
            '["PHASE2_' || t.source || '"]', 0.6, 0
        FROM text_mined_units t
        LEFT JOIN canonical_units c ON c.bbl = t.bbl AND c.unit_number = t.unit_number
        WHERE t.bbl IS NOT NULL
        AND c.unit_id IS NULL
        AND t.source IN ('311_REQUESTS', 'DOB_CERTIFICATES')
    """)

    bbl_added = cursor.rowcount
    print(f"Added {bbl_added:,} units matched by BBL")

    # BIN-based merge
    cursor.execute("""
        INSERT OR IGNORE INTO canonical_units (
            unit_id, bbl, bin, unit_number, full_address,
            source_systems, confidence_score, verified
        )
        SELECT
            COALESCE(t.bbl, t.bin) || '-' || t.unit_number as unit_id,
            t.bbl, t.bin, t.unit_number, t.address,
            '["PHASE2_' || t.source || '"]', 0.6, 0
        FROM text_mined_units t
        LEFT JOIN canonical_units c ON c.bin = t.bin AND c.unit_number = t.unit_number
        WHERE t.bin IS NOT NULL
        AND t.bbl IS NULL
        AND c.unit_id IS NULL
        AND t.source IN ('311_REQUESTS', 'DOB_CERTIFICATES')
    """)

    bin_added = cursor.rowcount
    print(f"Added {bin_added:,} units matched by BIN")

    conn.commit()

    total_added = bbl_added + bin_added
    print(f"\nTotal new units added to canonical_units: {total_added:,}")

    return total_added

def main():
    print("="*60)
    print("PHASE 2 DATA PROCESSING")
    print("="*60)
    print(f"Started: {datetime.now()}")
    print()

    conn = sqlite3.connect(DB_PATH)

    # Get initial counts
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM canonical_units")
    initial_canonical = cursor.fetchone()[0]
    print(f"Initial canonical units: {initial_canonical:,}")
    print()

    # Process new 311 data
    new_311_units = extract_units_from_new_311(conn)

    # Process DOB certificates
    # NOTE: Skipping certificate processing for now - creates too many placeholder units
    # We want REAL unit numbers, not numbered 1-N placeholders
    # new_cert_units = extract_units_from_certificates(conn)

    # Merge into canonical_units
    added = merge_to_canonical(conn)

    # Final stats
    cursor.execute("SELECT COUNT(*) FROM canonical_units")
    final_canonical = cursor.fetchone()[0]

    cursor.execute("SELECT SUM(unitsres) FROM pluto")
    total_nyc = cursor.fetchone()[0]

    coverage = 100.0 * final_canonical / total_nyc

    conn.close()

    print("\n" + "="*60)
    print("PHASE 2 PROCESSING COMPLETE")
    print("="*60)
    print(f"Initial canonical units: {initial_canonical:,}")
    print(f"New units added: {added:,}")
    print(f"Final canonical units: {final_canonical:,}")
    print(f"NYC Coverage: {coverage:.1f}%")
    print(f"Finished: {datetime.now()}")
    print("="*60)

if __name__ == "__main__":
    main()

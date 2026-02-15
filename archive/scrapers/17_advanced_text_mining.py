#!/usr/bin/env python3
"""
Advanced text mining with improved patterns and inference
Extract units we missed in first pass
"""

import sqlite3
import re
from collections import defaultdict

DB_PATH = "/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"

# Enhanced patterns
UNIT_PATTERNS = [
    # Standard formats (already had these)
    r'(?:APT|APARTMENT|UNIT|SUITE|#|APT\.)\s*([0-9]{1,4}[A-Z]?)',
    r'(?:APT|APARTMENT|UNIT|SUITE)\s+([A-Z]-?[0-9]+)',
    r'\b([0-9]{1,2}[A-Z])\b',
    r'\b([A-Z][0-9]{1,3})\b',
    r'\b(PH-?[A-Z0-9]+)\b',

    # NEW: Floor-based patterns
    r'(?:FLOOR|FLR|FL)\s+([0-9]{1,2})\s+(?:FRONT|REAR|F|R)',  # "FLOOR 3 REAR"
    r'(?:FLOOR|FLR|FL)\s+([0-9]{1,2}[A-Z])',  # "FLOOR 3A"
    r'([0-9]{1,2})(?:ST|ND|RD|TH)\s+(?:FLOOR|FLR)',  # "3RD FLOOR"

    # NEW: Building/apartment ranges
    r'(?:APTS?|UNITS?)\s+([0-9]+)\s*-\s*([0-9]+)',  # "APTS 1-24"
    r'(?:APTS?|UNITS?)\s+([A-Z])\s*-\s*([A-Z])',  # "APTS A-F"

    # NEW: Basement/Garden/Penthouse variations
    r'\b(BSMT|BASEMENT|GARDEN|GDN|PENTHOUSE|PH)\b',
    r'\b(STORE|STOREFRONT|COMMERCIAL)\b',

    # NEW: Simple number patterns in address context
    r'(?:,|\s)#?([0-9]{1,3}[A-Z])(?:,|\s|$)',  # ", 3A,"
    r'(?:,|\s)APT\.?\s*([0-9]+)(?:,|\s|$)',  # ", APT 12,"
]

def extract_units(text):
    """Extract all possible unit numbers from text"""
    if not text:
        return []

    units = set()
    text = text.upper()

    for pattern in UNIT_PATTERNS:
        matches = re.findall(pattern, text)
        for match in matches:
            if isinstance(match, tuple):
                # Handle range patterns
                if match[0].isdigit() and match[1].isdigit():
                    # Range like "1-24"
                    start, end = int(match[0]), int(match[1])
                    if 1 <= start < end <= 500:  # Sanity check
                        for num in range(start, min(end + 1, start + 100)):
                            units.add(str(num))
                elif match[0].isalpha() and match[1].isalpha():
                    # Range like "A-F"
                    start_ord = ord(match[0])
                    end_ord = ord(match[1])
                    if start_ord < end_ord and (end_ord - start_ord) <= 26:
                        for i in range(start_ord, end_ord + 1):
                            units.add(chr(i))
            else:
                # Single unit
                unit = match.strip()
                if unit and len(unit) <= 10:  # Sanity check
                    units.add(unit)

    return list(units)

def mine_new_sources():
    """Mine the newly downloaded datasets"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    new_units = []

    # Mine 311 data
    print("\nMining 311 complete dataset...")
    try:
        cursor.execute("SELECT bbl, borough, raw_text FROM nyc_311_complete WHERE raw_text IS NOT NULL")
        for bbl, borough, text in cursor.fetchall():
            units = extract_units(text)
            for unit in units:
                new_units.append({
                    'bbl': bbl,
                    'borough': borough,
                    'unit': unit,
                    'source': '311_COMPLETE',
                    'confidence': 0.65
                })
        print(f"  Found {len([u for u in new_units if u['source'] == '311_COMPLETE'])} units from 311")
    except Exception as e:
        print(f"  311 data not ready yet: {e}")

    # Mine DOB complaints
    print("Mining DOB complaints...")
    try:
        cursor.execute("SELECT bbl, bin, borough, unit, raw_description FROM dob_complaints")
        for bbl, bin_id, borough, unit_field, text in cursor.fetchall():
            # First check explicit unit field
            if unit_field:
                units = extract_units(unit_field)
            else:
                units = extract_units(text)

            for unit in units:
                new_units.append({
                    'bbl': bbl,
                    'bin': bin_id,
                    'borough': borough,
                    'unit': unit,
                    'source': 'DOB_COMPLAINTS',
                    'confidence': 0.7
                })
        print(f"  Found {len([u for u in new_units if u['source'] == 'DOB_COMPLAINTS'])} units from DOB complaints")
    except Exception as e:
        print(f"  DOB complaints not ready yet: {e}")

    # Mine HPD litigation
    print("Mining HPD litigation...")
    try:
        cursor.execute("SELECT bbl, raw_text FROM hpd_litigation WHERE bbl IS NOT NULL")
        for bbl, text in cursor.fetchall():
            units = extract_units(text)
            for unit in units:
                new_units.append({
                    'bbl': bbl,
                    'unit': unit,
                    'source': 'HPD_LITIGATION',
                    'confidence': 0.65
                })
        print(f"  Found {len([u for u in new_units if u['source'] == 'HPD_LITIGATION'])} units from HPD litigation")
    except Exception as e:
        print(f"  HPD litigation not ready yet: {e}")

    conn.close()
    return new_units

def insert_discovered_units(units):
    """Insert newly discovered units into text_mined_units"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    inserted = 0
    for unit_data in units:
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO text_mined_units
                (bbl, bin, unit_number, source, address, confidence_score)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                unit_data.get('bbl'),
                unit_data.get('bin'),
                unit_data['unit'],
                unit_data.get('source', 'ADVANCED_MINING'),
                '',
                unit_data.get('confidence', 0.5)
            ))
            if cursor.rowcount > 0:
                inserted += 1
        except Exception as e:
            continue

    conn.commit()
    conn.close()
    print(f"\nInserted {inserted:,} new units into text_mined_units")
    return inserted

if __name__ == "__main__":
    print("=== ADVANCED TEXT MINING ===\n")

    # Mine new data sources
    new_units = mine_new_sources()

    # Insert discovered units
    if new_units:
        insert_discovered_units(new_units)

    print("\n=== COMPLETE ===")

#!/usr/bin/env python3
"""
Mine apartment numbers from all Phase 5 data sources.
Uses enhanced regex patterns to extract maximum units.

Sources:
  - DOB Complaints (3M+ records) - complaint_number as source_id
  - NYC 311 Complete (3.3M+ records) - unique_key as source_id
  - HPD Litigation (206K records) - litigationid as source_id

Schema: text_mined_units(source, source_id, bin, bbl, unit_number, address, raw_text)
  UNIQUE(source, source_id, unit_number)
"""

import sqlite3
import re
from datetime import datetime

DB_PATH = "/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"
BATCH_SIZE = 50000

# Comprehensive unit extraction patterns
UNIT_PATTERNS = [
    # Explicit apartment mentions
    r'(?:APT|APARTMENT|UNIT|SUITE|#|APT\.)\s*([0-9]{1,4}[A-Z]?)\b',
    r'(?:APT|APARTMENT|UNIT|SUITE)\s+([A-Z]-?[0-9]+)\b',

    # Compact unit formats
    r'\b([0-9]{1,2}[A-Z])\b(?!\s*(?:ST|ND|RD|TH|STREET|AVENUE|FLOOR))',  # 3A, 12B
    r'\b([A-Z][0-9]{1,3})\b(?!\s*STREET)',  # A12, B204

    # Penthouse/special units
    r'\b(PH-?[A-Z0-9]+)\b',
    r'\b(PENTHOUSE)\b',
    r'\b(BASEMENT|BSMT|GARDEN|GDN)\b',

    # Floor-based descriptions
    r'(?:FLOOR|FLR|FL)\s+([0-9]{1,2})\s*(?:FRONT|REAR|F|R)\b',
    r'(?:FLOOR|FLR|FL)\s+([0-9]{1,2}[A-Z])\b',
    r'\b([0-9]{1,2})(?:ST|ND|RD|TH)\s+(?:FLOOR|FLR)\b',

    # Apartment ranges
    r'(?:APTS?|UNITS?)\s+([0-9]+)\s*-\s*([0-9]+)\b',  # "APTS 1-24"
    r'(?:APTS?|UNITS?)\s+([A-Z])\s*-\s*([A-Z])\b',  # "APTS A-F"

    # Context-based (in addresses)
    r'(?:,|\s)#\s*([0-9]{1,3}[A-Z]?)(?:,|\s|$)',
    r'(?:,|\s)APT\.?\s*([0-9]+[A-Z]?)(?:,|\s|$)',
]

# Pre-compile patterns
COMPILED_PATTERNS = [re.compile(p) for p in UNIT_PATTERNS]


def extract_units(text):
    """Extract apartment numbers from text. Returns list of unit strings."""
    if not text or len(text) < 3:
        return []

    text = text.upper()
    units = set()

    for pattern in COMPILED_PATTERNS:
        matches = pattern.findall(text)
        for match in matches:
            if isinstance(match, tuple) and len(match) == 2:
                # Range patterns
                if match[0].isdigit() and match[1].isdigit():
                    start, end = int(match[0]), int(match[1])
                    if 1 <= start < end <= 100:
                        for num in range(start, min(end + 1, start + 30)):
                            units.add(str(num))
                elif match[0].isalpha() and match[1].isalpha():
                    start_ord, end_ord = ord(match[0]), ord(match[1])
                    if start_ord < end_ord and (end_ord - start_ord) <= 12:
                        for i in range(start_ord, end_ord + 1):
                            units.add(chr(i))
            else:
                unit = match.strip() if isinstance(match, str) else match
                if unit and 1 <= len(unit) <= 10:
                    if unit.isdigit() and int(unit) > 9999:
                        continue
                    units.add(unit)

    return list(units)


def mine_and_insert(conn, source_name, query, text_builder, id_extractor):
    """Generic mine-and-insert for any source. Streams rows to avoid OOM."""
    print(f"\n=== Mining {source_name} ===")

    read_cursor = conn.cursor()
    write_cursor = conn.cursor()
    read_cursor.execute(query)

    count = 0
    inserted = 0
    found = 0

    while True:
        rows = read_cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break

        for row in rows:
            count += 1
            source_id, bbl, bin_val, address, text = id_extractor(row), *text_builder(row)

            units = extract_units(text)
            for unit in units:
                found += 1
                try:
                    write_cursor.execute("""
                        INSERT OR IGNORE INTO text_mined_units
                        (source, source_id, bbl, bin, unit_number, address)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (source_name, source_id, bbl, bin_val, unit, address))
                    if write_cursor.rowcount > 0:
                        inserted += 1
                except Exception:
                    pass

        conn.commit()
        print(f"  Processed {count:,} rows... ({inserted:,} new units so far)")

    print(f"  DONE: {found:,} unit mentions found, {inserted:,} new units inserted from {count:,} rows")
    return inserted


def mine_dob_complaints(conn):
    """Mine DOB complaints - has explicit unit field + raw_description."""
    query = """
        SELECT complaint_number, bin, bbl, house_number, street, unit, raw_description
        FROM dob_complaints
        WHERE (bin IS NOT NULL AND LENGTH(bin) > 0)
           OR (bbl IS NOT NULL AND LENGTH(bbl) > 0)
    """

    def text_builder(row):
        _, _, bbl, house_number, street, unit_field, raw_desc = row
        address = f"{house_number or ''} {street or ''}".strip()
        # Combine explicit unit field + description for mining
        text = f"{unit_field or ''} {house_number or ''} {raw_desc or ''}"
        bin_val = row[1]
        return bbl, bin_val, address, text

    return mine_and_insert(conn, 'DOB_COMPLAINTS', query, text_builder, lambda row: row[0])


def mine_311_complete(conn):
    """Mine complete 311 dataset - has BBL + incident_address + raw_text."""
    query = """
        SELECT unique_key, bbl, incident_address, raw_text
        FROM nyc_311_complete
        WHERE bbl IS NOT NULL AND LENGTH(bbl) > 0
    """

    def text_builder(row):
        _, bbl, address, raw_text = row
        text = f"{address or ''} {raw_text or ''}"
        return bbl, None, address, text

    return mine_and_insert(conn, '311_COMPLETE', query, text_builder, lambda row: row[0])


def mine_hpd_litigation(conn):
    """Mine HPD litigation - court cases referencing apartments."""
    query = """
        SELECT litigationid, bbl, casetype, raw_text
        FROM hpd_litigation
        WHERE bbl IS NOT NULL AND LENGTH(bbl) > 0
    """

    def text_builder(row):
        _, bbl, casetype, raw_text = row
        text = f"{casetype or ''} {raw_text or ''}"
        return bbl, None, None, text

    return mine_and_insert(conn, 'HPD_LITIGATION', query, text_builder, lambda row: row[0])


def main():
    print("=" * 60)
    print("PHASE 5 DATA MINING")
    print(f"Started: {datetime.now()}")
    print("=" * 60)

    # Get baseline count
    conn = sqlite3.connect(DB_PATH)
    baseline = conn.execute("SELECT COUNT(*) FROM text_mined_units").fetchone()[0]
    print(f"\nBaseline text_mined_units: {baseline:,}")

    total_inserted = 0
    total_inserted += mine_dob_complaints(conn)
    total_inserted += mine_311_complete(conn)
    total_inserted += mine_hpd_litigation(conn)

    final = conn.execute("SELECT COUNT(*) FROM text_mined_units").fetchone()[0]

    # Show breakdown by source
    print(f"\n{'=' * 60}")
    print("SOURCE BREAKDOWN:")
    for row in conn.execute("SELECT source, COUNT(*) FROM text_mined_units GROUP BY source ORDER BY COUNT(*) DESC"):
        print(f"  {row[0]}: {row[1]:,}")

    print(f"\n{'=' * 60}")
    print("PHASE 5 COMPLETE")
    print(f"  Before: {baseline:,} text-mined units")
    print(f"  After:  {final:,} text-mined units")
    print(f"  Added:  {total_inserted:,} new units")
    print(f"  Finished: {datetime.now()}")
    print(f"{'=' * 60}")

    conn.close()


if __name__ == "__main__":
    main()

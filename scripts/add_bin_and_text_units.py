#!/usr/bin/env python3
"""Add BIN-only and text-BBL units to all_nyc_units.db"""

import sqlite3
import time
import re

BIG_DB = "/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"
NEW_DB = "/Users/pjump/Desktop/projects/vayo/all_nyc_units.db"

BORO_MAP = {
    'MANHATTAN': '1', 'MN': '1', '1': '1',
    'BRONX': '2', 'BX': '2', '2': '2',
    'BROOKLYN': '3', 'BK': '3', '3': '3',
    'QUEENS': '4', 'QN': '4', '4': '4',
    'STATEN ISLAND': '5', 'SI': '5', '5': '5',
    'STATENISLAND': '5',
}

def normalize_text_bbl(raw_bbl):
    """Convert 'BROOKLYN0232100008' -> 10-digit numeric BBL."""
    if not raw_bbl or len(raw_bbl) < 10:
        return None
    raw = raw_bbl.upper()
    for name, code in BORO_MAP.items():
        if raw.startswith(name):
            rest = raw[len(name):]
            # rest should be block (5 digits) + lot (4-5 digits)
            if len(rest) >= 9:
                block = rest[:5]
                lot = rest[5:]
                try:
                    return int(code + block + lot.ljust(5, '0')[:5])
                except ValueError:
                    return None
            break
    return None

print("=== Step 1: Load lookups ===")
t0 = time.time()

big = sqlite3.connect(BIG_DB)

# Load PLUTO lookup
pluto_by_bbl = {}
pluto_by_block = {}  # boro_block -> row with most units
for row in big.execute("""
    SELECT CAST(bbl AS INTEGER) as bbl, borough, address, zipcode, bldgclass,
           yearbuilt, numfloors, unitsres
    FROM pluto WHERE unitsres > 0
"""):
    bbl = row[0]
    pluto_by_bbl[bbl] = row
    bbl_str = str(bbl)
    if len(bbl_str) == 10:
        bb = bbl_str[:6]
        if bb not in pluto_by_block or row[7] > pluto_by_block[bb][7]:
            pluto_by_block[bb] = row

# Load BIN -> BBL mapping from buildings table
bin_to_bbl = {}
for row in big.execute("SELECT bin, bbl FROM buildings WHERE bin IS NOT NULL AND bbl IS NOT NULL"):
    bin_val, bbl_val = row
    try:
        bbl_int = int(float(bbl_val)) if bbl_val else None
        if bbl_int and bbl_int in pluto_by_bbl:
            bin_to_bbl[bin_val] = bbl_int
    except (ValueError, TypeError):
        pass

print(f"  PLUTO: {len(pluto_by_bbl)} buildings")
print(f"  BIN->BBL: {len(bin_to_bbl)} mappings")

# Load the units we need to add
print("\n=== Step 2: Load missing units ===")

# HPD units (BIN only)
hpd_units = big.execute("""
    SELECT unit_id, bbl, bin, borough, unit_number, full_address,
           ownership_type, source_systems, confidence_score
    FROM canonical_units
    WHERE source_systems LIKE '%HPD%'
    AND (bbl IS NULL OR LENGTH(bbl) != 10 OR bbl = '0000000000')
    AND bin IS NOT NULL
""").fetchall()
print(f"  HPD BIN-only units: {len(hpd_units)}")

# Text-mined units with text BBLs
text_units = big.execute("""
    SELECT unit_id, bbl, bin, borough, unit_number, full_address,
           ownership_type, source_systems, confidence_score
    FROM canonical_units
    WHERE source_systems LIKE '%TEXT_MINED%'
    AND (bbl IS NULL OR LENGTH(bbl) != 10 OR bbl = '0000000000')
""").fetchall()
print(f"  Text-mined non-standard BBL units: {len(text_units)}")

big.close()

# Map HPD units via BIN
print("\n=== Step 3: Map units to PLUTO ===")
new_rows = []  # (unit_id, bbl, borough, address, zipcode, unit_number, source_systems, confidence, ownership, bldgclass, yearbuilt, numfloors)

hpd_matched = 0
hpd_unmatched = 0
for row in hpd_units:
    unit_id, bbl, bin_val, borough, unit_number, full_address, ownership, source, confidence = row
    matched_bbl = bin_to_bbl.get(bin_val)
    if matched_bbl and matched_bbl in pluto_by_bbl:
        p = pluto_by_bbl[matched_bbl]
        new_rows.append((
            unit_id, str(matched_bbl), p[1], full_address or p[2], p[3],
            unit_number, 0, source, confidence or 0.7, ownership,
            p[4], p[5], p[6]
        ))
        hpd_matched += 1
    else:
        hpd_unmatched += 1

print(f"  HPD: {hpd_matched} matched, {hpd_unmatched} unmatched")

# Map text-mined units via BBL normalization
text_matched = 0
text_unmatched = 0
text_bin_matched = 0
for row in text_units:
    unit_id, bbl_raw, bin_val, borough, unit_number, full_address, ownership, source, confidence = row
    matched_bbl = None

    # Try to normalize the text BBL
    if bbl_raw:
        norm_bbl = normalize_text_bbl(bbl_raw)
        if norm_bbl:
            # Direct match
            if norm_bbl in pluto_by_bbl:
                matched_bbl = norm_bbl
            else:
                # Condo lot mapping
                norm_str = str(norm_bbl)
                if len(norm_str) == 10:
                    lot = int(norm_str[6:])
                    if lot >= 1000:
                        bb = norm_str[:6]
                        if bb in pluto_by_block:
                            matched_bbl = pluto_by_block[bb][0]

    # Fallback: try BIN
    if not matched_bbl and bin_val:
        matched_bbl = bin_to_bbl.get(bin_val)

    if matched_bbl and matched_bbl in pluto_by_bbl:
        p = pluto_by_bbl[matched_bbl]
        new_rows.append((
            unit_id, str(matched_bbl), p[1], full_address or p[2], p[3],
            unit_number, 0, source, confidence or 0.6, ownership,
            p[4], p[5], p[6]
        ))
        text_matched += 1
    else:
        text_unmatched += 1

print(f"  Text: {text_matched} matched, {text_unmatched} unmatched")
print(f"  Total new rows to insert: {len(new_rows)}")

# Insert into the database
print("\n=== Step 4: Insert into all_nyc_units.db ===")
new = sqlite3.connect(NEW_DB)
new.execute("PRAGMA journal_mode=WAL")
new.execute("PRAGMA synchronous=NORMAL")

# Get current counts
before = new.execute("SELECT COUNT(*) FROM all_nyc_units").fetchone()[0]
before_real = new.execute("SELECT COUNT(*) FROM all_nyc_units WHERE is_placeholder = 0").fetchone()[0]
print(f"  Before: {before:,} total, {before_real:,} real")

# Count how many discovered per building (to adjust placeholders)
# First, track which buildings gain new real units
bldg_new_counts = {}
for r in new_rows:
    bbl = r[1]
    bldg_new_counts[bbl] = bldg_new_counts.get(bbl, 0) + 1

# Insert new real units
inserted = 0
dupes = 0
batch = []
for r in new_rows:
    batch.append(r)
    if len(batch) >= 50000:
        cursor = new.executemany("""
            INSERT OR IGNORE INTO all_nyc_units
            (unit_id, bbl, borough, address, zipcode, unit_number,
             is_placeholder, source_systems, confidence_score, ownership_type,
             bldgclass, yearbuilt, numfloors)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, batch)
        inserted += cursor.rowcount
        new.commit()
        batch = []

if batch:
    cursor = new.executemany("""
        INSERT OR IGNORE INTO all_nyc_units
        (unit_id, bbl, borough, address, zipcode, unit_number,
         is_placeholder, source_systems, confidence_score, ownership_type,
         bldgclass, yearbuilt, numfloors)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, batch)
    inserted += cursor.rowcount
    new.commit()

print(f"  Inserted: {inserted:,} new real units")

# Now remove excess placeholders for buildings that gained real units
# For each building, if we added N real units, remove up to N placeholders
print("\n=== Step 5: Remove excess placeholders ===")
removed = 0
for bbl, count in bldg_new_counts.items():
    # Find placeholder IDs to remove
    phs = new.execute("""
        SELECT unit_id FROM all_nyc_units
        WHERE bbl = ? AND is_placeholder = 1
        ORDER BY unit_id DESC
        LIMIT ?
    """, (bbl, count)).fetchall()

    if phs:
        new.executemany("DELETE FROM all_nyc_units WHERE unit_id = ?", phs)
        removed += len(phs)

new.commit()
print(f"  Removed {removed:,} excess placeholders")

# Final report
print("\n" + "=" * 52)
print("  UPDATED COVERAGE REPORT")
print("=" * 52)

total = new.execute("SELECT COUNT(*) FROM all_nyc_units").fetchone()[0]
real = new.execute("SELECT COUNT(*) FROM all_nyc_units WHERE is_placeholder = 0").fetchone()[0]
ph = new.execute("SELECT COUNT(*) FROM all_nyc_units WHERE is_placeholder = 1").fetchone()[0]
bldgs = new.execute("SELECT COUNT(DISTINCT bbl) FROM all_nyc_units").fetchone()[0]

print(f"\nTotal units: {total:,}")
print(f"Real (discovered): {real:,} ({100*real/total:.1f}%)")
print(f"Placeholder: {ph:,}")
print(f"Buildings: {bldgs:,}")

print("\n--- BY BOROUGH ---")
for row in new.execute("""
    SELECT borough, COUNT(*) as total,
           SUM(CASE WHEN is_placeholder = 0 THEN 1 ELSE 0 END) as real,
           ROUND(100.0 * SUM(CASE WHEN is_placeholder = 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as pct
    FROM all_nyc_units GROUP BY borough ORDER BY total DESC
"""):
    print(f"  {row[0]:4s}  {row[1]:>10,} total  {row[2]:>10,} real  {row[3]}%")

print("\n--- BY SOURCE ---")
for row in new.execute("""
    SELECT CASE
        WHEN source_systems LIKE '%ACRIS%' THEN 'ACRIS'
        WHEN source_systems LIKE '%HPD%' THEN 'HPD'
        WHEN source_systems LIKE '%TEXT_MINED%' THEN 'Text Mining'
        WHEN source_systems LIKE '%PLUTO%' THEN 'PLUTO Inferred'
        ELSE 'Other'
    END, COUNT(*) FROM all_nyc_units GROUP BY 1 ORDER BY 2 DESC
"""):
    print(f"  {row[0]:20s} {row[1]:>10,}")

new.close()
print(f"\nTotal time: {time.time()-t0:.1f}s")

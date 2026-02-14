#!/usr/bin/env python3
"""Add BIN-only and text-BBL units to all_nyc_units.db using CSV exports."""

import sqlite3
import csv
import time

NEW_DB = "/Users/pjump/Desktop/projects/vayo/all_nyc_units.db"
HPD_CSV = "/Users/pjump/Desktop/projects/vayo/nycdb_data/hpd_units.csv"
TEXT_CSV = "/Users/pjump/Desktop/projects/vayo/nycdb_data/text_units_nomatch.csv"
BIN_CSV = "/Users/pjump/Desktop/projects/vayo/nycdb_data/bin_to_bbl.csv"

BORO_MAP = {
    'MANHATTAN': '1', 'BRONX': '2', 'BROOKLYN': '3',
    'QUEENS': '4', 'STATEN ISLAND': '5', 'STATENISLAND': '5',
}

def normalize_text_bbl(raw):
    if not raw or len(raw) < 10:
        return None
    upper = raw.upper()
    for name, code in BORO_MAP.items():
        if upper.startswith(name):
            rest = upper[len(name):]
            if len(rest) >= 9:
                block = rest[:5]
                lot = rest[5:].ljust(5, '0')[:5]
                try:
                    return int(code + block + lot)
                except ValueError:
                    return None
    return None

t0 = time.time()

# Load PLUTO from the small DB
print("Loading PLUTO from all_nyc_units.db...")
new = sqlite3.connect(NEW_DB)
# We need PLUTO data but it was cleaned up. Get building info from existing units.
# Actually, let's get distinct building data from the existing table
bldg_info = {}
for row in new.execute("""
    SELECT DISTINCT bbl, borough, address, zipcode, bldgclass, yearbuilt, numfloors
    FROM all_nyc_units WHERE bbl IS NOT NULL
"""):
    bldg_info[row[0]] = row
print(f"  {len(bldg_info)} buildings in all_nyc_units")

# Also load PLUTO directly from big DB via the already-exported data
# Actually we already have building info from the existing table. Let's also
# build a set of valid PLUTO BBLs
valid_bbls = set(bldg_info.keys())

# Also build boro_block -> best BBL mapping
pluto_by_block = {}
# We need unit counts - get from existing data
bbl_unit_counts = {}
for row in new.execute("SELECT bbl, COUNT(*) FROM all_nyc_units GROUP BY bbl"):
    bbl_unit_counts[row[0]] = row[1]
    bbl_str = row[0]
    if bbl_str and len(bbl_str) == 10:
        bb = bbl_str[:6]
        if bb not in pluto_by_block or row[1] > bbl_unit_counts.get(pluto_by_block[bb], 0):
            pluto_by_block[bb] = bbl_str

print(f"  {len(pluto_by_block)} boro+block groups")

# Load BIN -> BBL mapping
print("\nLoading BIN->BBL mapping...")
bin_to_bbl = {}
with open(BIN_CSV) as f:
    reader = csv.DictReader(f)
    for row in reader:
        bin_val = row['bin']
        bbl_val = row['bbl']
        try:
            bbl_int = str(int(float(bbl_val)))
            if bbl_int in valid_bbls:
                bin_to_bbl[bin_val] = bbl_int
        except (ValueError, TypeError):
            pass
print(f"  {len(bin_to_bbl)} BIN->BBL mappings (matching PLUTO)")

# Process HPD units
print("\nProcessing HPD units...")
hpd_rows = []
hpd_matched = 0
hpd_unmatched = 0

with open(HPD_CSV) as f:
    reader = csv.DictReader(f)
    for row in reader:
        bin_val = row['bin']
        matched_bbl = bin_to_bbl.get(bin_val)
        if matched_bbl and matched_bbl in bldg_info:
            b = bldg_info[matched_bbl]
            hpd_rows.append((
                row['unit_id'], matched_bbl, b[1], row['full_address'] or b[2],
                b[3], row['unit_number'], 0, row['source_systems'],
                float(row['confidence_score'] or 0.7), row['ownership_type'],
                b[4], b[5], b[6]
            ))
            hpd_matched += 1
        else:
            hpd_unmatched += 1

print(f"  HPD: {hpd_matched:,} matched, {hpd_unmatched:,} unmatched")

# Process text-mined units
print("\nProcessing text-mined units...")
text_rows = []
text_matched = 0
text_unmatched = 0

with open(TEXT_CSV) as f:
    reader = csv.DictReader(f)
    for row in reader:
        matched_bbl = None
        bbl_raw = row['bbl']

        # Try normalizing the text BBL
        if bbl_raw:
            norm = normalize_text_bbl(bbl_raw)
            if norm:
                norm_str = str(norm)
                if norm_str in valid_bbls:
                    matched_bbl = norm_str
                elif len(norm_str) == 10:
                    lot = int(norm_str[6:])
                    if lot >= 1000:
                        bb = norm_str[:6]
                        if bb in pluto_by_block:
                            matched_bbl = pluto_by_block[bb]

        # Fallback: BIN
        if not matched_bbl and row['bin']:
            matched_bbl = bin_to_bbl.get(row['bin'])

        if matched_bbl and matched_bbl in bldg_info:
            b = bldg_info[matched_bbl]
            text_rows.append((
                row['unit_id'], matched_bbl, b[1], row['full_address'] or b[2],
                b[3], row['unit_number'], 0, row['source_systems'],
                float(row['confidence_score'] or 0.6), row['ownership_type'],
                b[4], b[5], b[6]
            ))
            text_matched += 1
        else:
            text_unmatched += 1

print(f"  Text: {text_matched:,} matched, {text_unmatched:,} unmatched")

# Insert into DB
print(f"\nInserting {len(hpd_rows) + len(text_rows):,} new units...")
all_new = hpd_rows + text_rows

before_real = new.execute("SELECT COUNT(*) FROM all_nyc_units WHERE is_placeholder = 0").fetchone()[0]

batch_size = 50000
inserted = 0
for i in range(0, len(all_new), batch_size):
    batch = all_new[i:i+batch_size]
    cur = new.executemany("""
        INSERT OR IGNORE INTO all_nyc_units
        (unit_id, bbl, borough, address, zipcode, unit_number,
         is_placeholder, source_systems, confidence_score, ownership_type,
         bldgclass, yearbuilt, numfloors)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, batch)
    inserted += cur.rowcount
    new.commit()
    print(f"  ... {min(i+batch_size, len(all_new)):,} processed, {inserted:,} inserted")

# Remove excess placeholders
print("\nRemoving excess placeholders...")
bldg_new_counts = {}
for r in all_new:
    bbl = r[1]
    bldg_new_counts[bbl] = bldg_new_counts.get(bbl, 0) + 1

removed = 0
for bbl, count in bldg_new_counts.items():
    phs = new.execute("""
        SELECT unit_id FROM all_nyc_units
        WHERE bbl = ? AND is_placeholder = 1
        ORDER BY unit_id DESC LIMIT ?
    """, (bbl, count)).fetchall()
    if phs:
        new.executemany("DELETE FROM all_nyc_units WHERE unit_id = ?", phs)
        removed += len(phs)

new.commit()
print(f"  Removed {removed:,} placeholders")

# Final report
print("\n" + "=" * 52)
print("  UPDATED COVERAGE REPORT")
print("=" * 52)

total = new.execute("SELECT COUNT(*) FROM all_nyc_units").fetchone()[0]
real = new.execute("SELECT COUNT(*) FROM all_nyc_units WHERE is_placeholder = 0").fetchone()[0]
ph = total - real
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

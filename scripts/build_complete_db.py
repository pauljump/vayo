#!/usr/bin/env python3
"""Build complete NYC units database from PLUTO + discovered units."""

import sqlite3
import time
import os

BIG_DB = "/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"
NEW_DB = "/Users/pjump/Desktop/projects/vayo/all_nyc_units.db"

# Remove old DB
if os.path.exists(NEW_DB):
    os.remove(NEW_DB)

print("=== Step 1: Export from big DB ===")
t0 = time.time()

big = sqlite3.connect(BIG_DB)
big.execute("PRAGMA journal_mode=WAL")

# Export PLUTO
pluto_rows = big.execute("""
    SELECT CAST(bbl AS INTEGER), borough, address, zipcode, bldgclass,
           yearbuilt, numfloors, unitsres, unitstotal, ownername, zonedist1,
           assesstot, lotarea, bldgarea, resarea
    FROM pluto WHERE unitsres > 0
""").fetchall()
print(f"  PLUTO: {len(pluto_rows)} buildings")

# Export canonical_units (BBL-matched only)
cu_rows = big.execute("""
    SELECT unit_id, bbl, bin, borough, unit_number, full_address,
           ownership_type, source_systems, confidence_score
    FROM canonical_units
    WHERE bbl IS NOT NULL AND LENGTH(bbl) = 10 AND bbl != '0000000000'
""").fetchall()
print(f"  Canonical units: {len(cu_rows)} units")
big.close()
print(f"  Export took {time.time()-t0:.1f}s")

# Build PLUTO lookup
print("\n=== Step 2: Build lookups ===")
t1 = time.time()

# BBL -> pluto row
pluto_by_bbl = {}
# boro_block -> best pluto BBL (most units)
pluto_by_block = {}

for row in pluto_rows:
    bbl = row[0]
    unitsres = row[7]
    pluto_by_bbl[bbl] = row

    bbl_str = str(bbl)
    if len(bbl_str) == 10:
        boro_block = bbl_str[:6]
        if boro_block not in pluto_by_block or unitsres > pluto_by_block[boro_block][7]:
            pluto_by_block[boro_block] = row

print(f"  {len(pluto_by_bbl)} BBL lookups, {len(pluto_by_block)} boro+block lookups")

# Map canonical units to PLUTO buildings
mapped = []  # (unit_id, matched_bbl, unit_number, full_address, ownership_type, source_systems, confidence)
unmatched = 0

for row in cu_rows:
    unit_id, bbl_str, bin_val, borough, unit_number, full_address, ownership_type, source_systems, confidence = row
    bbl_int = int(bbl_str) if bbl_str else None
    matched_bbl = None

    # Direct match
    if bbl_int in pluto_by_bbl:
        matched_bbl = bbl_int
    elif len(bbl_str) == 10:
        # Condo lot mapping
        lot = int(bbl_str[6:10])
        if lot >= 1000:
            boro_block = bbl_str[:6]
            if boro_block in pluto_by_block:
                matched_bbl = pluto_by_block[boro_block][0]

    if matched_bbl is not None:
        mapped.append((unit_id, matched_bbl, unit_number, full_address, ownership_type, source_systems, confidence))
    else:
        unmatched += 1

print(f"  Matched: {len(mapped)}, Unmatched: {unmatched}")
print(f"  Mapping took {time.time()-t1:.1f}s")

# Count discovered per building
print("\n=== Step 3: Gap analysis ===")
bldg_discovered = {}
for m in mapped:
    bbl = m[1]
    bldg_discovered[bbl] = bldg_discovered.get(bbl, 0) + 1

fully_covered = 0
partial = 0
zero_coverage = 0
total_gap = 0

gaps = []  # (bbl, pluto_row, discovered, gap)
for bbl, prow in pluto_by_bbl.items():
    expected = prow[7]  # unitsres
    disc = bldg_discovered.get(bbl, 0)
    gap = max(0, expected - disc)

    if gap == 0:
        fully_covered += 1
    elif disc > 0:
        partial += 1
        gaps.append((bbl, prow, disc, gap))
    else:
        zero_coverage += 1
        gaps.append((bbl, prow, disc, gap))
    total_gap += gap

print(f"  Fully covered: {fully_covered}")
print(f"  Partially covered: {partial}")
print(f"  Zero coverage: {zero_coverage}")
print(f"  Placeholders needed: {total_gap}")

# Build the new database
print("\n=== Step 4: Build all_nyc_units database ===")
t2 = time.time()

new = sqlite3.connect(NEW_DB)
new.execute("PRAGMA journal_mode=WAL")
new.execute("PRAGMA synchronous=NORMAL")
new.execute("PRAGMA cache_size=-200000")  # 200MB cache

new.execute("""
    CREATE TABLE all_nyc_units (
        unit_id TEXT PRIMARY KEY,
        bbl TEXT NOT NULL,
        borough TEXT,
        address TEXT,
        zipcode TEXT,
        unit_number TEXT,
        is_placeholder BOOLEAN DEFAULT 0,
        source_systems TEXT,
        confidence_score REAL DEFAULT 0.5,
        ownership_type TEXT,
        bldgclass TEXT,
        yearbuilt INTEGER,
        numfloors REAL
    )
""")

# Insert discovered units
print("  Inserting discovered units...")
disc_rows = []
for m in mapped:
    unit_id, matched_bbl, unit_number, full_address, ownership_type, source_systems, confidence = m
    prow = pluto_by_bbl[matched_bbl]
    # prow: bbl, borough, address, zipcode, bldgclass, yearbuilt, numfloors, ...
    disc_rows.append((
        unit_id, str(matched_bbl), prow[1], full_address or prow[2],
        prow[3], unit_number, 0, source_systems, confidence, ownership_type,
        prow[4], prow[5], prow[6]
    ))

new.executemany("""
    INSERT OR IGNORE INTO all_nyc_units VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
""", disc_rows)
new.commit()
print(f"  Inserted {len(disc_rows)} discovered units")

# Insert placeholders
print("  Generating placeholders...")
batch = []
batch_size = 50000
total_ph = 0

for bbl, prow, disc, gap in gaps:
    bbl_str = str(bbl)
    for n in range(1, gap + 1):
        unit_seq = disc + n
        unit_id = f"{bbl_str}-PH-{unit_seq:05d}"
        unit_number = f"UNIT_{unit_seq:05d}"
        batch.append((
            unit_id, bbl_str, prow[1], prow[2], prow[3],
            unit_number, 1, '["PLUTO_INFERRED"]', 0.3, None,
            prow[4], prow[5], prow[6]
        ))
        total_ph += 1

        if len(batch) >= batch_size:
            new.executemany("INSERT INTO all_nyc_units VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", batch)
            new.commit()
            print(f"    ... {total_ph:,} placeholders")
            batch = []

if batch:
    new.executemany("INSERT INTO all_nyc_units VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", batch)
    new.commit()

print(f"  Total placeholders: {total_ph:,}")
print(f"  Data insert took {time.time()-t2:.1f}s")

# Create indexes
print("\n=== Step 5: Indexes ===")
t3 = time.time()
new.execute("CREATE INDEX idx_bbl ON all_nyc_units(bbl)")
new.execute("CREATE INDEX idx_borough ON all_nyc_units(borough)")
new.execute("CREATE INDEX idx_zip ON all_nyc_units(zipcode)")
new.execute("CREATE INDEX idx_ph ON all_nyc_units(is_placeholder)")
new.execute("CREATE INDEX idx_bldgclass ON all_nyc_units(bldgclass)")
new.commit()
print(f"  Indexes took {time.time()-t3:.1f}s")

# Final report
print("\n" + "=" * 52)
print("  ALL NYC UNITS - COMPLETE COVERAGE")
print("=" * 52)

total = new.execute("SELECT COUNT(*) FROM all_nyc_units").fetchone()[0]
real = new.execute("SELECT COUNT(*) FROM all_nyc_units WHERE is_placeholder = 0").fetchone()[0]
ph = new.execute("SELECT COUNT(*) FROM all_nyc_units WHERE is_placeholder = 1").fetchone()[0]
bldgs = new.execute("SELECT COUNT(DISTINCT bbl) FROM all_nyc_units").fetchone()[0]

print(f"\nTotal units: {total:,}")
print(f"Real (discovered): {real:,}")
print(f"Placeholder: {ph:,}")
print(f"Unique buildings: {bldgs:,}")

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

# File size
size_mb = os.path.getsize(NEW_DB) / (1024 * 1024)
print(f"\nDatabase file: {size_mb:.1f} MB")
print(f"Total time: {time.time()-t0:.1f}s")

#!/usr/bin/env python3
"""
Build complete NYC units database from PLUTO + all discovered unit sources.

Sources:
1. canonical_units from stuytown.db (HPD, ECB text mining, 311 text mining, etc.)
2. ACRIS transactions from vayo_clean.db (full 22.5M legals with unit data)

Run AFTER build_clean_db.py (needs vayo_clean.db to exist).
"""

import sqlite3
import json
import time
import os

BIG_DB = "/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"
CLEAN_DB = "/Users/pjump/Desktop/projects/vayo/vayo_clean.db"
NEW_DB = "/Users/pjump/Desktop/projects/vayo/all_nyc_units.db"

JUNK_UNITS = {'N/A', 'NA', '0', '00', '000', '-', '.', 'NONE', 'X', 'XX',
              'TIMES', 'APT', 'UNIT', 'BLDG', 'BUILDING', 'ALL', 'ENTIRE',
              'BSMT', 'BASEMENT', 'CELLAR', 'STORE', 'GARAGE', 'PARKING',
              'ROOF', 'LOBBY', 'HALLWAY', 'COMMON', 'COMMERCIAL', 'OFFICE',
              'SUPER', 'BOILER', 'PUBLIC', 'VACANT', 'OUTSIDE', 'HALL',
              'STAIRS', 'STAIRWAY'}

# Remove old DB
if os.path.exists(NEW_DB):
    os.remove(NEW_DB)

t0 = time.time()

# ============================================================================
# Step 1: Load PLUTO from vayo_clean.db
# ============================================================================
print("=== Step 1: Load PLUTO buildings ===")
clean = sqlite3.connect(CLEAN_DB)

pluto_by_bbl = {}
pluto_by_block = {}

for row in clean.execute("""
    SELECT bbl, borough, address, zipcode, bldgclass,
           yearbuilt, numfloors, unitsres
    FROM buildings WHERE unitsres > 0
"""):
    bbl, borough, address, zipcode, bldgclass, yearbuilt, numfloors, unitsres = row
    pluto_by_bbl[bbl] = row

    bbl_str = str(bbl)
    if len(bbl_str) == 10:
        boro_block = bbl_str[:6]
        if boro_block not in pluto_by_block or unitsres > pluto_by_block[boro_block][7]:
            pluto_by_block[boro_block] = row

print(f"  {len(pluto_by_bbl):,} buildings, {len(pluto_by_block):,} boro+block lookups")

# ============================================================================
# Step 2: Collect units from all sources
# ============================================================================
# Store as: {(bbl_str, unit_number): (source_systems, confidence, ownership_type, address)}
discovered = {}

def add_unit(bbl_int, unit_number, source, confidence=0.8, ownership_type=None, address=None):
    """Add a discovered unit, deduplicating by bbl+unit."""
    if not unit_number or unit_number.upper().strip() in JUNK_UNITS:
        return
    unit_number = unit_number.strip()
    if len(unit_number) > 10:
        return
    bbl_str = str(bbl_int)
    key = (bbl_str, unit_number)
    if key in discovered:
        # Merge sources
        existing = discovered[key]
        sources = json.loads(existing[0])
        if source not in sources:
            sources.append(source)
        # Keep highest confidence
        discovered[key] = (json.dumps(sources), max(existing[1], confidence),
                          existing[2] or ownership_type, existing[3] or address)
    else:
        discovered[key] = (json.dumps([source]), confidence, ownership_type, address)

# --- Source A: canonical_units from stuytown.db ---
print("\n=== Step 2a: canonical_units from stuytown.db ===")
big = sqlite3.connect(BIG_DB)
cu_count = 0
for row in big.execute("""
    SELECT bbl, unit_number, source_systems, confidence_score, ownership_type, full_address
    FROM canonical_units
    WHERE bbl IS NOT NULL AND LENGTH(bbl) = 10 AND bbl != '0000000000'
    AND unit_number IS NOT NULL AND unit_number != ''
"""):
    bbl_str = row[0]
    bbl_int = int(bbl_str)

    # Map condo lots to building BBL
    matched_bbl = None
    if bbl_int in pluto_by_bbl:
        matched_bbl = bbl_int
    else:
        lot = int(bbl_str[6:10])
        if lot >= 1000:
            boro_block = bbl_str[:6]
            if boro_block in pluto_by_block:
                matched_bbl = pluto_by_block[boro_block][0]

    if matched_bbl is not None:
        # Parse existing sources and add each
        try:
            sources = json.loads(row[2]) if row[2] else ['UNKNOWN']
        except json.JSONDecodeError:
            sources = [row[2]]
        for src in sources:
            add_unit(matched_bbl, row[1], src, row[3] or 0.8, row[4], row[5])
        cu_count += 1

big.close()
print(f"  {cu_count:,} canonical units mapped")

# --- Source B: ACRIS transactions from vayo_clean.db ---
print("\n=== Step 2b: ACRIS units from vayo_clean.db ===")
acris_count = 0
for row in clean.execute("""
    SELECT bbl, unit FROM acris_transactions
    WHERE unit IS NOT NULL AND unit != ''
"""):
    add_unit(row[0], row[1], 'ACRIS_FULL', 0.9)
    acris_count += 1

print(f"  {acris_count:,} ACRIS transaction rows with units")

# --- Source C: HPD complaints from vayo_clean.db ---
print("\n=== Step 2c: HPD complaint units from vayo_clean.db ===")
hpd_count = 0
for row in clean.execute("""
    SELECT DISTINCT bbl, unit FROM complaints
    WHERE unit IS NOT NULL AND unit != ''
"""):
    add_unit(row[0], row[1], 'HPD', 0.85)
    hpd_count += 1

print(f"  {hpd_count:,} distinct HPD bbl+unit combos")

clean.close()
print(f"\n  Total discovered units: {len(discovered):,}")

# ============================================================================
# Step 3: Gap analysis
# ============================================================================
print("\n=== Step 3: Gap analysis ===")
bldg_discovered = {}
for (bbl_str, unit_num) in discovered:
    bbl_int = int(bbl_str)
    bldg_discovered[bbl_int] = bldg_discovered.get(bbl_int, 0) + 1

fully_covered = 0
partial = 0
zero_coverage = 0
total_gap = 0
gaps = []

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

print(f"  Fully covered: {fully_covered:,}")
print(f"  Partially covered: {partial:,}")
print(f"  Zero coverage: {zero_coverage:,}")
print(f"  Placeholders needed: {total_gap:,}")

# ============================================================================
# Step 4: Build the database
# ============================================================================
print("\n=== Step 4: Build all_nyc_units database ===")
t2 = time.time()

new = sqlite3.connect(NEW_DB)
new.execute("PRAGMA journal_mode=WAL")
new.execute("PRAGMA synchronous=NORMAL")
new.execute("PRAGMA cache_size=-200000")

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
batch = []
for (bbl_str, unit_number), (sources, confidence, ownership, address) in discovered.items():
    bbl_int = int(bbl_str)
    prow = pluto_by_bbl.get(bbl_int)
    if not prow:
        continue
    unit_id = f"{bbl_str}-{unit_number}"
    batch.append((
        unit_id, bbl_str, prow[1], address or prow[2],
        prow[3], unit_number, 0, sources, confidence, ownership,
        prow[4], prow[5], prow[6]
    ))
    if len(batch) >= 50000:
        new.executemany("INSERT OR IGNORE INTO all_nyc_units VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", batch)
        new.commit()
        batch = []

if batch:
    new.executemany("INSERT OR IGNORE INTO all_nyc_units VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", batch)
    new.commit()

disc_inserted = new.execute("SELECT COUNT(*) FROM all_nyc_units").fetchone()[0]
print(f"  Inserted {disc_inserted:,} discovered units")

# Insert placeholders
print("  Generating placeholders...")
batch = []
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

        if len(batch) >= 50000:
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
        WHEN source_systems LIKE '%ACRIS_FULL%' THEN 'ACRIS (full cache)'
        WHEN source_systems LIKE '%ACRIS%' THEN 'ACRIS (legacy)'
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

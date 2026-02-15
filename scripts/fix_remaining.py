#!/usr/bin/env python3
"""Fix DOB complaints, DOB permits, and marshal evictions in vayo_clean.db"""

import sqlite3
import time
from collections import defaultdict

BIG_DB = "/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"
OUT_DB = "/Users/pjump/Desktop/projects/vayo/vayo_clean.db"

big = sqlite3.connect(BIG_DB)
out = sqlite3.connect(OUT_DB)
out.execute("PRAGMA journal_mode=WAL")
out.execute("PRAGMA synchronous=NORMAL")

t0 = time.time()

# Load BIN→BBL from clean db
bin_to_bbl = {}
for row in out.execute("SELECT bin, bbl FROM bin_map"):
    bin_to_bbl[row[0]] = row[1]
print(f"Loaded {len(bin_to_bbl)} BIN→BBL mappings")

valid_bbls = set()
for row in out.execute("SELECT bbl FROM buildings"):
    valid_bbls.add(row[0])

# ============================================================================
# DOB Complaints (have BIN, need to match via bin_to_bbl)
# ============================================================================
print("\n=== DOB Complaints ===")
out.execute("DROP TABLE IF EXISTS dob_complaints")
out.execute("""
    CREATE TABLE dob_complaints (
        bbl INTEGER, unit TEXT, complaint_category TEXT,
        disposition_code TEXT, disposition_date TEXT, raw_description TEXT
    )
""")

batch = []
count = 0
for row in big.execute("""
    SELECT bin, unit, complaint_category, disposition_code,
           disposition_date, raw_description
    FROM dob_complaints WHERE bin IS NOT NULL
"""):
    bbl = bin_to_bbl.get(row[0])
    if bbl:
        batch.append((bbl, row[1], row[2], row[3], row[4], row[5]))
        if len(batch) >= 50000:
            out.executemany("INSERT INTO dob_complaints VALUES (?,?,?,?,?,?)", batch)
            out.commit()
            count += len(batch)
            batch = []

if batch:
    out.executemany("INSERT INTO dob_complaints VALUES (?,?,?,?,?,?)", batch)
    out.commit()
    count += len(batch)

out.execute("CREATE INDEX idx_dobc_bbl ON dob_complaints(bbl)")
out.commit()
print(f"  {count:,} DOB complaints matched")

# ============================================================================
# DOB Permits (have boro+block+lot, construct BBL)
# ============================================================================
print("\n=== DOB Permits ===")
out.execute("DROP TABLE IF EXISTS dob_permits")
out.execute("""
    CREATE TABLE dob_permits (
        bbl INTEGER, job_type TEXT, job_description TEXT,
        job_status_description TEXT, latest_action_date TEXT,
        initial_cost TEXT, existing_dwelling_units TEXT,
        proposed_dwelling_units TEXT
    )
""")

# DOB permits have borough, block, lot as separate fields
batch = []
count = 0

# Map borough name to code
boro_map = {'MANHATTAN': 1, 'BRONX': 2, 'BROOKLYN': 3, 'QUEENS': 4, 'STATEN ISLAND': 5}

for row in big.execute("""
    SELECT borough, block, lot, job_type, job_description,
           job_status_description, latest_action_date, initial_cost,
           existing_dwelling_units, proposed_dwelling_units
    FROM dob_permits
    WHERE block IS NOT NULL AND lot IS NOT NULL
"""):
    try:
        boro_code = boro_map.get(row[0], row[0])
        if isinstance(boro_code, str):
            boro_code = int(boro_code) if boro_code.isdigit() else None
        if boro_code is None:
            continue
        block = str(int(row[1])).zfill(5)
        lot = str(int(row[2])).zfill(5)
        bbl = int(str(boro_code) + block + lot)
    except (ValueError, TypeError):
        continue

    if bbl not in valid_bbls:
        continue

    batch.append((bbl, row[3], row[4], row[5], row[6], row[7], row[8], row[9]))
    if len(batch) >= 50000:
        out.executemany("INSERT INTO dob_permits VALUES (?,?,?,?,?,?,?,?)", batch)
        out.commit()
        count += len(batch)
        batch = []

if batch:
    out.executemany("INSERT INTO dob_permits VALUES (?,?,?,?,?,?,?,?)", batch)
    out.commit()
    count += len(batch)

out.execute("CREATE INDEX idx_perm_bbl ON dob_permits(bbl)")
out.commit()
print(f"  {count:,} DOB permits matched")

# ============================================================================
# Marshal Evictions (address-match to BBL via PLUTO)
# ============================================================================
print("\n=== Marshal Evictions ===")
out.execute("DROP TABLE IF EXISTS marshal_evictions")
out.execute("""
    CREATE TABLE marshal_evictions (
        bbl INTEGER, eviction_address TEXT, apartment TEXT,
        executed_date TEXT, borough TEXT, zipcode TEXT, neighborhood TEXT
    )
""")

# Build address+zip → BBL lookup from PLUTO
addr_to_bbl = {}
for row in out.execute("SELECT bbl, address, zipcode FROM buildings WHERE address IS NOT NULL"):
    key = (row[1].upper().strip(), row[2])
    addr_to_bbl[key] = row[0]
    # Also store without zip for looser matching
    addr_to_bbl[row[1].upper().strip()] = row[0]

print(f"  {len(addr_to_bbl)} address lookups built")

batch = []
count = 0
unmatched = 0
for row in big.execute("""
    SELECT eviction_address, eviction_apartment, executed_date,
           borough, eviction_zip, neighborhood
    FROM marshal_evictions
"""):
    addr = (row[0] or '').upper().strip()
    zipcode = (row[4] or '').strip()

    # Try exact address+zip match
    bbl = addr_to_bbl.get((addr, zipcode))
    if not bbl:
        # Try address only
        bbl = addr_to_bbl.get(addr)

    if bbl:
        batch.append((bbl, row[0], row[1], row[2], row[3], row[4], row[5]))
        count += 1
    else:
        unmatched += 1

    if len(batch) >= 50000:
        out.executemany("INSERT INTO marshal_evictions VALUES (?,?,?,?,?,?,?)", batch)
        out.commit()
        batch = []

if batch:
    out.executemany("INSERT INTO marshal_evictions VALUES (?,?,?,?,?,?,?)", batch)
    out.commit()

out.execute("CREATE INDEX idx_evict_bbl ON marshal_evictions(bbl)")
out.commit()
print(f"  {count:,} evictions matched, {unmatched:,} unmatched")

# Final counts
print("\n=== UPDATED COUNTS ===")
for table in ['dob_complaints', 'dob_permits', 'marshal_evictions']:
    c = out.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"  {table:<25} {c:>12,}")

import os
size_mb = os.path.getsize(OUT_DB) / (1024 * 1024)
print(f"\n  Database size: {size_mb:.0f} MB")
print(f"  Time: {time.time()-t0:.1f}s")

big.close()
out.close()

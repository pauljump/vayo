#!/usr/bin/env python3
"""Pull rgb_stabilized_buildings, service_requests_311, and certificates_of_occupancy into vayo_clean.db"""

import sqlite3
import time
import os

BIG_DB = "/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"
OUT_DB = "/Users/pjump/Desktop/projects/vayo/vayo_clean.db"

big = sqlite3.connect(BIG_DB)
out = sqlite3.connect(OUT_DB)
out.execute("PRAGMA journal_mode=WAL")
out.execute("PRAGMA synchronous=NORMAL")
out.execute("PRAGMA cache_size=-500000")

t0 = time.time()

# Load lookups
valid_bbls = set(r[0] for r in out.execute("SELECT bbl FROM buildings"))
bin_to_bbl = dict(out.execute("SELECT bin, bbl FROM bin_map").fetchall())

# Build boro_block -> best BBL for condo mapping
block_best = {}
for row in out.execute("SELECT bbl, unitsres FROM buildings"):
    s = str(row[0])
    if len(s) == 10:
        bb = s[:6]
        if bb not in block_best or row[1] > block_best[bb][1]:
            block_best[bb] = (row[0], row[1])

print(f"Lookups: {len(valid_bbls)} BBLs, {len(bin_to_bbl)} BINs, {len(block_best)} blocks")

# ============================================================================
# 1. RGB Stabilized Buildings
# ============================================================================
print("\n=== 1. Rent Stabilized Buildings ===")
out.execute("""
    CREATE TABLE IF NOT EXISTS rent_stabilized (
        bbl INTEGER, address TEXT, borough TEXT, zipcode TEXT,
        num_stabilized_units INTEGER, list_year INTEGER,
        has_421a BOOLEAN, has_j51 BOOLEAN, is_coop_condo BOOLEAN
    )
""")

batch = []
matched = 0
unmatched = 0
for row in big.execute("""
    SELECT bbl, bin, address, borough, zip_code, num_stabilized_units,
           list_year, has_421a, has_j51, is_coop_condo
    FROM rgb_stabilized_buildings
"""):
    raw_bbl, bin_val, address, borough, zipcode, stab_units, year, a421, j51, coop = row
    matched_bbl = None

    # Try direct BBL
    if raw_bbl:
        try:
            bbl_int = int(raw_bbl)
            if bbl_int in valid_bbls:
                matched_bbl = bbl_int
        except (ValueError, TypeError):
            pass

    # Try BIN
    if not matched_bbl and bin_val:
        matched_bbl = bin_to_bbl.get(str(bin_val))

    if matched_bbl:
        batch.append((matched_bbl, address, borough, zipcode, stab_units, year, a421, j51, coop))
        matched += 1
    else:
        unmatched += 1

out.executemany("INSERT INTO rent_stabilized VALUES (?,?,?,?,?,?,?,?,?)", batch)
out.execute("CREATE INDEX IF NOT EXISTS idx_rs_bbl ON rent_stabilized(bbl)")
out.commit()
print(f"  {matched:,} matched, {unmatched:,} unmatched")

# ============================================================================
# 2. Service Requests 311 (detailed)
# ============================================================================
print("\n=== 2. Service Requests 311 (detailed, 20M rows) ===")
out.execute("""
    CREATE TABLE IF NOT EXISTS service_requests_311 (
        bbl INTEGER, complaint_type TEXT, descriptor TEXT,
        created_date TEXT, closed_date TEXT, status TEXT,
        resolution_description TEXT, incident_address TEXT,
        borough TEXT, latitude REAL, longitude REAL
    )
""")

batch = []
count = 0
matched_311 = 0
for row in big.execute("""
    SELECT bbl, complaint_type, descriptor, created_date, closed_date,
           status, resolution_description, incident_address, borough,
           latitude, longitude
    FROM service_requests_311
    WHERE bbl IS NOT NULL
"""):
    try:
        bbl_int = int(float(row[0])) if row[0] else None
    except (ValueError, TypeError):
        continue

    if bbl_int and bbl_int in valid_bbls:
        batch.append((bbl_int, row[1], row[2], row[3], row[4], row[5],
                      row[6], row[7], row[8], row[9], row[10]))
        matched_311 += 1

        if len(batch) >= 100000:
            out.executemany("INSERT INTO service_requests_311 VALUES (?,?,?,?,?,?,?,?,?,?,?)", batch)
            out.commit()
            count += len(batch)
            print(f"    {count:,} rows...")
            batch = []

if batch:
    out.executemany("INSERT INTO service_requests_311 VALUES (?,?,?,?,?,?,?,?,?,?,?)", batch)
    out.commit()
    count += len(batch)

out.execute("CREATE INDEX IF NOT EXISTS idx_sr311_bbl ON service_requests_311(bbl)")
out.execute("CREATE INDEX IF NOT EXISTS idx_sr311_type ON service_requests_311(complaint_type)")
out.execute("CREATE INDEX IF NOT EXISTS idx_sr311_date ON service_requests_311(created_date)")
out.commit()
print(f"  {count:,} service requests matched")

# ============================================================================
# 3. Certificates of Occupancy
# ============================================================================
print("\n=== 3. Certificates of Occupancy ===")
out.execute("""
    CREATE TABLE IF NOT EXISTS certificates_of_occupancy (
        bbl INTEGER, job_number TEXT, co_issue_date TEXT, co_type TEXT,
        existing_occupancy TEXT, proposed_occupancy TEXT,
        existing_dwelling_units TEXT, proposed_dwelling_units TEXT,
        existing_stories TEXT, proposed_stories TEXT
    )
""")

boro_map = {'Manhattan': 1, 'Bronx': 2, 'Brooklyn': 3, 'Queens': 4, 'Staten Island': 5}

batch = []
matched_co = 0
unmatched_co = 0
for row in big.execute("""
    SELECT bin, borough, block, lot, job_number, co_issue_date, co_type,
           existing_occupancy, proposed_occupancy,
           existing_dwelling_units, proposed_dwelling_units,
           existing_stories, proposed_stories
    FROM certificates_of_occupancy_new
"""):
    bin_val, borough, block, lot = row[0], row[1], row[2], row[3]
    matched_bbl = None

    # Try BIN
    if bin_val:
        matched_bbl = bin_to_bbl.get(str(bin_val))

    # Try boro+block+lot
    if not matched_bbl and borough and block and lot:
        boro_code = boro_map.get(borough)
        if boro_code:
            try:
                bbl = int(str(boro_code) + block[-5:] + lot[-4:])
                if bbl in valid_bbls:
                    matched_bbl = bbl
                else:
                    lot_num = int(lot[-4:])
                    if lot_num >= 1000:
                        bb = str(boro_code) + block[-5:]
                        best = block_best.get(bb)
                        if best:
                            matched_bbl = best[0]
            except (ValueError, TypeError):
                pass

    if matched_bbl:
        batch.append((matched_bbl, row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12]))
        matched_co += 1
    else:
        unmatched_co += 1

out.executemany("INSERT INTO certificates_of_occupancy VALUES (?,?,?,?,?,?,?,?,?,?)", batch)
out.execute("CREATE INDEX IF NOT EXISTS idx_co_bbl ON certificates_of_occupancy(bbl)")
out.commit()
print(f"  {matched_co:,} matched, {unmatched_co:,} unmatched")

# ============================================================================
# 4. Small reference tables worth keeping
# ============================================================================
print("\n=== 4. Reference tables ===")

# NYCHA developments
out.execute("""
    CREATE TABLE IF NOT EXISTS nycha_developments (
        development TEXT, borough TEXT, address TEXT, total_units INTEGER
    )
""")
nycha = big.execute("SELECT * FROM nycha_developments LIMIT 1").fetchone()
if nycha:
    # Check schema
    cols = [d[0] for d in big.execute("PRAGMA table_info(nycha_developments)").fetchall()]
    print(f"  NYCHA columns: {cols}")

    for row in big.execute("SELECT * FROM nycha_developments"):
        # Just grab what we can
        pass

nycha_count = big.execute("SELECT COUNT(*) FROM nycha_developments").fetchone()[0]
print(f"  NYCHA: {nycha_count} developments (small, skipping detailed import)")

# Housing connect
hc_count = big.execute("SELECT COUNT(*) FROM housing_connect_lotteries").fetchone()[0]
print(f"  Housing Connect: {hc_count} lotteries (small, skipping)")

# ============================================================================
# Final Report
# ============================================================================
print("\n" + "=" * 60)
print("  VAYO CLEAN DATABASE — FINAL STATE")
print("=" * 60)

for table in ['buildings', 'acris_transactions', 'complaints', 'complaints_311',
              'service_requests_311', 'ecb_violations', 'building_contacts',
              'hpd_litigation', 'dob_permits', 'dob_complaints',
              'marshal_evictions', 'rent_stabilized', 'certificates_of_occupancy',
              'bin_map']:
    c = out.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"  {table:<30} {c:>12,}")

size_mb = os.path.getsize(OUT_DB) / (1024 * 1024)
print(f"\n  Database size: {size_mb:,.0f} MB")
print(f"  Total time: {time.time()-t0:.1f}s")

big.close()
out.close()

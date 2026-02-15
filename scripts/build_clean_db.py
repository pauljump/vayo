#!/usr/bin/env python3
"""
VAYO Data Tightening
====================
Fix all BBL matching issues and build a clean, fast lookup database
for the apartment finder scoring engine.

Output: vayo_clean.db with properly joined tables
"""

import sqlite3
import time
from collections import defaultdict

BIG_DB = "/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"
OUT_DB = "/Users/pjump/Desktop/projects/vayo/vayo_clean.db"

import os
if os.path.exists(OUT_DB):
    os.remove(OUT_DB)

big = sqlite3.connect(BIG_DB)
big.row_factory = sqlite3.Row
out = sqlite3.connect(OUT_DB)
out.execute("PRAGMA journal_mode=WAL")
out.execute("PRAGMA synchronous=NORMAL")
out.execute("PRAGMA cache_size=-500000")  # 500MB cache

t0 = time.time()

# ============================================================================
# 1. PLUTO — the foundation
# ============================================================================
print("=== 1. PLUTO buildings ===")
out.execute("""
    CREATE TABLE buildings (
        bbl INTEGER PRIMARY KEY,
        borough TEXT, address TEXT, zipcode TEXT, bldgclass TEXT,
        yearbuilt INTEGER, numfloors INTEGER, unitsres INTEGER,
        unitstotal INTEGER, ownername TEXT, zonedist1 TEXT,
        assesstot REAL, lotarea INTEGER, bldgarea INTEGER,
        resarea INTEGER, comarea INTEGER,
        avg_sqft INTEGER, assessed_per_unit INTEGER
    )
""")

count = 0
for row in big.execute("""
    SELECT CAST(bbl AS INTEGER) as bbl, borough, address, zipcode, bldgclass,
           yearbuilt, CAST(numfloors AS INTEGER) as numfloors, unitsres,
           unitstotal, ownername, zonedist1, assesstot, lotarea, bldgarea,
           resarea, comarea
    FROM pluto WHERE unitsres > 0
"""):
    r = dict(row)
    r['avg_sqft'] = round(r['resarea'] / r['unitsres']) if r['unitsres'] > 0 and r['resarea'] else 0
    r['assessed_per_unit'] = round(r['assesstot'] / r['unitsres']) if r['unitsres'] > 0 and r['assesstot'] else 0
    out.execute("""INSERT INTO buildings VALUES (
        :bbl,:borough,:address,:zipcode,:bldgclass,:yearbuilt,:numfloors,
        :unitsres,:unitstotal,:ownername,:zonedist1,:assesstot,:lotarea,
        :bldgarea,:resarea,:comarea,:avg_sqft,:assessed_per_unit
    )""", r)
    count += 1

out.commit()
print(f"  {count} buildings")

# ============================================================================
# 2. BIN → BBL mapping (critical for joining HPD, complaints, ECB, DOB)
# ============================================================================
print("\n=== 2. BIN → BBL mapping ===")
bin_to_bbl = {}
for row in big.execute("SELECT bin, bbl FROM buildings WHERE bin IS NOT NULL AND bbl IS NOT NULL"):
    try:
        bbl_int = int(float(row['bbl']))
        bin_to_bbl[row['bin']] = bbl_int
    except (ValueError, TypeError):
        pass

# Verify against our PLUTO buildings
valid_bbls = set()
for row in out.execute("SELECT bbl FROM buildings"):
    valid_bbls.add(row[0])

bin_to_bbl = {k: v for k, v in bin_to_bbl.items() if v in valid_bbls}
print(f"  {len(bin_to_bbl)} BIN→BBL mappings (matching PLUTO)")

# Save mapping table
out.execute("CREATE TABLE bin_map (bin TEXT PRIMARY KEY, bbl INTEGER)")
out.executemany("INSERT INTO bin_map VALUES (?,?)", bin_to_bbl.items())
out.execute("CREATE INDEX idx_bm_bbl ON bin_map(bbl)")
out.commit()

# ============================================================================
# 3. ACRIS — build proper BBLs from boro+block+lot, link to PLUTO
# ============================================================================
print("\n=== 3. ACRIS transactions ===")

# Build boro_block → PLUTO BBL mapping for condo lots
pluto_by_block = defaultdict(list)
for row in out.execute("SELECT bbl, unitsres FROM buildings"):
    bbl_str = str(row[0])
    if len(bbl_str) == 10:
        pluto_by_block[bbl_str[:6]].append((row[0], row[1]))

# Sort each block by unit count descending
for bb in pluto_by_block:
    pluto_by_block[bb].sort(key=lambda x: x[1], reverse=True)

out.execute("""
    CREATE TABLE acris_transactions (
        document_id TEXT,
        bbl INTEGER,
        unit TEXT,
        doc_type TEXT,
        document_date TEXT,
        recorded_datetime TEXT,
        document_amt REAL,
        party_seller TEXT,
        party_buyer TEXT
    )
""")

# Process ACRIS: join master + real_property + parties
print("  Loading ACRIS data...")
# First get all documents with their parties pre-aggregated
doc_parties = defaultdict(lambda: {'sellers': [], 'buyers': []})
for row in big.execute("""
    SELECT document_id, party_type, name FROM acris_parties
    WHERE party_type IN ('1', '2') AND name IS NOT NULL AND name <> ''
"""):
    if row['party_type'] == '1':
        doc_parties[row['document_id']]['sellers'].append(row['name'])
    else:
        doc_parties[row['document_id']]['buyers'].append(row['name'])

print(f"  {len(doc_parties)} documents with parties")

# Now process real_property + master
acris_count = 0
batch = []
for row in big.execute("""
    SELECT r.document_id, r.borough, r.block, r.lot, r.unit,
           m.doc_type, m.document_date, m.recorded_datetime, m.document_amt
    FROM acris_real_property r
    JOIN acris_master m ON m.document_id = r.document_id
    WHERE r.borough IS NOT NULL AND r.block IS NOT NULL AND r.lot IS NOT NULL
    AND m.doc_type IN ('DEED','MTGE','SAT','AL&R','AALR','LPEN','RPTT&RET','AGMT')
"""):
    # Construct BBL
    try:
        boro = str(int(row['borough']))
        block = str(int(row['block'])).zfill(5)
        lot = str(int(row['lot'])).zfill(5) if row['lot'] else '00000'
        constructed_bbl = int(boro + block + lot)
    except (ValueError, TypeError):
        continue

    # Match to PLUTO
    matched_bbl = None
    if constructed_bbl in valid_bbls:
        matched_bbl = constructed_bbl
    else:
        # Condo lot mapping
        bb = (boro + block)
        lot_num = int(lot)
        if lot_num >= 1000 and bb in pluto_by_block:
            matched_bbl = pluto_by_block[bb][0][0]  # biggest building on block

    if matched_bbl is None:
        continue

    try:
        amt = float(row['document_amt']) if row['document_amt'] else 0
    except:
        amt = 0

    parties = doc_parties.get(row['document_id'], {'sellers': [], 'buyers': []})
    sellers = '; '.join(set(parties['sellers']))[:200]
    buyers = '; '.join(set(parties['buyers']))[:200]

    batch.append((
        row['document_id'], matched_bbl, row['unit'] or '',
        row['doc_type'], row['document_date'], row['recorded_datetime'],
        amt, sellers, buyers
    ))

    if len(batch) >= 50000:
        out.executemany("INSERT INTO acris_transactions VALUES (?,?,?,?,?,?,?,?,?)", batch)
        out.commit()
        acris_count += len(batch)
        print(f"    {acris_count:,} transactions...")
        batch = []

if batch:
    out.executemany("INSERT INTO acris_transactions VALUES (?,?,?,?,?,?,?,?,?)", batch)
    out.commit()
    acris_count += len(batch)

out.execute("CREATE INDEX idx_acris_bbl ON acris_transactions(bbl)")
out.execute("CREATE INDEX idx_acris_type ON acris_transactions(doc_type)")
out.execute("CREATE INDEX idx_acris_date ON acris_transactions(recorded_datetime)")
out.commit()
print(f"  {acris_count:,} ACRIS transactions matched to PLUTO buildings")

# ============================================================================
# 4. HPD Complaints (by BIN → BBL)
# ============================================================================
print("\n=== 4. HPD Complaints ===")
out.execute("""
    CREATE TABLE complaints (
        bbl INTEGER, unit TEXT, major_category TEXT, minor_category TEXT,
        status TEXT, received_date TEXT, type TEXT
    )
""")

comp_count = 0
batch = []
for row in big.execute("""
    SELECT bin, apartment, major_category, minor_category, status,
           received_date, type
    FROM complaints WHERE bin IS NOT NULL
"""):
    bbl = bin_to_bbl.get(row['bin'])
    if bbl:
        batch.append((bbl, row['apartment'], row['major_category'],
                      row['minor_category'], row['status'], row['received_date'],
                      row['type']))
        if len(batch) >= 50000:
            out.executemany("INSERT INTO complaints VALUES (?,?,?,?,?,?,?)", batch)
            out.commit()
            comp_count += len(batch)
            batch = []

if batch:
    out.executemany("INSERT INTO complaints VALUES (?,?,?,?,?,?,?)", batch)
    out.commit()
    comp_count += len(batch)

out.execute("CREATE INDEX idx_comp_bbl ON complaints(bbl)")
out.execute("CREATE INDEX idx_comp_date ON complaints(received_date)")
out.commit()
print(f"  {comp_count:,} complaints matched")

# ============================================================================
# 5. 311 (BBL already clean)
# ============================================================================
print("\n=== 5. 311 Complaints ===")
out.execute("""
    CREATE TABLE complaints_311 (
        bbl INTEGER, complaint_type TEXT, descriptor TEXT,
        created_date TEXT, incident_address TEXT
    )
""")

count_311 = 0
batch = []
for row in big.execute("""
    SELECT CAST(bbl AS INTEGER) as bbl, complaint_type, descriptor,
           created_date, incident_address
    FROM nyc_311_complete
    WHERE bbl IS NOT NULL AND CAST(bbl AS INTEGER) > 0
"""):
    if row['bbl'] in valid_bbls:
        batch.append((row['bbl'], row['complaint_type'], row['descriptor'],
                      row['created_date'], row['incident_address']))
        if len(batch) >= 50000:
            out.executemany("INSERT INTO complaints_311 VALUES (?,?,?,?,?)", batch)
            out.commit()
            count_311 += len(batch)
            batch = []

if batch:
    out.executemany("INSERT INTO complaints_311 VALUES (?,?,?,?,?)", batch)
    out.commit()
    count_311 += len(batch)

out.execute("CREATE INDEX idx_311_bbl ON complaints_311(bbl)")
out.execute("CREATE INDEX idx_311_type ON complaints_311(complaint_type)")
out.commit()
print(f"  {count_311:,} 311 complaints matched")

# ============================================================================
# 6. ECB Violations (BIN → BBL)
# ============================================================================
print("\n=== 6. ECB Violations ===")
out.execute("""
    CREATE TABLE ecb_violations (
        bbl INTEGER, severity TEXT, violation_type TEXT,
        issue_date TEXT, ecb_violation_status TEXT,
        violation_description TEXT, penality_imposed TEXT, balance_due TEXT
    )
""")

ecb_count = 0
batch = []
for row in big.execute("""
    SELECT bin, severity, violation_type, issue_date, ecb_violation_status,
           violation_description, penality_imposed, balance_due
    FROM ecb_violations WHERE bin IS NOT NULL
"""):
    bbl = bin_to_bbl.get(row['bin'])
    if bbl:
        batch.append((bbl, row['severity'], row['violation_type'],
                      row['issue_date'], row['ecb_violation_status'],
                      row['violation_description'], row['penality_imposed'],
                      row['balance_due']))
        if len(batch) >= 50000:
            out.executemany("INSERT INTO ecb_violations VALUES (?,?,?,?,?,?,?,?)", batch)
            out.commit()
            ecb_count += len(batch)
            batch = []

if batch:
    out.executemany("INSERT INTO ecb_violations VALUES (?,?,?,?,?,?,?,?)", batch)
    out.commit()
    ecb_count += len(batch)

out.execute("CREATE INDEX idx_ecb_bbl ON ecb_violations(bbl)")
out.commit()
print(f"  {ecb_count:,} ECB violations matched")

# ============================================================================
# 7. HPD Contacts (registrations → BIN → BBL)
# ============================================================================
print("\n=== 7. HPD Contacts ===")
out.execute("""
    CREATE TABLE building_contacts (
        bbl INTEGER, contact_type TEXT, corporation_name TEXT,
        first_name TEXT, last_name TEXT, registration_date TEXT
    )
""")

contact_count = 0
batch = []
for row in big.execute("""
    SELECT r.bin, c.contacttype, c.corporationname, c.firstname, c.lastname,
           r.lastregistrationdate
    FROM hpd_registrations r
    JOIN hpd_contacts c ON c.registrationid = r.registrationid
    WHERE r.bin IS NOT NULL
"""):
    bbl = bin_to_bbl.get(row['bin'])
    if bbl:
        batch.append((bbl, row['contacttype'], row['corporationname'],
                      row['firstname'], row['lastname'],
                      row['lastregistrationdate']))
        if len(batch) >= 50000:
            out.executemany("INSERT INTO building_contacts VALUES (?,?,?,?,?,?)", batch)
            out.commit()
            contact_count += len(batch)
            batch = []

if batch:
    out.executemany("INSERT INTO building_contacts VALUES (?,?,?,?,?,?)", batch)
    out.commit()
    contact_count += len(batch)

out.execute("CREATE INDEX idx_bc_bbl ON building_contacts(bbl)")
out.execute("CREATE INDEX idx_bc_type ON building_contacts(contact_type)")
out.commit()
print(f"  {contact_count:,} building contacts matched")

# ============================================================================
# 8. HPD Litigation
# ============================================================================
print("\n=== 8. HPD Litigation ===")
out.execute("""
    CREATE TABLE hpd_litigation (
        bbl INTEGER, casetype TEXT, caseopendate TEXT,
        casestatus TEXT
    )
""")

lit_count = 0
batch = []
for row in big.execute("""
    SELECT CAST(bbl AS INTEGER) as bbl, casetype, caseopendate, casestatus
    FROM hpd_litigation WHERE bbl IS NOT NULL
"""):
    if row['bbl'] in valid_bbls:
        batch.append((row['bbl'], row['casetype'], row['caseopendate'], row['casestatus']))

if batch:
    out.executemany("INSERT INTO hpd_litigation VALUES (?,?,?,?)", batch)
    lit_count = len(batch)

out.execute("CREATE INDEX idx_lit_bbl ON hpd_litigation(bbl)")
out.commit()
print(f"  {lit_count:,} litigation records matched")

# ============================================================================
# 9. DOB Permits (BIN → BBL)
# ============================================================================
print("\n=== 9. DOB Permits ===")
out.execute("""
    CREATE TABLE dob_permits (
        bbl INTEGER, job_type TEXT, job_description TEXT,
        job_status_description TEXT, latest_action_date TEXT,
        initial_cost TEXT, existing_dwelling_units TEXT,
        proposed_dwelling_units TEXT
    )
""")

permit_count = 0
batch = []
for row in big.execute("""
    SELECT bin, job_type, job_description, job_status_description,
           latest_action_date, initial_cost, existing_dwelling_units,
           proposed_dwelling_units
    FROM dob_permits WHERE bin IS NOT NULL
"""):
    bbl = bin_to_bbl.get(row['bin'])
    if bbl:
        batch.append((bbl, row['job_type'], row['job_description'],
                      row['job_status_description'], row['latest_action_date'],
                      row['initial_cost'], row['existing_dwelling_units'],
                      row['proposed_dwelling_units']))
        if len(batch) >= 50000:
            out.executemany("INSERT INTO dob_permits VALUES (?,?,?,?,?,?,?,?)", batch)
            out.commit()
            permit_count += len(batch)
            batch = []

if batch:
    out.executemany("INSERT INTO dob_permits VALUES (?,?,?,?,?,?,?,?)", batch)
    out.commit()
    permit_count += len(batch)

out.execute("CREATE INDEX idx_perm_bbl ON dob_permits(bbl)")
out.commit()
print(f"  {permit_count:,} DOB permits matched")

# ============================================================================
# 10. DOB Complaints (has BBL directly)
# ============================================================================
print("\n=== 10. DOB Complaints ===")
out.execute("""
    CREATE TABLE dob_complaints (
        bbl INTEGER, unit TEXT, complaint_category TEXT,
        disposition_code TEXT, disposition_date TEXT, raw_description TEXT
    )
""")

dobc_count = 0
batch = []
for row in big.execute("""
    SELECT CAST(bbl AS INTEGER) as bbl, unit, complaint_category,
           disposition_code, disposition_date, raw_description
    FROM dob_complaints WHERE bbl IS NOT NULL
"""):
    if row['bbl'] in valid_bbls:
        batch.append((row['bbl'], row['unit'], row['complaint_category'],
                      row['disposition_code'], row['disposition_date'],
                      row['raw_description']))
        if len(batch) >= 50000:
            out.executemany("INSERT INTO dob_complaints VALUES (?,?,?,?,?,?)", batch)
            out.commit()
            dobc_count += len(batch)
            batch = []

if batch:
    out.executemany("INSERT INTO dob_complaints VALUES (?,?,?,?,?,?)", batch)
    out.commit()
    dobc_count += len(batch)

out.execute("CREATE INDEX idx_dobc_bbl ON dob_complaints(bbl)")
out.commit()
print(f"  {dobc_count:,} DOB complaints matched")

# ============================================================================
# 11. Marshal Evictions
# ============================================================================
print("\n=== 11. Marshal Evictions ===")
out.execute("""
    CREATE TABLE marshal_evictions (
        eviction_address TEXT, executed_date TEXT, borough TEXT,
        residential_commercial TEXT
    )
""")
# These don't have BBL, keep for address-based matching later
big.execute("""SELECT COUNT(*) FROM marshal_evictions WHERE residential_commercial = 'Residential'""")
out.executemany("INSERT INTO marshal_evictions VALUES (?,?,?,?)",
    big.execute("""SELECT eviction_address, executed_date, borough, residential_commercial
                   FROM marshal_evictions WHERE residential_commercial = 'Residential'"""))
out.commit()
evict_count = out.execute("SELECT COUNT(*) FROM marshal_evictions").fetchone()[0]
print(f"  {evict_count:,} residential evictions loaded (address-only, no BBL)")

# ============================================================================
# FINAL REPORT
# ============================================================================
print("\n" + "=" * 60)
print("  VAYO CLEAN DATABASE — SUMMARY")
print("=" * 60)

for table in ['buildings', 'acris_transactions', 'complaints', 'complaints_311',
              'ecb_violations', 'building_contacts', 'hpd_litigation',
              'dob_permits', 'dob_complaints', 'marshal_evictions', 'bin_map']:
    count = out.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"  {table:<25} {count:>12,}")

size_mb = os.path.getsize(OUT_DB) / (1024 * 1024)
print(f"\n  Database size: {size_mb:.0f} MB")
print(f"  Total time: {time.time()-t0:.1f}s")

big.close()
out.close()

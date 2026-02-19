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
import json
from collections import defaultdict
from pathlib import Path

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
# 3. ACRIS — stream from full cache files, link to PLUTO
# ============================================================================
print("\n=== 3. ACRIS transactions (from full cache) ===")

ACRIS_CACHE = "/Users/pjump/Desktop/projects/vayo/acris_cache/full"
TARGET_DOC_TYPES = {
    'DEED', 'MTGE', 'SAT', 'AGMT', 'LPNS', 'AL&R', 'AALR', 'LPEN', 'RPTT&RET',
    'DEED, TS',  # Tax sale deeds (distress signal)
    'DEED, LE',  # Life estate deeds
    'ASST',      # Mortgage assignments (bank-to-bank transfers)
    'PREL',      # Pre-liens (foreclosure indicator)
}
JUNK_UNITS = {'N/A', 'NA', '0', '00', '000', '-', '.', 'NONE', 'X', 'XX', 'TIMES', 'APT', 'UNIT'}

def iter_cache_dir(path):
    """Iterate over all batch_*.json files in a directory (flat or nested)."""
    for f in sorted(Path(path).glob('**/batch_*.json')):
        with open(f) as fh:
            yield from json.load(fh)

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

# --- Step 1: Stream master → filter to target doc types ---
print("  Step 1: Streaming master records...")
master_docs = {}  # {doc_id: (doc_type, document_amt, document_date, recorded_datetime)}
master_total = 0
for rec in iter_cache_dir(f"{ACRIS_CACHE}/master"):
    master_total += 1
    if rec['doc_type'] in TARGET_DOC_TYPES:
        try:
            amt = float(rec['document_amt']) if rec['document_amt'] else 0
        except (ValueError, TypeError):
            amt = 0
        master_docs[rec['document_id']] = (
            rec['doc_type'], amt,
            rec['document_date'], rec['recorded_datetime']
        )
    if master_total % 5_000_000 == 0:
        print(f"    scanned {master_total:,} master, kept {len(master_docs):,}")
print(f"    Done: {master_total:,} master scanned → {len(master_docs):,} target docs")

# --- Step 2: Stream legals → match to master + PLUTO → doc_bbl ---
print("  Step 2: Streaming legals records...")
doc_bbl = {}  # {doc_id: (bbl, unit)}
legals_total = 0
for rec in iter_cache_dir(f"{ACRIS_CACHE}/legals_parts"):
    legals_total += 1
    doc_id = rec['document_id']
    if doc_id not in master_docs:
        continue
    # Construct BBL
    try:
        boro = str(int(rec['borough']))
        block = str(int(rec['block'])).zfill(5)
        lot_raw = rec.get('lot')
        lot = str(int(lot_raw)).zfill(4) if lot_raw else '0000'
        constructed_bbl = int(boro + block + lot)
    except (ValueError, TypeError):
        continue

    # Match to PLUTO
    matched_bbl = None
    if constructed_bbl in valid_bbls:
        matched_bbl = constructed_bbl
    else:
        bb = boro + block
        lot_num = int(lot)
        if lot_num >= 1000 and bb in pluto_by_block:
            matched_bbl = pluto_by_block[bb][0][0]

    if matched_bbl is not None:
        unit = (rec.get('unit') or '').strip()
        if unit.upper() in JUNK_UNITS:
            unit = ''
        doc_bbl[doc_id] = (matched_bbl, unit)

    if legals_total % 5_000_000 == 0:
        print(f"    scanned {legals_total:,} legals, matched {len(doc_bbl):,}")
print(f"    Done: {legals_total:,} legals scanned → {len(doc_bbl):,} matched to PLUTO")

# --- Step 3: Stream parties → only for matched doc_ids ---
print("  Step 3: Streaming parties records...")
doc_parties = defaultdict(lambda: {'sellers': [], 'buyers': []})
parties_total = 0
parties_kept = 0
for rec in iter_cache_dir(f"{ACRIS_CACHE}/parties_parts"):
    parties_total += 1
    doc_id = rec['document_id']
    if doc_id not in doc_bbl:
        continue
    if not rec.get('name'):
        continue
    parties_kept += 1
    bucket = doc_parties[doc_id]
    if rec['party_type'] == '1':
        if len(bucket['sellers']) < 3:
            bucket['sellers'].append(rec['name'])
    else:
        if len(bucket['buyers']) < 3:
            bucket['buyers'].append(rec['name'])
    if parties_total % 10_000_000 == 0:
        print(f"    scanned {parties_total:,} parties, kept {parties_kept:,}")
print(f"    Done: {parties_total:,} parties scanned → {len(doc_parties):,} docs with parties")

# --- Step 4: Join and insert ---
print("  Step 4: Joining and inserting...")
acris_count = 0
batch = []
for doc_id, (bbl, unit) in doc_bbl.items():
    doc_type, amt, doc_date, rec_dt = master_docs[doc_id]
    parties = doc_parties.get(doc_id, {'sellers': [], 'buyers': []})
    sellers = '; '.join(parties['sellers'])[:200]
    buyers = '; '.join(parties['buyers'])[:200]

    batch.append((doc_id, bbl, unit, doc_type, doc_date, rec_dt, amt, sellers, buyers))

    if len(batch) >= 50000:
        out.executemany("INSERT INTO acris_transactions VALUES (?,?,?,?,?,?,?,?,?)", batch)
        out.commit()
        acris_count += len(batch)
        print(f"    {acris_count:,} transactions inserted...")
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

# Free memory
del master_docs, doc_bbl, doc_parties

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
# 12. StreetEasy slug → BBL mapping
# ============================================================================
print("\n=== 12. StreetEasy slug → BBL mapping ===")

SE_SLUGS_FILE = "/Users/pjump/Desktop/projects/vayo/se_sitemaps/all_buildings.txt"

# NYC neighborhood/city → borough mapping
NYC_CITIES = {
    'new_york': 'MN', 'manhattan': 'MN',
    'brooklyn': 'BK', 'bronx': 'BX', 'queens': 'QN', 'staten_island': 'SI',
    # Brooklyn neighborhoods
    'bed_stuy': 'BK', 'williamsburg': 'BK', 'bushwick': 'BK', 'park_slope': 'BK',
    'greenpoint': 'BK', 'flatbush': 'BK', 'crown_heights': 'BK',
    'east_new_york': 'BK', 'sunset_park': 'BK', 'bay_ridge': 'BK',
    'bensonhurst': 'BK', 'sheepshead_bay': 'BK', 'canarsie': 'BK',
    'east_flatbush': 'BK', 'brownsville': 'BK', 'prospect_heights': 'BK',
    'prospect_lefferts_gardens': 'BK', 'windsor_terrace': 'BK', 'kensington': 'BK',
    'borough_park': 'BK', 'dyker_heights': 'BK', 'gravesend': 'BK', 'midwood': 'BK',
    'brighton_beach': 'BK', 'coney_island': 'BK', 'marine_park': 'BK',
    'mill_basin': 'BK', 'cobble_hill': 'BK', 'carroll_gardens': 'BK',
    'boerum_hill': 'BK', 'fort_greene': 'BK', 'clinton_hill': 'BK', 'dumbo': 'BK',
    'downtown_brooklyn': 'BK', 'brooklyn_heights': 'BK', 'red_hook': 'BK',
    'gowanus': 'BK', 'flatlands': 'BK', 'bergen_beach': 'BK',
    'gerritsen_beach': 'BK', 'manhattan_beach': 'BK', 'bath_beach': 'BK',
    'cypress_hills': 'BK', 'east_williamsburg': 'BK', 'greenwood': 'BK',
    'south_slope': 'BK', 'ditmas_park': 'BK', 'victorian_flatbush': 'BK',
    'prospect_park_south': 'BK', 'columbia_street_waterfront': 'BK',
    'vinegar_hill': 'BK', 'navy_yard': 'BK', 'weeksville': 'BK',
    'stuyvesant_heights': 'BK', 'ocean_hill': 'BK', 'starrett_city': 'BK',
    'remsen_village': 'BK', 'rugby': 'BK', 'sea_gate': 'BK',
    # Queens neighborhoods
    'astoria': 'QN', 'flushing': 'QN', 'jamaica': 'QN', 'ozone_park': 'QN',
    'south_ozone_park': 'QN', 'corona': 'QN', 'jackson_heights': 'QN',
    'queens_village': 'QN', 'forest_hills': 'QN', 'richmond_hill': 'QN',
    'south_richmond_hill': 'QN', 'saint_albans': 'QN', 'hollis': 'QN',
    'east_elmhurst': 'QN', 'far_rockaway': 'QN', 'woodside': 'QN',
    'rego_park': 'QN', 'bayside': 'QN', 'long_island_city': 'QN',
    'sunnyside': 'QN', 'kew_gardens': 'QN', 'elmhurst': 'QN', 'ridgewood': 'QN',
    'middle_village': 'QN', 'maspeth': 'QN', 'woodhaven': 'QN',
    'howard_beach': 'QN', 'springfield_gardens': 'QN', 'fresh_meadows': 'QN',
    'college_point': 'QN', 'whitestone': 'QN', 'belle_harbor': 'QN',
    'rockaway_park': 'QN', 'arverne': 'QN', 'cambria_heights': 'QN',
    'laurelton': 'QN', 'rosedale': 'QN', 'briarwood': 'QN', 'oakland_gardens': 'QN',
    'glen_oaks': 'QN', 'little_neck': 'QN', 'douglaston': 'QN',
    'floral_park': 'QN', 'bellerose': 'QN', 'kew_gardens_hills': 'QN',
    'south_jamaica': 'QN', 'st._albans': 'QN', 'addisleigh_park': 'QN',
    'murray_hill_queens': 'QN', 'ditmars': 'QN', 'steinway': 'QN',
    'hunters_point': 'QN', 'ravenswood': 'QN', 'broad_channel': 'QN',
    'rockaway_beach': 'QN', 'neponsit': 'QN', 'breezy_point': 'QN',
    # Bronx neighborhoods
    'riverdale': 'BX', 'kingsbridge': 'BX', 'fordham': 'BX', 'pelham_bay': 'BX',
    'throgs_neck': 'BX', 'morris_park': 'BX', 'parkchester': 'BX',
    'soundview': 'BX', 'hunts_point': 'BX', 'mott_haven': 'BX',
    'highbridge': 'BX', 'concourse': 'BX', 'tremont': 'BX', 'belmont': 'BX',
    'university_heights': 'BX', 'norwood': 'BX', 'wakefield': 'BX',
    'williamsbridge': 'BX', 'baychester': 'BX', 'co_op_city': 'BX',
    'city_island': 'BX', 'country_club': 'BX', 'castle_hill': 'BX',
    'clason_point': 'BX', 'van_nest': 'BX', 'westchester_square': 'BX',
    'schuylerville': 'BX', 'edgewater_park': 'BX', 'spuyten_duyvil': 'BX',
    'fieldston': 'BX', 'mount_hope': 'BX', 'morrisania': 'BX',
    'longwood': 'BX', 'port_morris': 'BX', 'melrose': 'BX',
    # Staten Island neighborhoods
    'new_brighton': 'SI', 'st._george': 'SI', 'tompkinsville': 'SI',
    'stapleton': 'SI', 'great_kills': 'SI', 'tottenville': 'SI',
    'new_dorp': 'SI', 'midland_beach': 'SI', 'south_beach': 'SI',
    'dongan_hills': 'SI', 'grant_city': 'SI', 'eltingville': 'SI',
    'annadale': 'SI', 'huguenot': 'SI', 'princes_bay': 'SI',
    'rossville': 'SI', 'woodrow': 'SI', 'charleston': 'SI',
    'west_brighton': 'SI', 'port_richmond': 'SI', 'mariners_harbor': 'SI',
    'bulls_head': 'SI', 'travis': 'SI', 'willowbrook': 'SI',
    'westerleigh': 'SI', 'castleton_corners': 'SI', 'todt_hill': 'SI',
    'richmondtown': 'SI', 'oakwood': 'SI', 'grasmere': 'SI',
}

def normalize_addr(addr):
    """Normalize address for matching: uppercase, abbreviate street types."""
    if not addr:
        return ''
    a = addr.upper().strip()
    a = a.replace('_', '-')
    # Abbreviate common suffixes
    for full, abbr in [(' AVENUE', ' AVE'), (' STREET', ' ST'), (' BOULEVARD', ' BLVD'),
                       (' DRIVE', ' DR'), (' PLACE', ' PL'), (' ROAD', ' RD'),
                       (' COURT', ' CT'), (' LANE', ' LN'), (' TERRACE', ' TERR'),
                       (' PARKWAY', ' PKWY'), (' SQUARE', ' SQ'), (' CRESCENT', ' CRES')]:
        a = a.replace(full, abbr)
    return a

# Build PLUTO address lookup
pluto_addr_lookup = {}
for row in out.execute("SELECT bbl, address, borough FROM buildings"):
    key = (normalize_addr(row[1]), row[2])
    pluto_addr_lookup[key] = row[0]

out.execute("""
    CREATE TABLE se_buildings (
        slug TEXT PRIMARY KEY,
        bbl INTEGER,
        se_address TEXT,
        borough TEXT
    )
""")

se_matched = 0
se_total = 0
batch = []

if os.path.exists(SE_SLUGS_FILE):
    with open(SE_SLUGS_FILE) as f:
        for line in f:
            slug = line.strip()
            if not slug:
                continue

            # Parse slug: strip URL encoding, extract address and city
            slug_clean = slug.split('?')[0].strip('/').lstrip('%23')

            # Match city suffix (longest first to avoid partial matches)
            matched_city = None
            matched_boro = None
            for city in sorted(NYC_CITIES.keys(), key=len, reverse=True):
                if slug_clean.endswith('-' + city):
                    matched_city = city
                    matched_boro = NYC_CITIES[city]
                    break

            if not matched_boro:
                continue

            se_total += 1
            addr_part = slug_clean[:-(len(matched_city) + 1)]
            addr = normalize_addr(addr_part.replace('-', ' '))

            bbl = pluto_addr_lookup.get((addr, matched_boro))
            if bbl:
                se_matched += 1
                batch.append((slug, bbl, addr_part.replace('-', ' '), matched_boro))

                if len(batch) >= 50000:
                    out.executemany("INSERT OR IGNORE INTO se_buildings VALUES (?,?,?,?)", batch)
                    out.commit()
                    batch = []

if batch:
    out.executemany("INSERT OR IGNORE INTO se_buildings VALUES (?,?,?,?)", batch)
    out.commit()

out.execute("CREATE INDEX idx_se_bbl ON se_buildings(bbl)")
out.commit()
print(f"  {se_total:,} NYC slugs parsed, {se_matched:,} matched to PLUTO ({100*se_matched/max(se_total,1):.1f}%)")

# ============================================================================
# FINAL REPORT
# ============================================================================
print("\n" + "=" * 60)
print("  VAYO CLEAN DATABASE — SUMMARY")
print("=" * 60)

for table in ['buildings', 'acris_transactions', 'complaints', 'complaints_311',
              'ecb_violations', 'building_contacts', 'hpd_litigation',
              'dob_permits', 'dob_complaints', 'marshal_evictions', 'se_buildings', 'bin_map']:
    count = out.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"  {table:<25} {count:>12,}")

size_mb = os.path.getsize(OUT_DB) / (1024 * 1024)
print(f"\n  Database size: {size_mb:.0f} MB")
print(f"  Total time: {time.time()-t0:.1f}s")

big.close()
out.close()

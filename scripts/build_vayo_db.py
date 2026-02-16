#!/usr/bin/env python3
"""
VAYO Database Builder
=====================
Single script that creates vayo_clean.db from scratch with a clean,
consistent schema. All tables keyed on BBL with proper types and indexes.

Data sources:
  - Previous vayo_clean.db (renamed to vayo_old.db during build)
    Contains: buildings, HPD complaints, 311, ECB, DOB, contacts, etc.
  - ACRIS cache files from pull_acris_full.py (if available)
    Falls back to ACRIS data from old DB

Usage:
    python3 scripts/build_vayo_db.py [--acris-from-cache] [--acris-from-old]
"""

import sqlite3
import json
import time
import os
import sys
import shutil
from collections import defaultdict
from pathlib import Path

PROJECT = Path("/Users/pjump/Desktop/projects/vayo")
OUT_DB = PROJECT / "vayo_clean.db"
OLD_DB = PROJECT / "vayo_old.db"
ACRIS_CACHE = PROJECT / "acris_cache" / "full"
DATA_CACHE = PROJECT / "data_cache"

TARGET_DOC_TYPES = {'DEED', 'MTGE', 'SAT', 'AGMT', 'LPNS', 'AL&R'}


def parse_float(val):
    """Safely parse a float from any value."""
    if val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def parse_int(val):
    """Safely parse an int from any value."""
    if val is None:
        return 0
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return 0


def normalize_date(val):
    """Normalize various date formats to ISO YYYY-MM-DD."""
    if not val:
        return None
    val = str(val).strip()
    # Already ISO
    if len(val) >= 10 and val[4] == '-':
        return val[:10]
    # YYYYMMDD format (ECB violations use this)
    if len(val) == 8 and val.isdigit():
        return f"{val[:4]}-{val[4:6]}-{val[6:8]}"
    # MM/DD/YYYY
    if '/' in val:
        parts = val.split('/')
        if len(parts) == 3:
            m, d, y = parts
            if len(y) == 4:
                return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    return val[:10] if len(val) >= 10 else val


def make_bbl(boro, block, lot):
    """Construct BBL from boro, block, lot strings."""
    try:
        b = str(int(boro))
        bl = str(int(block)).zfill(5)
        lt = str(int(lot)).zfill(4)
        return int(f"{b}{bl}{lt}")
    except (ValueError, TypeError):
        return None


def iter_acris_cache(name):
    """Iterate over cached ACRIS batch files."""
    cache_dir = ACRIS_CACHE / name
    if not cache_dir.exists():
        return
    for f in sorted(cache_dir.glob('batch_*.json')):
        with open(f) as fh:
            yield from json.load(fh)


def iter_data_cache(name):
    """Iterate over cached dataset batch files."""
    cache_dir = DATA_CACHE / name
    if not cache_dir.exists():
        return
    for f in sorted(cache_dir.glob('batch_*.json')):
        with open(f) as fh:
            yield from json.load(fh)


def batch_insert(db, sql, rows, batch_size=50000, label=""):
    """Insert rows in batches with progress."""
    total = 0
    batch = []
    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            db.executemany(sql, batch)
            db.commit()
            total += len(batch)
            if label:
                print(f"    {label}: {total:,}...", flush=True)
            batch = []
    if batch:
        db.executemany(sql, batch)
        db.commit()
        total += len(batch)
    return total


def main():
    args = set(sys.argv[1:])
    use_cache = '--acris-from-cache' in args or ACRIS_CACHE.exists()
    force_old = '--acris-from-old' in args

    t0 = time.time()

    # ── Prepare files ─────────────────────────────────────────────────────
    if OLD_DB.exists():
        print(f"Using existing {OLD_DB.name} as source")
        if OUT_DB.exists():
            os.remove(str(OUT_DB))
    elif OUT_DB.exists():
        print(f"Renaming {OUT_DB.name} → {OLD_DB.name}")
        shutil.move(str(OUT_DB), str(OLD_DB))
    else:
        print(f"ERROR: Neither {OUT_DB} nor {OLD_DB} found. Nothing to rebuild from.")
        sys.exit(1)

    old = sqlite3.connect(str(OLD_DB))
    old.row_factory = sqlite3.Row

    out = sqlite3.connect(str(OUT_DB))
    out.execute("PRAGMA journal_mode=WAL")
    out.execute("PRAGMA synchronous=NORMAL")
    out.execute("PRAGMA cache_size=-500000")

    print()
    print("=" * 70)
    print("  VAYO DATABASE BUILD")
    print("=" * 70)
    print()

    # ══════════════════════════════════════════════════════════════════════
    # 1. BUILDINGS — the foundation
    # ══════════════════════════════════════════════════════════════════════
    print("━━━ 1/18 Buildings ━━━")
    out.execute("""
        CREATE TABLE buildings (
            bbl INTEGER PRIMARY KEY,
            borough TEXT NOT NULL,
            address TEXT,
            zipcode TEXT,
            building_class TEXT,
            year_built INTEGER,
            num_floors INTEGER,
            units_residential INTEGER,
            units_total INTEGER,
            owner_name TEXT,
            zoning TEXT,
            assessed_total REAL,
            lot_area INTEGER,
            building_area INTEGER,
            residential_area INTEGER,
            commercial_area INTEGER,
            avg_unit_sqft INTEGER,
            assessed_per_unit INTEGER
        )
    """)

    count = 0
    for row in old.execute("""
        SELECT bbl, borough, address, zipcode, bldgclass,
               yearbuilt, numfloors, unitsres, unitstotal,
               ownername, zonedist1, assesstot, lotarea, bldgarea,
               resarea, comarea, avg_sqft, assessed_per_unit
        FROM buildings
    """):
        out.execute("""INSERT INTO buildings VALUES (
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
        )""", (
            row['bbl'], row['borough'], row['address'], row['zipcode'],
            row['bldgclass'], row['yearbuilt'], row['numfloors'],
            row['unitsres'], row['unitstotal'], row['ownername'],
            row['zonedist1'], row['assesstot'], row['lotarea'],
            row['bldgarea'], row['resarea'], row['comarea'],
            row['avg_sqft'], row['assessed_per_unit']
        ))
        count += 1
    out.commit()
    print(f"  {count:,} buildings")

    # Build BBL lookup sets
    valid_bbls = set(r[0] for r in out.execute("SELECT bbl FROM buildings"))

    # ══════════════════════════════════════════════════════════════════════
    # 2. BIN → BBL mapping
    # ══════════════════════════════════════════════════════════════════════
    print("\n━━━ 2/18 BIN → BBL mapping ━━━")
    out.execute("CREATE TABLE _bin_map (bin INTEGER PRIMARY KEY, bbl INTEGER NOT NULL)")

    bin_to_bbl = {}
    for row in old.execute("SELECT bin, bbl FROM bin_map"):
        try:
            b = int(row['bin'])
            bbl = int(row['bbl'])
            if bbl in valid_bbls:
                bin_to_bbl[b] = bbl
        except (ValueError, TypeError):
            pass

    out.executemany("INSERT INTO _bin_map VALUES (?,?)", bin_to_bbl.items())
    out.execute("CREATE INDEX idx_binmap_bbl ON _bin_map(bbl)")
    out.commit()
    print(f"  {len(bin_to_bbl):,} BIN→BBL mappings")

    # ══════════════════════════════════════════════════════════════════════
    # 3. SALES (ACRIS transactions)
    # ══════════════════════════════════════════════════════════════════════
    print("\n━━━ 3/18 Sales (ACRIS) ━━━")
    out.execute("""
        CREATE TABLE sales (
            document_id TEXT NOT NULL,
            bbl INTEGER NOT NULL,
            unit TEXT,
            doc_type TEXT NOT NULL,
            document_date TEXT,
            recorded_date TEXT,
            amount REAL,
            seller TEXT,
            buyer TEXT
        )
    """)

    # Build block index for condo lot resolution
    block_index = defaultdict(list)
    for bbl in valid_bbls:
        s = str(bbl)
        if len(s) == 10:
            block_index[s[:6]].append(bbl)

    acris_source = 'old'
    acris_count = 0

    if not force_old and (ACRIS_CACHE / 'master').exists():
        # ── Load from full ACRIS cache ──────────────────────────────────
        acris_source = 'cache'
        print("  Loading from ACRIS cache (full history)...")

        # Index master records by document_id
        print("  Reading master records...", flush=True)
        master_by_id = {}
        m_count = 0
        for rec in iter_acris_cache('master'):
            did = rec.get('document_id', '')
            dt = rec.get('doc_type', '')
            if did and dt in TARGET_DOC_TYPES:
                master_by_id[did] = rec
            m_count += 1
            if m_count % 1_000_000 == 0:
                print(f"    {m_count:,} master records read...", flush=True)
        print(f"  Master: {m_count:,} total → {len(master_by_id):,} target doc types")

        # Map documents to BBLs via legals
        print("  Reading legals...", flush=True)
        doc_bbl = {}
        doc_unit = {}
        l_count = 0
        for leg in iter_acris_cache('legals'):
            did = leg.get('document_id', '')
            if did not in master_by_id:
                continue
            bbl = make_bbl(leg.get('borough', ''), leg.get('block', ''), leg.get('lot', ''))
            if not bbl:
                continue
            if bbl in valid_bbls:
                doc_bbl[did] = bbl
                doc_unit[did] = leg.get('unit', '')
            else:
                # Condo lot resolution: match to largest building on same block
                bbl_str = str(bbl)
                if len(bbl_str) == 10:
                    lot_num = int(bbl_str[6:])
                    if lot_num >= 1000:
                        candidates = block_index.get(bbl_str[:6], [])
                        if candidates:
                            doc_bbl[did] = candidates[0]
                            doc_unit[did] = leg.get('unit', '')
            l_count += 1
            if l_count % 1_000_000 == 0:
                print(f"    {l_count:,} legals read...", flush=True)
        print(f"  Matched {len(doc_bbl):,} documents to buildings")

        # Map parties
        print("  Reading parties...", flush=True)
        party_by_doc = {}
        p_count = 0
        for p in iter_acris_cache('parties'):
            did = p.get('document_id', '')
            if did not in doc_bbl:
                continue
            if did not in party_by_doc:
                party_by_doc[did] = {'sellers': [], 'buyers': []}
            pt = p.get('party_type', '')
            name = (p.get('name') or '').strip()
            if pt == '1' and len(party_by_doc[did]['sellers']) < 3:
                party_by_doc[did]['sellers'].append(name)
            elif pt == '2' and len(party_by_doc[did]['buyers']) < 3:
                party_by_doc[did]['buyers'].append(name)
            p_count += 1
            if p_count % 1_000_000 == 0:
                print(f"    {p_count:,} parties read...", flush=True)
        print(f"  Parties for {len(party_by_doc):,} documents")

        # Insert
        def gen_sales_from_cache():
            for did, m in master_by_id.items():
                if did not in doc_bbl:
                    continue
                pi = party_by_doc.get(did, {'sellers': [], 'buyers': []})
                yield (
                    did,
                    doc_bbl[did],
                    doc_unit.get(did, ''),
                    m.get('doc_type', ''),
                    (m.get('document_date') or '')[:10],
                    (m.get('recorded_datetime') or '')[:10],
                    parse_float(m.get('document_amt')),
                    '; '.join(pi['sellers'])[:200],
                    '; '.join(pi['buyers'])[:200],
                )

        acris_count = batch_insert(
            out,
            "INSERT INTO sales VALUES (?,?,?,?,?,?,?,?,?)",
            gen_sales_from_cache(),
            label="sales"
        )

    else:
        # ── Load from old database ──────────────────────────────────────
        print("  Loading from old database...")

        def gen_sales_from_old():
            for row in old.execute("""
                SELECT document_id, bbl, unit, doc_type, document_date,
                       recorded_datetime, document_amt, party_seller, party_buyer
                FROM acris_transactions
            """):
                yield (
                    row['document_id'],
                    row['bbl'],
                    row['unit'],
                    row['doc_type'],
                    (row['document_date'] or '')[:10],
                    (row['recorded_datetime'] or '')[:10],
                    parse_float(row['document_amt']),
                    row['party_seller'] or '',
                    row['party_buyer'] or '',
                )

        acris_count = batch_insert(
            out,
            "INSERT INTO sales VALUES (?,?,?,?,?,?,?,?,?)",
            gen_sales_from_old(),
            label="sales"
        )

    out.execute("CREATE INDEX idx_sales_bbl ON sales(bbl)")
    out.execute("CREATE INDEX idx_sales_date ON sales(recorded_date)")
    out.execute("CREATE INDEX idx_sales_type ON sales(doc_type)")
    out.execute("CREATE INDEX idx_sales_bbl_type ON sales(bbl, doc_type)")
    out.commit()
    print(f"  {acris_count:,} sales from {acris_source}")

    # ══════════════════════════════════════════════════════════════════════
    # 4. HPD COMPLAINTS
    # ══════════════════════════════════════════════════════════════════════
    print("\n━━━ 4/18 HPD complaints ━━━")
    out.execute("""
        CREATE TABLE hpd_complaints (
            bbl INTEGER NOT NULL,
            unit TEXT,
            category TEXT,
            subcategory TEXT,
            status TEXT,
            received_date TEXT,
            severity TEXT
        )
    """)

    def gen_hpd():
        for row in old.execute("""
            SELECT bbl, unit, major_category, minor_category,
                   status, received_date, type
            FROM complaints
        """):
            yield (
                row['bbl'], row['unit'],
                row['major_category'], row['minor_category'],
                row['status'], row['received_date'], row['type']
            )

    hpd_count = batch_insert(out, "INSERT INTO hpd_complaints VALUES (?,?,?,?,?,?,?)",
                              gen_hpd(), label="hpd_complaints")
    out.execute("CREATE INDEX idx_hpd_bbl ON hpd_complaints(bbl)")
    out.execute("CREATE INDEX idx_hpd_date ON hpd_complaints(received_date)")
    out.commit()
    print(f"  {hpd_count:,} HPD complaints")

    # ══════════════════════════════════════════════════════════════════════
    # 5. SERVICE REQUESTS (311) — merge both tables, deduplicate
    # ══════════════════════════════════════════════════════════════════════
    print("\n━━━ 5/18 Service requests (311) ━━━")
    out.execute("""
        CREATE TABLE service_requests (
            bbl INTEGER NOT NULL,
            complaint_type TEXT,
            descriptor TEXT,
            created_date TEXT,
            closed_date TEXT,
            status TEXT,
            resolution TEXT,
            address TEXT,
            borough TEXT,
            latitude REAL,
            longitude REAL
        )
    """)

    # Use the larger service_requests_311 table (15M) as primary
    # The smaller complaints_311 (3.3M) is a subset with fewer columns
    sr_count = 0

    # Check which tables exist in old DB
    old_tables = set(r[0] for r in old.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"))

    if 'service_requests_311' in old_tables:
        def gen_sr():
            for row in old.execute("""
                SELECT bbl, complaint_type, descriptor, created_date,
                       closed_date, status, resolution_description,
                       incident_address, borough, latitude, longitude
                FROM service_requests_311
                WHERE bbl IS NOT NULL
            """):
                yield (
                    row['bbl'], row['complaint_type'], row['descriptor'],
                    row['created_date'], row['closed_date'], row['status'],
                    row['resolution_description'], row['incident_address'],
                    row['borough'], row['latitude'], row['longitude']
                )

        sr_count = batch_insert(out, "INSERT INTO service_requests VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                                gen_sr(), label="service_requests")
    elif 'complaints_311' in old_tables:
        def gen_sr_fallback():
            for row in old.execute("""
                SELECT bbl, complaint_type, descriptor, created_date,
                       incident_address
                FROM complaints_311
                WHERE bbl IS NOT NULL
            """):
                yield (
                    row['bbl'], row['complaint_type'], row['descriptor'],
                    row['created_date'], None, None, None,
                    row['incident_address'], None, None, None
                )

        sr_count = batch_insert(out, "INSERT INTO service_requests VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                                gen_sr_fallback(), label="service_requests")

    out.execute("CREATE INDEX idx_sr_bbl ON service_requests(bbl)")
    out.execute("CREATE INDEX idx_sr_date ON service_requests(created_date)")
    out.execute("CREATE INDEX idx_sr_type ON service_requests(complaint_type)")
    out.commit()
    print(f"  {sr_count:,} service requests")

    # ══════════════════════════════════════════════════════════════════════
    # 6. DOB PERMITS
    # ══════════════════════════════════════════════════════════════════════
    print("\n━━━ 6/18 DOB permits ━━━")
    out.execute("""
        CREATE TABLE permits (
            bbl INTEGER NOT NULL,
            job_type TEXT,
            description TEXT,
            status TEXT,
            action_date TEXT,
            estimated_cost REAL,
            existing_units INTEGER,
            proposed_units INTEGER
        )
    """)

    def gen_permits():
        for row in old.execute("""
            SELECT bbl, job_type, job_description, job_status_description,
                   latest_action_date, initial_cost,
                   existing_dwelling_units, proposed_dwelling_units
            FROM dob_permits
        """):
            yield (
                row['bbl'], row['job_type'], row['job_description'],
                row['job_status_description'], row['latest_action_date'],
                parse_float(row['initial_cost']),
                parse_int(row['existing_dwelling_units']),
                parse_int(row['proposed_dwelling_units']),
            )

    permit_count = batch_insert(out, "INSERT INTO permits VALUES (?,?,?,?,?,?,?,?)",
                                gen_permits(), label="permits")
    out.execute("CREATE INDEX idx_permits_bbl ON permits(bbl)")
    out.execute("CREATE INDEX idx_permits_date ON permits(action_date)")
    out.commit()
    print(f"  {permit_count:,} permits")

    # ══════════════════════════════════════════════════════════════════════
    # 7. DOB COMPLAINTS
    # ══════════════════════════════════════════════════════════════════════
    print("\n━━━ 7/18 DOB complaints ━━━")
    out.execute("""
        CREATE TABLE dob_complaints (
            bbl INTEGER NOT NULL,
            unit TEXT,
            category TEXT,
            disposition TEXT,
            disposition_date TEXT,
            description TEXT
        )
    """)

    def gen_dobc():
        for row in old.execute("""
            SELECT bbl, unit, complaint_category, disposition_code,
                   disposition_date, raw_description
            FROM dob_complaints
        """):
            yield (
                row['bbl'], row['unit'], row['complaint_category'],
                row['disposition_code'], row['disposition_date'],
                row['raw_description']
            )

    dobc_count = batch_insert(out, "INSERT INTO dob_complaints VALUES (?,?,?,?,?,?)",
                               gen_dobc(), label="dob_complaints")
    out.execute("CREATE INDEX idx_dobc_bbl ON dob_complaints(bbl)")
    out.commit()
    print(f"  {dobc_count:,} DOB complaints")

    # ══════════════════════════════════════════════════════════════════════
    # 8. ECB VIOLATIONS
    # ══════════════════════════════════════════════════════════════════════
    print("\n━━━ 8/18 Violations (ECB) ━━━")
    out.execute("""
        CREATE TABLE violations (
            bbl INTEGER NOT NULL,
            severity TEXT,
            violation_type TEXT,
            issue_date TEXT,
            status TEXT,
            description TEXT,
            penalty REAL,
            balance_due REAL
        )
    """)

    def gen_violations():
        for row in old.execute("""
            SELECT bbl, severity, violation_type, issue_date,
                   ecb_violation_status, violation_description,
                   penality_imposed, balance_due
            FROM ecb_violations
        """):
            yield (
                row['bbl'], row['severity'], row['violation_type'],
                normalize_date(row['issue_date']),
                row['ecb_violation_status'], row['violation_description'],
                parse_float(row['penality_imposed']),
                parse_float(row['balance_due']),
            )

    viol_count = batch_insert(out, "INSERT INTO violations VALUES (?,?,?,?,?,?,?,?)",
                               gen_violations(), label="violations")
    out.execute("CREATE INDEX idx_violations_bbl ON violations(bbl)")
    out.execute("CREATE INDEX idx_violations_date ON violations(issue_date)")
    out.commit()
    print(f"  {viol_count:,} violations")

    # ══════════════════════════════════════════════════════════════════════
    # 9. HPD LITIGATION
    # ══════════════════════════════════════════════════════════════════════
    print("\n━━━ 9/18 Litigation (HPD) ━━━")
    out.execute("""
        CREATE TABLE litigation (
            bbl INTEGER NOT NULL,
            case_type TEXT,
            opened_date TEXT,
            status TEXT
        )
    """)

    def gen_lit():
        for row in old.execute("""
            SELECT bbl, casetype, caseopendate, casestatus
            FROM hpd_litigation
        """):
            yield (row['bbl'], row['casetype'], row['caseopendate'], row['casestatus'])

    lit_count = batch_insert(out, "INSERT INTO litigation VALUES (?,?,?,?)",
                              gen_lit(), label="litigation")
    out.execute("CREATE INDEX idx_lit_bbl ON litigation(bbl)")
    out.commit()
    print(f"  {lit_count:,} litigation records")

    # ══════════════════════════════════════════════════════════════════════
    # 10. HPD CONTACTS
    # ══════════════════════════════════════════════════════════════════════
    print("\n━━━ 10/18 Contacts (HPD) ━━━")

    # Map contact type codes to readable names
    CONTACT_ROLES = {
        'CorporateOwner': 'owner',
        'IndividualOwner': 'owner',
        'HeadOfficer': 'officer',
        'Officer': 'officer',
        'Agent': 'agent',
        'SiteManager': 'site_manager',
        'Superintendent': 'site_manager',
    }

    out.execute("""
        CREATE TABLE contacts (
            bbl INTEGER NOT NULL,
            role TEXT,
            company TEXT,
            first_name TEXT,
            last_name TEXT,
            registered_date TEXT
        )
    """)

    def gen_contacts():
        for row in old.execute("""
            SELECT bbl, contact_type, corporation_name,
                   first_name, last_name, registration_date
            FROM building_contacts
        """):
            role = CONTACT_ROLES.get(row['contact_type'], row['contact_type'])
            yield (
                row['bbl'], role, row['corporation_name'],
                row['first_name'], row['last_name'], row['registration_date']
            )

    contact_count = batch_insert(out, "INSERT INTO contacts VALUES (?,?,?,?,?,?)",
                                  gen_contacts(), label="contacts")
    out.execute("CREATE INDEX idx_contacts_bbl ON contacts(bbl)")
    out.commit()
    print(f"  {contact_count:,} contacts")

    # ══════════════════════════════════════════════════════════════════════
    # 11. RENT STABILIZATION
    # ══════════════════════════════════════════════════════════════════════
    print("\n━━━ 11/18 Rent stabilization ━━━")
    out.execute("""
        CREATE TABLE rent_stabilization (
            bbl INTEGER NOT NULL,
            address TEXT,
            borough TEXT,
            zipcode TEXT,
            stabilized_units INTEGER,
            year INTEGER,
            has_421a BOOLEAN,
            has_j51 BOOLEAN,
            is_coop_condo BOOLEAN
        )
    """)

    def gen_rs():
        for row in old.execute("""
            SELECT bbl, address, borough, zipcode,
                   num_stabilized_units, list_year,
                   has_421a, has_j51, is_coop_condo
            FROM rent_stabilized
        """):
            yield (
                row['bbl'], row['address'], row['borough'], row['zipcode'],
                row['num_stabilized_units'], row['list_year'],
                row['has_421a'], row['has_j51'], row['is_coop_condo']
            )

    rs_count = batch_insert(out, "INSERT INTO rent_stabilization VALUES (?,?,?,?,?,?,?,?,?)",
                             gen_rs(), label="rent_stabilization")
    out.execute("CREATE INDEX idx_rs_bbl ON rent_stabilization(bbl)")
    out.commit()
    print(f"  {rs_count:,} rent stabilization records")

    # ══════════════════════════════════════════════════════════════════════
    # 12. EVICTIONS + CERTIFICATES OF OCCUPANCY
    # ══════════════════════════════════════════════════════════════════════
    print("\n━━━ 12/18 Evictions + Certificates of Occupancy ━━━")

    # Marshal evictions
    out.execute("""
        CREATE TABLE evictions (
            bbl INTEGER,
            address TEXT,
            unit TEXT,
            executed_date TEXT,
            borough TEXT,
            zipcode TEXT
        )
    """)

    def gen_evictions():
        for row in old.execute("""
            SELECT bbl, eviction_address, apartment, executed_date,
                   borough, zipcode
            FROM marshal_evictions
        """):
            yield (
                row['bbl'], row['eviction_address'], row['apartment'],
                row['executed_date'], row['borough'], row['zipcode']
            )

    evict_count = batch_insert(out, "INSERT INTO evictions VALUES (?,?,?,?,?,?)",
                                gen_evictions(), label="evictions")
    out.execute("CREATE INDEX idx_evict_bbl ON evictions(bbl)")
    out.commit()
    print(f"  {evict_count:,} evictions")

    # Certificates of occupancy
    out.execute("""
        CREATE TABLE certificates_of_occupancy (
            bbl INTEGER NOT NULL,
            job_number TEXT,
            issue_date TEXT,
            co_type TEXT,
            existing_occupancy TEXT,
            proposed_occupancy TEXT,
            existing_units INTEGER,
            proposed_units INTEGER
        )
    """)

    if 'certificates_of_occupancy' in old_tables:
        def gen_co():
            for row in old.execute("""
                SELECT bbl, job_number, co_issue_date, co_type,
                       existing_occupancy, proposed_occupancy,
                       existing_dwelling_units, proposed_dwelling_units
                FROM certificates_of_occupancy
            """):
                yield (
                    row['bbl'], row['job_number'], row['co_issue_date'],
                    row['co_type'], row['existing_occupancy'],
                    row['proposed_occupancy'],
                    parse_int(row['existing_dwelling_units']),
                    parse_int(row['proposed_dwelling_units']),
                )

        co_count = batch_insert(out, "INSERT INTO certificates_of_occupancy VALUES (?,?,?,?,?,?,?,?)",
                                gen_co(), label="certificates_of_occupancy")
    else:
        co_count = 0

    out.execute("CREATE INDEX idx_co_bbl ON certificates_of_occupancy(bbl)")
    out.commit()
    print(f"  {co_count:,} certificates of occupancy")

    # ══════════════════════════════════════════════════════════════════════
    # 13. HPD VIOLATIONS (from data_cache)
    # ══════════════════════════════════════════════════════════════════════
    print("\n━━━ 13/18 HPD violations ━━━")
    out.execute("""
        CREATE TABLE hpd_violations (
            bbl INTEGER NOT NULL,
            apartment TEXT,
            class TEXT,
            inspection_date TEXT,
            approved_date TEXT,
            description TEXT,
            status TEXT,
            status_date TEXT,
            violation_status TEXT,
            nov_type TEXT,
            rent_impairing TEXT
        )
    """)

    hpd_viol_count = 0
    if (DATA_CACHE / 'hpd_violations').exists():
        def gen_hpd_violations():
            for rec in iter_data_cache('hpd_violations'):
                boro = rec.get('boroid', '')
                block = rec.get('block', '')
                lot = rec.get('lot', '')
                bbl = make_bbl(boro, block, lot)
                if not bbl or bbl not in valid_bbls:
                    continue
                yield (
                    bbl,
                    rec.get('apartment', ''),
                    rec.get('class', ''),
                    (rec.get('inspectiondate') or '')[:10],
                    (rec.get('approveddate') or '')[:10],
                    rec.get('novdescription', ''),
                    rec.get('currentstatus', ''),
                    (rec.get('currentstatusdate') or '')[:10],
                    rec.get('violationstatus', ''),
                    rec.get('novtype', ''),
                    rec.get('rentimpairing', ''),
                )

        hpd_viol_count = batch_insert(out, "INSERT INTO hpd_violations VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                                       gen_hpd_violations(), label="hpd_violations")
    out.execute("CREATE INDEX idx_hpdv_bbl ON hpd_violations(bbl)")
    out.execute("CREATE INDEX idx_hpdv_date ON hpd_violations(inspection_date)")
    out.execute("CREATE INDEX idx_hpdv_class ON hpd_violations(class)")
    out.commit()
    print(f"  {hpd_viol_count:,} HPD violations")

    # ══════════════════════════════════════════════════════════════════════
    # 14. ROLLING SALES (DOF)
    # ══════════════════════════════════════════════════════════════════════
    print("\n━━━ 14/18 Rolling sales (DOF) ━━━")
    out.execute("""
        CREATE TABLE rolling_sales (
            bbl INTEGER NOT NULL,
            address TEXT,
            zipcode TEXT,
            residential_units INTEGER,
            building_class TEXT,
            year_built INTEGER,
            gross_sqft INTEGER,
            sale_price REAL,
            sale_date TEXT
        )
    """)

    rs_sale_count = 0
    if (DATA_CACHE / 'rolling_sales').exists():
        def gen_rolling_sales():
            for rec in iter_data_cache('rolling_sales'):
                bbl = make_bbl(
                    rec.get('borough', ''),
                    rec.get('block', ''),
                    rec.get('lot', '')
                )
                if not bbl:
                    continue
                yield (
                    bbl,
                    rec.get('address', ''),
                    rec.get('zip_code', ''),
                    parse_int(rec.get('residential_units')),
                    rec.get('building_class_at_present', ''),
                    parse_int(rec.get('year_built')),
                    parse_int(rec.get('gross_square_feet')),
                    parse_float(rec.get('sale_price')),
                    (rec.get('sale_date') or '')[:10],
                )

        rs_sale_count = batch_insert(out, "INSERT INTO rolling_sales VALUES (?,?,?,?,?,?,?,?,?)",
                                      gen_rolling_sales(), label="rolling_sales")
    out.execute("CREATE INDEX idx_rsales_bbl ON rolling_sales(bbl)")
    out.execute("CREATE INDEX idx_rsales_date ON rolling_sales(sale_date)")
    out.commit()
    print(f"  {rs_sale_count:,} rolling sales")

    # ══════════════════════════════════════════════════════════════════════
    # 15. TAX LIENS
    # ══════════════════════════════════════════════════════════════════════
    print("\n━━━ 15/18 Tax liens ━━━")
    out.execute("""
        CREATE TABLE tax_liens (
            bbl INTEGER NOT NULL,
            tax_class TEXT,
            building_class TEXT,
            address TEXT,
            zipcode TEXT,
            water_debt_only TEXT,
            cycle TEXT
        )
    """)

    lien_count = 0
    if (DATA_CACHE / 'tax_liens').exists():
        def gen_tax_liens():
            for rec in iter_data_cache('tax_liens'):
                bbl = make_bbl(
                    rec.get('borough', ''),
                    rec.get('block', ''),
                    rec.get('lot', '')
                )
                if not bbl:
                    continue
                addr_parts = [rec.get('house_number', ''), rec.get('street_name', '')]
                addr = ' '.join(p for p in addr_parts if p).strip()
                yield (
                    bbl,
                    rec.get('tax_class_code', ''),
                    rec.get('building_class', ''),
                    addr,
                    rec.get('zip_code', ''),
                    rec.get('water_debt_only', ''),
                    rec.get('cycle', ''),
                )

        lien_count = batch_insert(out, "INSERT INTO tax_liens VALUES (?,?,?,?,?,?,?)",
                                   gen_tax_liens(), label="tax_liens")
    out.execute("CREATE INDEX idx_liens_bbl ON tax_liens(bbl)")
    out.commit()
    print(f"  {lien_count:,} tax liens")

    # ══════════════════════════════════════════════════════════════════════
    # 16. DOB NOW JOBS
    # ══════════════════════════════════════════════════════════════════════
    print("\n━━━ 16/18 DOB NOW jobs ━━━")
    out.execute("""
        CREATE TABLE dob_now_jobs (
            bbl INTEGER NOT NULL,
            job_type TEXT,
            filing_status TEXT,
            initial_cost REAL,
            existing_units INTEGER,
            proposed_units INTEGER,
            filing_date TEXT,
            status_date TEXT,
            first_permit_date TEXT,
            latitude REAL,
            longitude REAL
        )
    """)

    dob_now_count = 0
    if (DATA_CACHE / 'dob_now_jobs').exists():
        def gen_dob_now():
            for rec in iter_data_cache('dob_now_jobs'):
                bbl_str = rec.get('bbl', '')
                if bbl_str:
                    try:
                        bbl = int(bbl_str)
                    except (ValueError, TypeError):
                        bbl = make_bbl(
                            rec.get('borough', ''),
                            rec.get('block', ''),
                            rec.get('lot', '')
                        )
                else:
                    bbl = make_bbl(
                        rec.get('borough', ''),
                        rec.get('block', ''),
                        rec.get('lot', '')
                    )
                if not bbl:
                    continue
                yield (
                    bbl,
                    rec.get('job_type', ''),
                    rec.get('filing_status', ''),
                    parse_float(rec.get('initial_cost')),
                    parse_int(rec.get('existing_dwelling_units')),
                    parse_int(rec.get('proposed_dwelling_units')),
                    (rec.get('filing_date') or '')[:10],
                    (rec.get('current_status_date') or '')[:10],
                    (rec.get('first_permit_date') or '')[:10],
                    parse_float(rec.get('latitude')),
                    parse_float(rec.get('longitude')),
                )

        dob_now_count = batch_insert(out, "INSERT INTO dob_now_jobs VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                                      gen_dob_now(), label="dob_now_jobs")
    out.execute("CREATE INDEX idx_dobnow_bbl ON dob_now_jobs(bbl)")
    out.execute("CREATE INDEX idx_dobnow_date ON dob_now_jobs(filing_date)")
    out.commit()
    print(f"  {dob_now_count:,} DOB NOW jobs")

    # ══════════════════════════════════════════════════════════════════════
    # 17. VACATE ORDERS
    # ══════════════════════════════════════════════════════════════════════
    print("\n━━━ 17/18 Vacate orders ━━━")
    out.execute("""
        CREATE TABLE vacate_orders (
            bbl INTEGER NOT NULL,
            vacate_reason TEXT,
            vacate_type TEXT,
            effective_date TEXT,
            rescind_date TEXT,
            vacated_units INTEGER,
            latitude REAL,
            longitude REAL
        )
    """)

    vacate_count = 0
    if (DATA_CACHE / 'vacate_orders').exists():
        def gen_vacate():
            for rec in iter_data_cache('vacate_orders'):
                bbl_str = rec.get('bbl', '')
                try:
                    bbl = int(bbl_str) if bbl_str else None
                except (ValueError, TypeError):
                    bbl = None
                if not bbl:
                    continue
                yield (
                    bbl,
                    rec.get('primary_vacate_reason', ''),
                    rec.get('vacate_type', ''),
                    (rec.get('vacate_effective_date') or '')[:10],
                    (rec.get('actual_rescind_date') or '')[:10],
                    parse_int(rec.get('number_of_vacated_units')),
                    parse_float(rec.get('latitude')),
                    parse_float(rec.get('longitude')),
                )

        vacate_count = batch_insert(out, "INSERT INTO vacate_orders VALUES (?,?,?,?,?,?,?,?)",
                                     gen_vacate(), label="vacate_orders")
    out.execute("CREATE INDEX idx_vacate_bbl ON vacate_orders(bbl)")
    out.commit()
    print(f"  {vacate_count:,} vacate orders")

    # ══════════════════════════════════════════════════════════════════════
    # 18. SUBWAY STATIONS
    # ══════════════════════════════════════════════════════════════════════
    print("\n━━━ 18/18 Subway stations ━━━")
    out.execute("""
        CREATE TABLE subway_stations (
            station_id INTEGER,
            complex_id INTEGER,
            name TEXT,
            borough TEXT,
            routes TEXT,
            structure TEXT,
            latitude REAL,
            longitude REAL,
            ada INTEGER
        )
    """)

    station_count = 0
    if (DATA_CACHE / 'subway_stations').exists():
        def gen_stations():
            for rec in iter_data_cache('subway_stations'):
                yield (
                    parse_int(rec.get('station_id')),
                    parse_int(rec.get('complex_id')),
                    rec.get('stop_name', ''),
                    rec.get('borough', ''),
                    rec.get('daytime_routes', ''),
                    rec.get('structure', ''),
                    parse_float(rec.get('gtfs_latitude')),
                    parse_float(rec.get('gtfs_longitude')),
                    parse_int(rec.get('ada')),
                )

        station_count = batch_insert(out, "INSERT INTO subway_stations VALUES (?,?,?,?,?,?,?,?,?)",
                                      gen_stations(), label="subway_stations")
    out.commit()
    print(f"  {station_count:,} subway stations")

    # ══════════════════════════════════════════════════════════════════════
    # FINAL REPORT
    # ══════════════════════════════════════════════════════════════════════
    print()
    print("=" * 60)
    print("  VAYO CLEAN DATABASE — BUILD COMPLETE")
    print("=" * 60)

    tables = [
        'buildings', 'sales', 'hpd_complaints', 'service_requests',
        'permits', 'dob_complaints', 'violations', 'litigation',
        'contacts', 'rent_stabilization', 'evictions',
        'certificates_of_occupancy',
        'hpd_violations', 'rolling_sales', 'tax_liens',
        'dob_now_jobs', 'vacate_orders', 'subway_stations',
        '_bin_map',
    ]
    for table in tables:
        count = out.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]
        print(f"  {table:<30} {count:>12,}")

    size_mb = os.path.getsize(str(OUT_DB)) / (1024 * 1024)
    elapsed = time.time() - t0
    print(f"\n  Database size: {size_mb:,.0f} MB")
    print(f"  Build time:    {elapsed:.1f}s")
    print(f"  ACRIS source:  {acris_source}")
    print(f"\n  Output: {OUT_DB}")

    old.close()
    out.close()


if __name__ == '__main__':
    main()

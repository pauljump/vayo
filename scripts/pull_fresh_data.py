#!/usr/bin/env python3
"""
Pull fresh data from NYC Open Data to fill gaps in vayo_clean.db:
1. DOB Permits (DOB NOW) — strongest signal, currently stale
2. ACRIS full coverage — currently only 0.64% of buildings
3. ACRIS Lis Pendens — missing entirely

Uses curl + Socrata API with pagination.
"""

import subprocess
import json
import sqlite3
import time
import sys

DB = "/Users/pjump/Desktop/projects/vayo/vayo_clean.db"
BATCH = 50000

def fetch(url, label):
    """Fetch JSON from URL using curl."""
    for attempt in range(3):
        try:
            result = subprocess.run(
                ['curl', '-s', '-f', '--max-time', '120', url],
                capture_output=True, text=True
            )
            if result.returncode != 0:
                print(f"  curl error (attempt {attempt+1}): {result.stderr[:200]}")
                time.sleep(5)
                continue
            data = json.loads(result.stdout)
            return data
        except json.JSONDecodeError as e:
            print(f"  JSON error: {e}")
            time.sleep(5)
    return []

def fetch_all(base_url, label, max_records=None):
    """Paginate through a Socrata endpoint."""
    all_rows = []
    offset = 0
    sep = '&' if '?' in base_url else '?'

    while True:
        url = f"{base_url}{sep}$limit={BATCH}&$offset={offset}"
        print(f"  [{label}] offset {offset:,}...", end=' ', flush=True)
        data = fetch(url, label)
        print(f"{len(data):,} rows")

        if not data:
            break
        all_rows.extend(data)
        if max_records and len(all_rows) >= max_records:
            all_rows = all_rows[:max_records]
            break
        if len(data) < BATCH:
            break
        offset += BATCH
        time.sleep(1.5)

    print(f"  [{label}] Total: {len(all_rows):,}")
    return all_rows


def load_valid_bbls(db):
    """Load set of valid building BBLs and block-level index."""
    valid = set(r[0] for r in db.execute("SELECT bbl FROM buildings"))
    blocks = {}
    for bbl in valid:
        s = str(bbl)
        if len(s) == 10:
            key = s[:6]
            if key not in blocks:
                blocks[key] = []
            blocks[key].append(bbl)
    return valid, blocks


def make_bbl(boro, block, lot):
    """Construct integer BBL from components."""
    boro = str(boro).strip()
    block = str(block).strip()
    lot = str(lot).strip()

    # Map borough names to codes
    boro_map = {'MANHATTAN': '1', 'BRONX': '2', 'BROOKLYN': '3',
                'QUEENS': '4', 'STATEN ISLAND': '5', 'STATEN IS': '5'}
    if boro.upper() in boro_map:
        boro = boro_map[boro.upper()]

    if not boro or not block or not lot:
        return None
    try:
        return int(f"{boro}{block.zfill(5)}{lot.zfill(4)[-4:]}")
    except (ValueError, TypeError):
        return None


def pull_dob_now(db):
    """Pull DOB NOW permits — the newer system with fresh data."""
    print("\n" + "=" * 70)
    print("  PULLING DOB NOW PERMITS")
    print("=" * 70)

    valid_bbls, _ = load_valid_bbls(db)

    # DOB NOW has current_status_date for dates and bbl field directly
    data = fetch_all(
        "https://data.cityofnewyork.us/resource/w9ak-ipjd.json?$where=current_status_date>'2022-01-01T00:00:00'",
        "DOB-NOW"
    )

    # Also get records without dates (many have null dates)
    data2 = fetch_all(
        "https://data.cityofnewyork.us/resource/w9ak-ipjd.json?$where=current_status_date IS NULL",
        "DOB-NOW-nodate"
    )
    data.extend(data2)

    # Also pull legacy permits (all available)
    print()
    legacy = fetch_all(
        "https://data.cityofnewyork.us/resource/ic3t-wcy2.json",
        "DOB-Legacy"
    )

    db.execute("DELETE FROM dob_permits")
    inserted = 0
    seen = set()

    for row in data:
        bbl = None
        if row.get('bbl'):
            try:
                bbl = int(float(row['bbl']))
            except (ValueError, TypeError):
                pass
        if not bbl:
            bbl = make_bbl(row.get('borough',''), row.get('block',''), row.get('lot',''))
        if not bbl or bbl not in valid_bbls:
            continue

        job_id = row.get('job_filing_number', '')
        if job_id in seen:
            continue
        seen.add(job_id)

        try:
            cost = float(row.get('initial_cost') or 0)
        except (ValueError, TypeError):
            cost = 0

        date = (row.get('current_status_date') or row.get('filing_date') or '')[:10]

        try:
            ex = int(row.get('existing_dwelling_units') or 0)
        except (ValueError, TypeError):
            ex = 0
        try:
            pr = int(row.get('proposed_dwelling_units') or 0)
        except (ValueError, TypeError):
            pr = 0

        db.execute("""INSERT INTO dob_permits
            (bbl, job_type, job_description, job_status_description,
             latest_action_date, initial_cost, existing_dwelling_units, proposed_dwelling_units)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (bbl, row.get('job_type', ''), (row.get('job_description') or '')[:200],
             row.get('filing_status', ''), date, cost, ex, pr))
        inserted += 1

    for row in legacy:
        bbl = make_bbl(row.get('borough',''), row.get('block',''), row.get('lot',''))
        if not bbl or bbl not in valid_bbls:
            continue

        job_id = row.get('job__', '')
        if job_id in seen:
            continue
        seen.add(job_id)

        try:
            cost = float(row.get('initial_cost') or 0)
        except (ValueError, TypeError):
            cost = 0

        date = (row.get('latest_action_date') or '')[:10]

        try:
            ex = int(row.get('existing_dwelling_units') or 0)
        except (ValueError, TypeError):
            ex = 0
        try:
            pr = int(row.get('proposed_dwelling_units') or 0)
        except (ValueError, TypeError):
            pr = 0

        db.execute("""INSERT INTO dob_permits
            (bbl, job_type, job_description, job_status_description,
             latest_action_date, initial_cost, existing_dwelling_units, proposed_dwelling_units)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (bbl, row.get('job_type', ''), (row.get('job_description') or '')[:200],
             row.get('job_status', ''), date, cost, ex, pr))
        inserted += 1

    db.commit()
    print(f"\n  DOB PERMITS: {inserted:,} inserted")
    return inserted


def pull_acris(db):
    """Pull ACRIS master + legals + parties for recent transactions."""
    print("\n" + "=" * 70)
    print("  PULLING ACRIS DATA (2022+)")
    print("=" * 70)

    valid_bbls, block_index = load_valid_bbls(db)

    # Target doc types — including lis pendens (LPNS)
    target_types = ['DEED', 'MTGE', 'SAT', 'AGMT', 'LPNS', 'RPTT']
    type_filter = ' OR '.join(f"doc_type='{t}'" for t in target_types)

    # Pull master records
    print("\n  --- Master (2022+, key doc types) ---")
    master = fetch_all(
        f"https://data.cityofnewyork.us/resource/bnx9-e6tj.json?$where=recorded_datetime>'2022-01-01T00:00:00' AND ({type_filter})",
        "Master"
    )

    master_by_id = {}
    for m in master:
        did = m.get('document_id', '')
        if did:
            master_by_id[did] = m
    print(f"  {len(master_by_id):,} unique documents")

    # Pull legals — use good_through_date to filter to recent
    print("\n  --- Legals (property linkage) ---")
    legals = fetch_all(
        "https://data.cityofnewyork.us/resource/8h5j-fqxa.json?$where=good_through_date>'2022-01-01T00:00:00'",
        "Legals"
    )

    # Map document_id -> BBL
    doc_bbl = {}
    doc_unit = {}
    for leg in legals:
        did = leg.get('document_id', '')
        if did not in master_by_id:
            continue

        boro = str(leg.get('borough', '')).strip()
        block = str(leg.get('block', '')).strip()
        lot = str(leg.get('lot', '')).strip()
        bbl = make_bbl(boro, block, lot)
        if not bbl:
            continue

        # Direct match
        if bbl in valid_bbls:
            doc_bbl[did] = bbl
            doc_unit[did] = leg.get('unit', '')
            continue

        # Condo lot -> building resolution
        block_key = f"{boro}{block.zfill(5)}"
        candidates = block_index.get(block_key, [])
        if candidates:
            doc_bbl[did] = candidates[0]
            doc_unit[did] = leg.get('unit', '')

    print(f"  Matched {len(doc_bbl):,} documents to buildings")

    # Pull parties
    print("\n  --- Parties (buyer/seller) ---")
    parties = fetch_all(
        "https://data.cityofnewyork.us/resource/636b-3b5g.json?$where=good_through_date>'2022-01-01T00:00:00'",
        "Parties"
    )

    party_by_doc = {}
    for p in parties:
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

    print(f"  Matched parties for {len(party_by_doc):,} documents")

    # Insert into DB
    db.execute("DELETE FROM acris_transactions")
    inserted = 0

    for did, m in master_by_id.items():
        if did not in doc_bbl:
            continue

        bbl = doc_bbl[did]
        unit = doc_unit.get(did, '')
        pi = party_by_doc.get(did, {'sellers': [], 'buyers': []})

        try:
            amt = float(m.get('doc_amount') or m.get('document_amt') or 0)
        except (ValueError, TypeError):
            amt = 0

        db.execute("""INSERT INTO acris_transactions
            (document_id, bbl, unit, doc_type, document_date,
             recorded_datetime, document_amt, party_seller, party_buyer)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (did, bbl, unit, m.get('doc_type', ''),
             (m.get('document_date') or m.get('doc_date') or '')[:10],
             (m.get('recorded_datetime') or m.get('recorded_filed') or '')[:10],
             amt,
             '; '.join(pi['sellers']),
             '; '.join(pi['buyers'])))
        inserted += 1

    db.commit()
    print(f"\n  ACRIS: {inserted:,} transactions")
    return inserted


def main():
    db = sqlite3.connect(DB)

    print("=" * 70)
    print("  VAYO DATA REFRESH")
    print("=" * 70)

    dob_count = pull_dob_now(db)
    acris_count = pull_acris(db)

    # Summary
    print("\n" + "=" * 70)
    print("  RESULTS")
    print("=" * 70)

    total = db.execute("SELECT COUNT(*) FROM buildings").fetchone()[0]
    dob_bbls = db.execute("SELECT COUNT(DISTINCT bbl) FROM dob_permits").fetchone()[0]
    acris_bbls = db.execute("SELECT COUNT(DISTINCT bbl) FROM acris_transactions").fetchone()[0]
    lpen = db.execute("SELECT COUNT(*) FROM acris_transactions WHERE doc_type IN ('LPNS','LPEN')").fetchone()[0]
    dob_max = db.execute("SELECT MAX(latest_action_date) FROM dob_permits").fetchone()[0]
    acris_max = db.execute("SELECT MAX(recorded_datetime) FROM acris_transactions").fetchone()[0]

    print(f"  Buildings:        {total:,}")
    print(f"  DOB permits:      {dob_count:,} records, {dob_bbls:,} BBLs ({dob_bbls/total*100:.1f}%)")
    print(f"  ACRIS:            {acris_count:,} records, {acris_bbls:,} BBLs ({acris_bbls/total*100:.1f}%)")
    print(f"  Lis Pendens:      {lpen:,}")
    print(f"  DOB latest date:  {dob_max}")
    print(f"  ACRIS latest:     {acris_max}")

    db.close()

if __name__ == '__main__':
    main()

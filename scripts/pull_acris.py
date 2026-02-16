#!/usr/bin/env python3
"""
Pull ACRIS data with a single date filter (AND clauses time out on Socrata).
Uses $order + $offset pagination from most recent backwards.
"""

import subprocess
import json
import sqlite3
import time
import os
import urllib.parse
from collections import defaultdict

DB = "/Users/pjump/Desktop/projects/vayo/vayo_clean.db"
CACHE_DIR = "/Users/pjump/Desktop/projects/vayo/acris_cache"
BATCH = 50000

def fetch(url):
    for attempt in range(5):
        result = subprocess.run(
            ['curl', '-s', '--max-time', '300', url],
            capture_output=True, text=True
        )
        if result.returncode == 0 and result.stdout.strip().startswith('['):
            try:
                return json.loads(result.stdout)
            except json.JSONDecodeError:
                pass
        wait = 10 * (attempt + 1)
        print(f"    retry {attempt+1} (wait {wait}s)...", flush=True)
        time.sleep(wait)
    return []

def build_url(base, params):
    """Build a properly URL-encoded Socrata API URL."""
    return f"{base}?{urllib.parse.urlencode(params)}"

def fetch_all(base, query_params, label):
    """Paginate through a Socrata endpoint with proper URL encoding."""
    all_rows = []
    offset = 0
    while True:
        params = {**query_params, '$limit': BATCH, '$offset': offset}
        url = build_url(base, params)
        print(f"  [{label}] offset {offset:,}...", end=' ', flush=True)
        data = fetch(url)
        print(f"{len(data):,}", flush=True)
        if not data:
            break
        all_rows.extend(data)
        if len(data) < BATCH:
            break
        offset += BATCH
        time.sleep(2)
    print(f"  [{label}] Total: {len(all_rows):,}")
    return all_rows

def make_bbl(boro, block, lot):
    boro = str(boro).strip()
    block = str(block).strip()
    lot = str(lot).strip()
    if not boro or not block or not lot:
        return None
    try:
        return int(f"{boro}{block.zfill(5)}{lot.zfill(4)[-4:]}")
    except (ValueError, TypeError):
        return None

def main():
    os.makedirs(CACHE_DIR, exist_ok=True)
    db = sqlite3.connect(DB)

    valid_bbls = set(r[0] for r in db.execute("SELECT bbl FROM buildings"))
    block_index = defaultdict(list)
    for bbl in valid_bbls:
        s = str(bbl)
        if len(s) == 10:
            block_index[s[:6]].append(bbl)
    print(f"Loaded {len(valid_bbls):,} buildings, {len(block_index):,} blocks")

    target_types = {'DEED', 'MTGE', 'SAT', 'AGMT', 'LPNS'}

    # Pull Master with single > date filter (AND clauses fail on Socrata)
    cache_master = f"{CACHE_DIR}/master_2022_all.json"
    if os.path.exists(cache_master):
        print("\nLoading cached master...")
        with open(cache_master) as f:
            raw_master = json.load(f)
    else:
        print("\nPulling Master records (2022+)...")
        raw_master = fetch_all(
            "https://data.cityofnewyork.us/resource/bnx9-e6tj.json",
            {'$where': "recorded_datetime>'2022-01-01T00:00:00.000'",
             '$order': 'recorded_datetime DESC'},
            "Master"
        )
        if raw_master:
            print(f"  Saving cache ({len(raw_master):,} records)...")
            with open(cache_master, 'w') as f:
                json.dump(raw_master, f)

    master_by_id = {}
    for m in raw_master:
        did = m.get('document_id', '')
        dt = m.get('doc_type', '')
        if did and dt in target_types:
            master_by_id[did] = m
    print(f"Master: {len(raw_master):,} total -> {len(master_by_id):,} target doc types")

    # Pull Legals
    cache_legals = f"{CACHE_DIR}/legals.json"
    if os.path.exists(cache_legals):
        print("\nLoading cached legals...")
        with open(cache_legals) as f:
            legals = json.load(f)
    else:
        print("\nPulling Legals (2022+)...")
        legals = fetch_all(
            "https://data.cityofnewyork.us/resource/8h5j-fqxa.json",
            {'$where': "good_through_date>'2022-01-01T00:00:00.000'",
             '$order': 'good_through_date DESC'},
            "Legals"
        )
        if legals:
            with open(cache_legals, 'w') as f:
                json.dump(legals, f)
    print(f"Legals: {len(legals):,}")

    # Map documents to BBLs
    doc_bbl = {}
    doc_unit = {}
    for leg in legals:
        did = leg.get('document_id', '')
        if did not in master_by_id:
            continue
        bbl = make_bbl(leg.get('borough',''), leg.get('block',''), leg.get('lot',''))
        if not bbl:
            continue
        if bbl in valid_bbls:
            doc_bbl[did] = bbl
            doc_unit[did] = leg.get('unit', '')
        else:
            block_key = f"{str(leg.get('borough','')).strip()}{str(leg.get('block','')).strip().zfill(5)}"
            candidates = block_index.get(block_key, [])
            if candidates:
                doc_bbl[did] = candidates[0]
                doc_unit[did] = leg.get('unit', '')
    print(f"Matched {len(doc_bbl):,} documents to buildings")

    # Pull Parties
    cache_parties = f"{CACHE_DIR}/parties.json"
    if os.path.exists(cache_parties):
        print("\nLoading cached parties...")
        with open(cache_parties) as f:
            parties = json.load(f)
    else:
        print("\nPulling Parties (2022+)...")
        parties = fetch_all(
            "https://data.cityofnewyork.us/resource/636b-3b5g.json",
            {'$where': "good_through_date>'2022-01-01T00:00:00.000'",
             '$order': 'good_through_date DESC'},
            "Parties"
        )
        if parties:
            with open(cache_parties, 'w') as f:
                json.dump(parties, f)
    print(f"Parties: {len(parties):,}")

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
    print(f"Matched parties for {len(party_by_doc):,} documents")

    # Insert
    db.execute("DELETE FROM acris_transactions")
    inserted = 0
    for did, m in master_by_id.items():
        if did not in doc_bbl:
            continue
        bbl = doc_bbl[did]
        unit = doc_unit.get(did, '')
        pi = party_by_doc.get(did, {'sellers': [], 'buyers': []})
        try:
            amt = float(m.get('document_amt') or 0)
        except (ValueError, TypeError):
            amt = 0
        db.execute("""INSERT INTO acris_transactions
            (document_id, bbl, unit, doc_type, document_date,
             recorded_datetime, document_amt, party_seller, party_buyer)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (did, bbl, unit, m.get('doc_type', ''),
             (m.get('document_date') or '')[:10],
             (m.get('recorded_datetime') or '')[:10],
             amt,
             '; '.join(pi['sellers']),
             '; '.join(pi['buyers'])))
        inserted += 1
    db.commit()

    # Summary
    acris_bbls = db.execute("SELECT COUNT(DISTINCT bbl) FROM acris_transactions").fetchone()[0]
    lpen = db.execute("SELECT COUNT(*) FROM acris_transactions WHERE doc_type='LPNS'").fetchone()[0]
    by_type = db.execute("SELECT doc_type, COUNT(*) FROM acris_transactions GROUP BY doc_type ORDER BY COUNT(*) DESC").fetchall()
    acris_max = db.execute("SELECT MAX(recorded_datetime) FROM acris_transactions").fetchone()[0]

    print(f"\n{'='*70}")
    print(f"  ACRIS REFRESH COMPLETE")
    print(f"{'='*70}")
    print(f"  Total records:   {inserted:,}")
    print(f"  Distinct BBLs:   {acris_bbls:,} ({acris_bbls/len(valid_bbls)*100:.1f}% coverage)")
    print(f"  Lis Pendens:     {lpen:,}")
    print(f"  Latest date:     {acris_max}")
    print(f"\n  By doc type:")
    for row in by_type:
        print(f"    {row[0]}: {row[1]:,}")
    db.close()

if __name__ == '__main__':
    main()

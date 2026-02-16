#!/usr/bin/env python3
"""
Partitioned ACRIS puller — splits by filter (borough/doc_id range) to avoid
slow high-offset pagination. All partitions run in parallel.

Legals: 5 partitions by borough (~22.5M total)
Parties: 12 partitions by document_id range (~46M total)

Usage:
    python3 scripts/pull_acris_partitioned.py [--legals-only] [--parties-only]
"""

import json
import time
import sys
import urllib.parse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import requests

CACHE_DIR = Path("/Users/pjump/Desktop/projects/vayo/acris_cache/full")
BATCH = 50000
MAX_RETRIES = 8

print_lock = threading.Lock()
def tprint(msg):
    with print_lock:
        print(msg, flush=True)

# --- Partition definitions ---

LEGALS_BASE = "https://data.cityofnewyork.us/resource/8h5j-fqxa.json"
LEGALS_SELECT = "document_id,borough,block,lot,unit"

LEGALS_PARTITIONS = [
    {"name": "boro_1", "where": "borough='1'", "expected": 5_500_000},
    {"name": "boro_2", "where": "borough='2'", "expected": 2_500_000},
    {"name": "boro_3", "where": "borough='3'", "expected": 7_400_000},
    {"name": "boro_4", "where": "borough='4'", "expected": 7_100_000},
    {"name": "boro_5", "where": "borough='5'", "expected": 210_000},
]

PARTIES_BASE = "https://data.cityofnewyork.us/resource/636b-3b5g.json"
PARTIES_SELECT = "document_id,party_type,name"

PARTIES_PARTITIONS = [
    {"name": "num_2000_2005", "where": "document_id>='2000' AND document_id<'2005'", "expected": 3_100_000},
    {"name": "num_2005_2010", "where": "document_id>='2005' AND document_id<'2010'", "expected": 6_200_000},
    {"name": "num_2010_2015", "where": "document_id>='2010' AND document_id<'2015'", "expected": 4_950_000},
    {"name": "num_2015_2020", "where": "document_id>='2015' AND document_id<'2020'", "expected": 5_050_000},
    {"name": "num_2020_2025", "where": "document_id>='2020' AND document_id<'2025'", "expected": 4_850_000},
    {"name": "num_2025_plus", "where": "document_id>='2025' AND document_id<'A'", "expected": 950_000},
    {"name": "alpha_BK", "where": "document_id>='B' AND document_id<'C'", "expected": 5_150_000},
    {"name": "alpha_FT_1", "where": "document_id>='FT_1' AND document_id<'FT_2'", "expected": 2_900_000},
    {"name": "alpha_FT_2", "where": "document_id>='FT_2' AND document_id<'FT_3'", "expected": 1_800_000},
    {"name": "alpha_FT_3", "where": "document_id>='FT_3' AND document_id<'FT_4'", "expected": 5_400_000},
    {"name": "alpha_FT_4", "where": "document_id>='FT_4' AND document_id<'FT_5'", "expected": 6_000_000},
]


def fetch_with_retry(url, label=""):
    """Fetch URL with retries and backoff."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, timeout=600)
            if resp.status_code == 200 and resp.text.strip().startswith('['):
                return resp.json()
            elif resp.status_code == 429:
                wait = min(60 * (attempt + 1), 300)
                tprint(f"    [{label}] rate limited, wait {wait}s...")
                time.sleep(wait)
                continue
        except (requests.RequestException, json.JSONDecodeError) as e:
            pass
        wait = min(15 * (attempt + 1), 120)
        tprint(f"    [{label}] retry {attempt+1}/{MAX_RETRIES} (wait {wait}s)...")
        time.sleep(wait)
    return None


def pull_partition(endpoint_name, base_url, select, partition):
    """Pull all records for a single partition. Returns total count."""
    pname = partition["name"]
    where = partition["where"]
    label = f"{endpoint_name}/{pname}"
    cache_dir = CACHE_DIR / f"{endpoint_name}_parts" / pname
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Check for resume
    existing = sorted(cache_dir.glob("batch_*.json"))
    start_batch = 0
    if existing:
        # Check if last batch was partial (meaning complete)
        with open(existing[-1]) as f:
            last_count = len(json.load(f))
        if last_count < BATCH:
            total = sum(len(json.load(open(bf))) for bf in existing)
            tprint(f"  [{label}] already complete: {total:,} records")
            return total
        start_batch = int(existing[-1].stem.split('_')[1]) + 1

    offset = start_batch * BATCH
    batch_num = start_batch
    total = start_batch * BATCH

    if start_batch > 0:
        tprint(f"  [{label}] resuming from batch {start_batch} (offset {offset:,})")
    else:
        tprint(f"  [{label}] starting (expected ~{partition['expected']:,})")

    while True:
        params = {
            '$select': select,
            '$where': where,
            '$order': 'document_id',
            '$limit': BATCH,
            '$offset': offset,
        }
        url = f"{base_url}?{urllib.parse.urlencode(params)}"

        data = fetch_with_retry(url, label)

        if data is None:
            tprint(f"  [{label}] FAILED at batch {batch_num}")
            return total

        count = len(data)

        # Save
        batch_file = cache_dir / f"batch_{batch_num:05d}.json"
        with open(batch_file, 'w') as f:
            json.dump(data, f)

        total += count
        tprint(f"  [{label}] batch {batch_num}: {count:,} (total {total:,})")

        if count < BATCH:
            break

        batch_num += 1
        offset += BATCH
        time.sleep(0.3)

    tprint(f"  [{label}] COMPLETE: {total:,} records")
    return total


def pull_endpoint(endpoint_name, base_url, select, partitions, max_workers=5):
    """Pull all partitions for an endpoint in parallel."""
    tprint(f"\n{'='*70}")
    tprint(f"  {endpoint_name.upper()} — {len(partitions)} partitions, {max_workers} workers")
    tprint(f"{'='*70}")

    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(pull_partition, endpoint_name, base_url, select, p): p["name"]
            for p in partitions
        }
        for fut in as_completed(futures):
            pname = futures[fut]
            try:
                results[pname] = fut.result()
            except Exception as e:
                tprint(f"  [{endpoint_name}/{pname}] ERROR: {e}")
                results[pname] = 0

    grand_total = sum(results.values())
    tprint(f"\n  {endpoint_name.upper()} TOTAL: {grand_total:,}")
    for pname, count in sorted(results.items()):
        tprint(f"    {pname}: {count:,}")
    return grand_total


def main():
    args = set(sys.argv[1:])
    do_legals = '--parties-only' not in args
    do_parties = '--legals-only' not in args

    tprint("=" * 70)
    tprint("  ACRIS PARTITIONED PULL")
    tprint("=" * 70)

    totals = {}

    if do_legals:
        totals['legals'] = pull_endpoint(
            'legals', LEGALS_BASE, LEGALS_SELECT, LEGALS_PARTITIONS, max_workers=5
        )

    if do_parties:
        totals['parties'] = pull_endpoint(
            'parties', PARTIES_BASE, PARTIES_SELECT, PARTIES_PARTITIONS, max_workers=6
        )

    tprint(f"\n{'='*70}")
    tprint(f"  ALL DONE")
    tprint(f"{'='*70}")
    for name, total in totals.items():
        tprint(f"  {name}: {total:,}")

    total_size = sum(f.stat().st_size for f in CACHE_DIR.rglob('*.json'))
    tprint(f"  Total cache: {total_size / (1024**3):.1f} GB")


if __name__ == '__main__':
    main()

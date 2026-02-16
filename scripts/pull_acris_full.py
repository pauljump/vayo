#!/usr/bin/env python3
"""
Pull complete ACRIS history from NYC Open Data (Socrata API).

Three endpoints:
  - Master:  https://data.cityofnewyork.us/resource/bnx9-e6tj.json  (~16.9M)
  - Legals:  https://data.cityofnewyork.us/resource/8h5j-fqxa.json  (~22.5M)
  - Parties: https://data.cityofnewyork.us/resource/636b-3b5g.json  (~46M)

Records are cached as line-delimited JSON (one JSON array per batch file)
to avoid loading everything into memory at once.

Usage:
    python3 scripts/pull_acris_full.py [--resume] [--master-only] [--legals-only] [--parties-only]

Estimated time: 6-10 hours for all three endpoints.
Estimated disk: ~15GB in acris_cache/full/
"""

import subprocess
import json
import time
import os
import sys
import urllib.parse
from pathlib import Path

CACHE_DIR = Path("/Users/pjump/Desktop/projects/vayo/acris_cache/full")
BATCH = 50000
MAX_RETRIES = 8

ENDPOINTS = {
    'master': {
        'url': 'https://data.cityofnewyork.us/resource/bnx9-e6tj.json',
        'select': 'document_id,doc_type,document_amt,document_date,recorded_datetime',
        'order': 'recorded_datetime DESC',
    },
    'legals': {
        'url': 'https://data.cityofnewyork.us/resource/8h5j-fqxa.json',
        'select': 'document_id,borough,block,lot,unit',
        'order': 'document_id DESC',
    },
    'parties': {
        'url': 'https://data.cityofnewyork.us/resource/636b-3b5g.json',
        'select': 'document_id,party_type,name',
        'order': 'document_id DESC',
    },
}

TARGET_DOC_TYPES = {'DEED', 'MTGE', 'SAT', 'AGMT', 'LPNS', 'AL&R'}


def fetch(url):
    """Fetch a single URL with retries."""
    for attempt in range(MAX_RETRIES):
        result = subprocess.run(
            ['curl', '-s', '--max-time', '600', url],
            capture_output=True, text=True
        )
        if result.returncode == 0 and result.stdout.strip().startswith('['):
            try:
                return json.loads(result.stdout)
            except json.JSONDecodeError:
                pass
        wait = min(15 * (attempt + 1), 120)
        print(f"    retry {attempt+1}/{MAX_RETRIES} (wait {wait}s)...", flush=True)
        time.sleep(wait)
    return None


def pull_endpoint(name, config, resume=False):
    """Pull all records from a Socrata endpoint, saving batches to disk."""
    cache_dir = CACHE_DIR / name
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Find resume point
    start_batch = 0
    if resume:
        existing = sorted(cache_dir.glob('batch_*.json'))
        if existing:
            last = existing[-1].stem  # e.g., batch_0050
            start_batch = int(last.split('_')[1]) + 1
            print(f"  Resuming from batch {start_batch} ({start_batch * BATCH:,} offset)")

    offset = start_batch * BATCH
    total_fetched = start_batch * BATCH  # approximate
    batch_num = start_batch

    base_params = {
        '$select': config['select'],
        '$order': config['order'],
        '$limit': BATCH,
    }

    while True:
        params = {**base_params, '$offset': offset}
        url = f"{config['url']}?{urllib.parse.urlencode(params)}"

        print(f"  [{name}] batch {batch_num} (offset {offset:,})...", end=' ', flush=True)
        data = fetch(url)

        if data is None:
            print(f"FAILED after {MAX_RETRIES} retries. Run with --resume to continue.", flush=True)
            return total_fetched

        print(f"{len(data):,} records", flush=True)

        if not data:
            break

        # Save batch to disk
        batch_file = cache_dir / f"batch_{batch_num:05d}.json"
        with open(batch_file, 'w') as f:
            json.dump(data, f)

        total_fetched += len(data)
        batch_num += 1

        if len(data) < BATCH:
            break

        offset += BATCH
        time.sleep(1.5)  # Be nice to the API

    print(f"  [{name}] COMPLETE: {total_fetched:,} records in {batch_num} batches")
    return total_fetched


def count_cached(name):
    """Count total records in cached batches."""
    cache_dir = CACHE_DIR / name
    if not cache_dir.exists():
        return 0
    total = 0
    for f in sorted(cache_dir.glob('batch_*.json')):
        with open(f) as fh:
            total += len(json.load(fh))
    return total


def iter_cached(name):
    """Iterate over all cached records for an endpoint."""
    cache_dir = CACHE_DIR / name
    if not cache_dir.exists():
        return
    for f in sorted(cache_dir.glob('batch_*.json')):
        with open(f) as fh:
            yield from json.load(fh)


def main():
    args = set(sys.argv[1:])
    resume = '--resume' in args

    pull_master = '--master-only' in args or '--legals-only' not in args and '--parties-only' not in args
    pull_legals = '--legals-only' in args or '--master-only' not in args and '--parties-only' not in args
    pull_parties = '--parties-only' in args or '--master-only' not in args and '--legals-only' not in args

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("  ACRIS FULL HISTORY PULL")
    print("=" * 70)
    print(f"  Cache dir: {CACHE_DIR}")
    print(f"  Resume: {resume}")
    print(f"  Target doc types: {', '.join(sorted(TARGET_DOC_TYPES))}")
    print()

    totals = {}

    if pull_master:
        print(f"{'─'*70}")
        print(f"  MASTER (~16.9M records)")
        print(f"{'─'*70}")
        totals['master'] = pull_endpoint('master', ENDPOINTS['master'], resume)

    if pull_legals:
        print(f"\n{'─'*70}")
        print(f"  LEGALS (~22.5M records)")
        print(f"{'─'*70}")
        totals['legals'] = pull_endpoint('legals', ENDPOINTS['legals'], resume)

    if pull_parties:
        print(f"\n{'─'*70}")
        print(f"  PARTIES (~46M records)")
        print(f"{'─'*70}")
        totals['parties'] = pull_endpoint('parties', ENDPOINTS['parties'], resume)

    # Summary
    print(f"\n{'='*70}")
    print(f"  PULL COMPLETE")
    print(f"{'='*70}")
    for name, count in totals.items():
        print(f"  {name:<10} {count:>15,} records")

    # Disk usage
    total_size = sum(f.stat().st_size for f in CACHE_DIR.rglob('*.json'))
    print(f"\n  Total disk: {total_size / (1024**3):.1f} GB")
    print(f"  Cache dir:  {CACHE_DIR}")


if __name__ == '__main__':
    main()

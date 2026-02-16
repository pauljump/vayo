#!/usr/bin/env python3
"""
Parallel ACRIS puller — resumes from existing batches in acris_cache/full/.

Speedups over pull_acris_full.py:
  1. Multiple concurrent workers per endpoint (default 4)
  2. Legals + Parties run simultaneously
  3. Reduced sleep (0.5s vs 1.5s)
  4. Uses requests instead of subprocess curl

Usage:
    python3 scripts/pull_acris_parallel.py [--workers 4]
"""

import json
import time
import sys
import urllib.parse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

try:
    import requests
except ImportError:
    print("Installing requests...")
    import subprocess
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'requests', '-q'])
    import requests

CACHE_DIR = Path("/Users/pjump/Desktop/projects/vayo/acris_cache/full")
BATCH = 50000
MAX_RETRIES = 8
SLEEP = 0.5

ENDPOINTS = {
    'legals': {
        'url': 'https://data.cityofnewyork.us/resource/8h5j-fqxa.json',
        'select': 'document_id,borough,block,lot,unit',
        'order': 'document_id DESC',
        'expected': 22_500_000,
    },
    'parties': {
        'url': 'https://data.cityofnewyork.us/resource/636b-3b5g.json',
        'select': 'document_id,party_type,name',
        'order': 'document_id DESC',
        'expected': 46_000_000,
    },
}

# Thread-safe print
print_lock = threading.Lock()
def tprint(msg):
    with print_lock:
        print(msg, flush=True)

def rate_limited_fetch(name, url):
    """Fetch with retries and backoff."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, timeout=600)
            if resp.status_code == 200 and resp.text.strip().startswith('['):
                return resp.json()
            elif resp.status_code == 429:
                wait = min(30 * (attempt + 1), 180)
                tprint(f"    [{name}] rate limited, wait {wait}s...")
                time.sleep(wait)
                continue
        except (requests.RequestException, json.JSONDecodeError):
            pass
        wait = min(15 * (attempt + 1), 120)
        tprint(f"    [{name}] retry {attempt+1}/{MAX_RETRIES} (wait {wait}s)...")
        time.sleep(wait)
    return None


def fetch_batch(name, config, batch_num):
    """Fetch a single batch and save to disk. Returns (batch_num, count) or None."""
    cache_dir = CACHE_DIR / name
    batch_file = cache_dir / f"batch_{batch_num:05d}.json"

    # Skip if already exists
    if batch_file.exists():
        return batch_num, -1  # already done

    offset = batch_num * BATCH
    params = {
        '$select': config['select'],
        '$order': config['order'],
        '$limit': BATCH,
        '$offset': offset,
    }
    url = f"{config['url']}?{urllib.parse.urlencode(params)}"

    tprint(f"  [{name}] batch {batch_num} (offset {offset:,})...")
    data = rate_limited_fetch(name, url)

    if data is None:
        tprint(f"  [{name}] batch {batch_num} FAILED")
        return None

    if not data:
        tprint(f"  [{name}] batch {batch_num} empty — endpoint exhausted")
        return batch_num, 0

    with open(batch_file, 'w') as f:
        json.dump(data, f)

    tprint(f"  [{name}] batch {batch_num}: {len(data):,} records")
    time.sleep(SLEEP)  # rate limit per worker
    return batch_num, len(data)


def pull_endpoint_parallel(name, config, num_workers=4):
    """Pull remaining batches for an endpoint using parallel workers."""
    cache_dir = CACHE_DIR / name
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Find where we left off
    existing = sorted(cache_dir.glob('batch_*.json'))
    if existing:
        last_batch = int(existing[-1].stem.split('_')[1])
        # Check if the last batch was a full batch (meaning more data exists)
        with open(existing[-1]) as f:
            last_count = len(json.load(f))
        if last_count < BATCH:
            tprint(f"  [{name}] Already complete ({len(existing)} batches)")
            return
        start_batch = last_batch + 1
    else:
        start_batch = 0

    # Estimate how many batches remain
    expected_total = config['expected']
    current_records = start_batch * BATCH
    remaining_records = expected_total - current_records
    estimated_remaining_batches = (remaining_records // BATCH) + 20  # add buffer
    end_batch = start_batch + estimated_remaining_batches

    tprint(f"  [{name}] Resuming from batch {start_batch} ({current_records:,} records done)")
    tprint(f"  [{name}] Estimated ~{remaining_records:,} remaining, ~{estimated_remaining_batches} batches")
    tprint(f"  [{name}] Using {num_workers} workers")

    # Work in chunks: submit num_workers batches at a time, wait for all,
    # then check if any returned < BATCH (meaning we hit the end).
    batch = start_batch
    total_new = 0
    done = False

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        while not done and batch < end_batch:
            chunk_end = min(batch + num_workers, end_batch)
            futures = {
                executor.submit(fetch_batch, name, config, b): b
                for b in range(batch, chunk_end)
            }

            results = {}
            for fut in as_completed(futures):
                result = fut.result()
                b = futures[fut]
                if result is None:
                    tprint(f"  [{name}] batch {b} failed — stopping")
                    done = True
                    break
                results[result[0]] = result[1]

            if done:
                break

            # Check results in order
            for b in range(batch, chunk_end):
                count = results.get(b, -1)
                if count == 0:
                    done = True
                    break
                if count > 0:
                    total_new += count
                if 0 < count < BATCH:
                    done = True
                    break

            batch = chunk_end

    # Count total
    all_batches = sorted(cache_dir.glob('batch_*.json'))
    total = sum(len(json.load(open(f))) for f in all_batches)
    tprint(f"  [{name}] DONE: {total:,} total records in {len(all_batches)} batches (fetched {total_new:,} new)")


def main():
    num_workers = 4
    for i, arg in enumerate(sys.argv[1:]):
        if arg == '--workers' and i + 2 < len(sys.argv):
            num_workers = int(sys.argv[i + 2])

    print("=" * 70)
    print("  ACRIS PARALLEL PULL (legals + parties)")
    print("=" * 70)
    print(f"  Workers per endpoint: {num_workers}")
    print(f"  Sleep between requests: {SLEEP}s")
    print()

    # Run both endpoints in parallel
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(pull_endpoint_parallel, name, config, num_workers): name
            for name, config in ENDPOINTS.items()
        }
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                fut.result()
            except Exception as e:
                print(f"  [{name}] ERROR: {e}")

    print(f"\n{'='*70}")
    print(f"  ALL DONE")
    print(f"{'='*70}")
    for name in ENDPOINTS:
        d = CACHE_DIR / name
        files = sorted(d.glob('batch_*.json'))
        total = sum(len(json.load(open(f))) for f in files)
        print(f"  {name:<10} {total:>15,} records in {len(files)} batches")

    total_size = sum(f.stat().st_size for f in CACHE_DIR.rglob('*.json'))
    print(f"\n  Total disk: {total_size / (1024**3):.1f} GB")


if __name__ == '__main__':
    main()

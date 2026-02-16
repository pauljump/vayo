#!/usr/bin/env python3
"""
VAYO Dataset Puller
===================
Pulls all supplementary datasets from NYC Open Data (and MTA/NY State).
Each dataset is cached as JSON batches in data_cache/<name>/.

Datasets:
  hpd_violations    10.7M   HPD inspector violations (A/B/C severity)
  rolling_sales      80K    DOF arm's-length property sales with real prices
  tax_liens         264K    Properties with unpaid tax liens
  dob_now_jobs      863K    DOB NOW active job filings
  vacate_orders       8K    HPD vacate/repair orders
  subway_stations    ~500   MTA subway station locations with lat/lon

Usage:
    python3 scripts/pull_datasets.py                    # pull all
    python3 scripts/pull_datasets.py hpd_violations     # pull one
    python3 scripts/pull_datasets.py --resume            # resume interrupted pulls
    python3 scripts/pull_datasets.py --list              # show status
"""

import subprocess
import json
import time
import os
import sys
import urllib.parse
from pathlib import Path

CACHE_DIR = Path("/Users/pjump/Desktop/projects/vayo/data_cache")
BATCH = 50000
MAX_RETRIES = 6

# ── Dataset definitions ───────────────────────────────────────────────────
# Each dataset: (name, base_url, select_fields, order_field, estimated_records)

DATASETS = {
    'hpd_violations': {
        'url': 'https://data.cityofnewyork.us/resource/wvxf-dwi5.json',
        'select': 'boroid,block,lot,apartment,class,inspectiondate,approveddate,'
                  'novdescription,currentstatus,currentstatusdate,violationstatus,'
                  'novtype,rentimpairing',
        'order': 'inspectiondate DESC',
        'estimate': '10.7M',
    },
    'rolling_sales': {
        'url': 'https://data.cityofnewyork.us/resource/usep-8jbt.json',
        'select': 'borough,block,lot,address,zip_code,residential_units,'
                  'building_class_at_present,year_built,gross_square_feet,'
                  'sale_price,sale_date',
        'order': 'sale_date DESC',
        'estimate': '80K',
    },
    'tax_liens': {
        'url': 'https://data.cityofnewyork.us/resource/9rz4-mjek.json',
        'select': 'borough,block,lot,tax_class_code,building_class,'
                  'house_number,street_name,zip_code,water_debt_only,cycle',
        'order': 'cycle DESC',
        'estimate': '264K',
    },
    'dob_now_jobs': {
        'url': 'https://data.cityofnewyork.us/resource/w9ak-ipjd.json',
        'select': 'bbl,bin,borough,block,lot,house_no,street_name,job_type,'
                  'filing_status,initial_cost,existing_dwelling_units,'
                  'proposed_dwelling_units,filing_date,current_status_date,'
                  'first_permit_date,latitude,longitude',
        'order': 'filing_date DESC',
        'estimate': '863K',
    },
    'vacate_orders': {
        'url': 'https://data.cityofnewyork.us/resource/tb8q-a3ar.json',
        'select': 'bbl,bin,boro_short_name,house_number,street_name,'
                  'primary_vacate_reason,vacate_type,vacate_effective_date,'
                  'actual_rescind_date,number_of_vacated_units,latitude,longitude',
        'order': 'vacate_effective_date DESC',
        'estimate': '8K',
    },
    'subway_stations': {
        'url': 'https://data.ny.gov/resource/39hk-dx4f.json',
        'select': 'station_id,complex_id,stop_name,borough,daytime_routes,'
                  'structure,gtfs_latitude,gtfs_longitude,ada',
        'order': 'station_id',
        'estimate': '500',
    },
}


def fetch(url):
    """Fetch a URL with retries."""
    for attempt in range(MAX_RETRIES):
        result = subprocess.run(
            ['curl', '-s', '--max-time', '300', url],
            capture_output=True, text=True
        )
        if result.returncode == 0 and result.stdout.strip().startswith('['):
            try:
                return json.loads(result.stdout)
            except json.JSONDecodeError:
                pass
        wait = min(10 * (attempt + 1), 60)
        print(f"    retry {attempt+1}/{MAX_RETRIES} (wait {wait}s)...", flush=True)
        time.sleep(wait)
    return None


def pull_dataset(name, config, resume=False):
    """Pull all records for a dataset."""
    cache_dir = CACHE_DIR / name
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Resume support
    start_batch = 0
    if resume:
        existing = sorted(cache_dir.glob('batch_*.json'))
        if existing:
            last = existing[-1].stem
            start_batch = int(last.split('_')[1]) + 1
            print(f"  Resuming from batch {start_batch} ({start_batch * BATCH:,} offset)")

    offset = start_batch * BATCH
    batch_num = start_batch
    total = start_batch * BATCH

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
            print(f"FAILED. Run with --resume to continue.", flush=True)
            return total

        print(f"{len(data):,} records", flush=True)

        if not data:
            break

        batch_file = cache_dir / f"batch_{batch_num:05d}.json"
        with open(batch_file, 'w') as f:
            json.dump(data, f)

        total += len(data)
        batch_num += 1

        if len(data) < BATCH:
            break

        offset += BATCH
        time.sleep(1.0)

    print(f"  [{name}] COMPLETE: {total:,} records")
    return total


def show_status():
    """Show current cache status for all datasets."""
    print(f"\n{'Dataset':<20} {'Cached':<12} {'Expected':<10} {'Disk':<10}")
    print("-" * 55)
    for name, config in DATASETS.items():
        cache_dir = CACHE_DIR / name
        if cache_dir.exists():
            total = 0
            size = 0
            for f in cache_dir.glob('batch_*.json'):
                with open(f) as fh:
                    total += len(json.load(fh))
                size += f.stat().st_size
            cached_str = f"{total:,}"
            size_str = f"{size / (1024**2):.0f} MB"
        else:
            cached_str = "-"
            size_str = "-"
        print(f"  {name:<20} {cached_str:<12} {config['estimate']:<10} {size_str}")
    print()


def main():
    args = [a for a in sys.argv[1:] if not a.startswith('-')]
    flags = {a for a in sys.argv[1:] if a.startswith('-')}
    resume = '--resume' in flags

    if '--list' in flags or '--status' in flags:
        show_status()
        return

    # Which datasets to pull
    if args:
        targets = {name: DATASETS[name] for name in args if name in DATASETS}
        unknown = [a for a in args if a not in DATASETS]
        if unknown:
            print(f"Unknown datasets: {', '.join(unknown)}")
            print(f"Available: {', '.join(DATASETS.keys())}")
            sys.exit(1)
    else:
        targets = DATASETS

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("  VAYO DATASET PULLER")
    print("=" * 60)
    print(f"  Pulling: {', '.join(targets.keys())}")
    print(f"  Cache: {CACHE_DIR}")
    print(f"  Resume: {resume}")
    print()

    results = {}
    for name, config in targets.items():
        print(f"{'─'*60}")
        print(f"  {name.upper()} (~{config['estimate']} records)")
        print(f"{'─'*60}")
        results[name] = pull_dataset(name, config, resume)
        print()

    # Summary
    print(f"{'='*60}")
    print(f"  COMPLETE")
    print(f"{'='*60}")
    for name, count in results.items():
        print(f"  {name:<20} {count:>12,}")
    total_size = sum(
        f.stat().st_size
        for f in CACHE_DIR.rglob('*.json')
    )
    print(f"\n  Total disk: {total_size / (1024**2):,.0f} MB")


if __name__ == '__main__':
    main()

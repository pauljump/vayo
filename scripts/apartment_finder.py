#!/usr/bin/env python3
"""
VAYO Apartment Finder v2
========================
Signal-validated scoring engine for NYC residential buildings.

Two scores:
1. GEM SCORE (0-100): How desirable is this building?
   Based on: size, maintenance record, noise, character, rent stabilization

2. AVAILABILITY SCORE (0-100): How likely is a unit to open up soon?
   Based on validated signals with measured predictive lift:
   - DOB renovation permits (16-18x lift)
   - HPD litigation (6-17x lift by type)
   - 311 distress complaint spikes (7-8x lift)
   - Marshal evictions (6x lift)
   - ACRIS agreements (4x lift)
   - Mortgage satisfactions (3x lift)
   - ECB violations with unpaid fines
   - Corporate ownership registration changes (3-4x lift)
   Signal convergence (multiple signals on same BBL) dramatically
   amplifies prediction: 2 signals = 12x, 3+ signals = 23-33x lift.

Uses vayo_clean.db (all tables keyed on BBL).
"""

import sqlite3
import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta

DB = "/Users/pjump/Desktop/projects/vayo/vayo_clean.db"

# ── Configuration ──────────────────────────────────────────────────────────
# Override these via command line or import as module
TARGET_ZIPS = None          # None = all NYC, or list like ['10003', '10010']
TARGET_BOROUGH = None       # 'MANHATTAN', 'BROOKLYN', etc.
MIN_UNITS = 4               # Focus on multi-unit buildings
LOOKBACK_MONTHS = 24        # How far back to look for signals
OUTPUT_FILE = None           # JSON output path

def parse_args():
    """Simple arg parsing for CLI use."""
    global TARGET_ZIPS, TARGET_BOROUGH, MIN_UNITS, OUTPUT_FILE
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == '--zips' and i + 1 < len(args):
            TARGET_ZIPS = [z.strip() for z in args[i+1].split(',')]
            i += 2
        elif args[i] == '--borough' and i + 1 < len(args):
            TARGET_BOROUGH = args[i+1].upper()
            i += 2
        elif args[i] == '--min-units' and i + 1 < len(args):
            MIN_UNITS = int(args[i+1])
            i += 2
        elif args[i] == '--output' and i + 1 < len(args):
            OUTPUT_FILE = args[i+1]
            i += 2
        else:
            print(f"Unknown arg: {args[i]}")
            print("Usage: apartment_finder.py [--zips 10003,10010] [--borough MANHATTAN] [--min-units 4] [--output results.json]")
            sys.exit(1)

parse_args()

# Date cutoff for "recent" signals
cutoff_iso = (datetime.now() - timedelta(days=LOOKBACK_MONTHS * 30)).strftime('%Y-%m-%d')

db = sqlite3.connect(DB)
db.row_factory = sqlite3.Row

print("=" * 70)
print("  VAYO APARTMENT FINDER v2 — Signal-Validated Engine")
print("=" * 70)
if TARGET_ZIPS:
    print(f"  Target: ZIP codes {', '.join(TARGET_ZIPS)}")
elif TARGET_BOROUGH:
    print(f"  Target: {TARGET_BOROUGH}")
else:
    print(f"  Target: All NYC")
print(f"  Min units: {MIN_UNITS} | Lookback: {LOOKBACK_MONTHS} months")
print(f"  Signal cutoff: {cutoff_iso}")
print()

# ═══════════════════════════════════════════════════════════════════════════
# STEP 1: Load target buildings
# ═══════════════════════════════════════════════════════════════════════════
print("[1/9] Loading buildings...")

where_clauses = ["units_residential >= ?", "residential_area > 0"]
params = [MIN_UNITS]

if TARGET_ZIPS:
    where_clauses.append(f"zipcode IN ({','.join('?' * len(TARGET_ZIPS))})")
    params.extend(TARGET_ZIPS)
elif TARGET_BOROUGH:
    # Support both full names and 2-letter codes
    borough_map = {
        'MANHATTAN': 'MN', 'BROOKLYN': 'BK', 'BRONX': 'BX',
        'QUEENS': 'QN', 'STATEN ISLAND': 'SI',
        'MN': 'MN', 'BK': 'BK', 'BX': 'BX', 'QN': 'QN', 'SI': 'SI',
    }
    boro_code = borough_map.get(TARGET_BOROUGH, TARGET_BOROUGH)
    where_clauses.append("borough = ?")
    params.append(boro_code)

query = f"""
    SELECT bbl, address, zipcode, borough,
           year_built, num_floors, units_residential, units_total,
           building_class, owner_name, residential_area, building_area,
           assessed_total, zoning, lot_area, avg_unit_sqft, assessed_per_unit
    FROM buildings
    WHERE {' AND '.join(where_clauses)}
"""

buildings = {}
for row in db.execute(query, params):
    b = dict(row)
    buildings[b['bbl']] = b

print(f"  {len(buildings):,} buildings loaded")
target_bbls = set(buildings.keys())

# ═══════════════════════════════════════════════════════════════════════════
# STEP 2: HPD Complaints (quality signal for gem score)
# ═══════════════════════════════════════════════════════════════════════════
print("[2/9] Loading HPD complaints...")

complaint_counts = defaultdict(int)
recent_complaints = defaultdict(int)

# Total complaints per BBL
for row in db.execute("SELECT bbl, COUNT(*) as cnt FROM hpd_complaints GROUP BY bbl"):
    if row['bbl'] in target_bbls:
        complaint_counts[row['bbl']] = row['cnt']

# Recent complaints
for row in db.execute("SELECT bbl, COUNT(*) as cnt FROM hpd_complaints WHERE received_date >= ? GROUP BY bbl", [cutoff_iso]):
    if row['bbl'] in target_bbls:
        recent_complaints[row['bbl']] = row['cnt']

print(f"  {len(complaint_counts):,} buildings with complaints")

# ═══════════════════════════════════════════════════════════════════════════
# STEP 3: 311 Service Requests (quality + availability signal)
# ═══════════════════════════════════════════════════════════════════════════
print("[3/9] Loading 311 data...")

noise_counts = defaultdict(int)
distress_311 = defaultdict(int)

# Noise (quality signal)
for row in db.execute("""
    SELECT bbl, COUNT(*) as cnt FROM service_requests
    WHERE complaint_type LIKE '%Noise%' AND bbl IS NOT NULL
    GROUP BY bbl
"""):
    if row['bbl'] in target_bbls:
        noise_counts[row['bbl']] = row['cnt']

# Distress complaints — validated 7-8x lift (availability signal)
# HEAT/HOT WATER, WATER LEAK, PLUMBING, UNSANITARY, PAINT/PLASTER
for row in db.execute("""
    SELECT bbl, COUNT(*) as cnt FROM service_requests
    WHERE bbl IS NOT NULL
    AND created_date >= ?
    AND (complaint_type LIKE '%HEAT%' OR complaint_type LIKE '%WATER%'
         OR complaint_type LIKE '%PLUMBING%' OR complaint_type LIKE '%UNSANITARY%'
         OR complaint_type LIKE '%PAINT%' OR complaint_type LIKE '%PLASTER%')
    GROUP BY bbl
""", [cutoff_iso]):
    if row['bbl'] in target_bbls:
        distress_311[row['bbl']] = row['cnt']

print(f"  {len(noise_counts):,} buildings with noise | {len(distress_311):,} with distress complaints")

# ═══════════════════════════════════════════════════════════════════════════
# STEP 4: Rent Stabilization (quality signal)
# ═══════════════════════════════════════════════════════════════════════════
print("[4/9] Loading rent stabilization...")

rent_stab = {}
for row in db.execute("""
    SELECT bbl, MAX(stabilized_units) as stab_units,
           MAX(has_421a) as has_421a, MAX(has_j51) as has_j51
    FROM rent_stabilization
    GROUP BY bbl
"""):
    if row['bbl'] in target_bbls:
        rent_stab[row['bbl']] = {
            'stab_units': row['stab_units'] or 0,
            'has_421a': row['has_421a'] or 0,
            'has_j51': row['has_j51'] or 0,
        }

print(f"  {len(rent_stab):,} buildings with rent stabilization")

# ═══════════════════════════════════════════════════════════════════════════
# STEP 5: ACRIS Transactions (availability signals — 3-4x lift)
# ═══════════════════════════════════════════════════════════════════════════
print("[5/9] Loading ACRIS signals...")

acris = defaultdict(lambda: {
    'deeds': 0, 'mortgages': 0, 'satisfactions': 0,
    'agreements': 0, 'estate_deeds': 0, 'llc_deeds': 0,
    'units_traded': set(), 'last_sale_date': None, 'last_sale_amt': 0,
})

for row in db.execute("""
    SELECT bbl, doc_type, recorded_date, amount, buyer, seller, unit
    FROM sales
    WHERE recorded_date >= ?
""", [cutoff_iso]):
    if row['bbl'] not in target_bbls:
        continue
    sig = acris[row['bbl']]
    dt = row['doc_type'] or ''
    buyer = (row['buyer'] or '').upper()
    seller = (row['seller'] or '').upper()
    unit = row['unit'] or ''

    if dt == 'DEED':
        sig['deeds'] += 1
        if unit:
            sig['units_traded'].add(unit)
        try:
            amt = float(row['amount'] or 0)
        except (ValueError, TypeError):
            amt = 0
        rec = row['recorded_date'] or ''
        if rec > (sig['last_sale_date'] or ''):
            sig['last_sale_date'] = rec
            sig['last_sale_amt'] = amt
        if any(kw in buyer or kw in seller for kw in ['ESTATE', 'EXECUTOR', 'ADMINISTRATOR']):
            sig['estate_deeds'] += 1
        if 'LLC' in buyer:
            sig['llc_deeds'] += 1
    elif dt == 'MTGE':
        sig['mortgages'] += 1
    elif dt == 'SAT':
        sig['satisfactions'] += 1
    elif dt == 'AGMT':
        sig['agreements'] += 1

print(f"  {len(acris):,} buildings with ACRIS activity")

# ═══════════════════════════════════════════════════════════════════════════
# STEP 6: DOB Permits — STRONGEST signal (16-18x lift)
# ═══════════════════════════════════════════════════════════════════════════
print("[6/9] Loading DOB permits...")

dob_permits = defaultdict(lambda: {'count': 0, 'max_cost': 0, 'has_alteration': False})

# NOTE: DOB permit data may be stale (last update varies).
# Use all permits if data doesn't cover the lookback window.
dob_max_date = db.execute("SELECT MAX(action_date) FROM permits").fetchone()[0] or ''
if dob_max_date < cutoff_iso:
    print(f"  WARNING: DOB permit data only goes to {dob_max_date[:10]}. Using all permits.")
    dob_cutoff = '2000-01-01'
else:
    dob_cutoff = cutoff_iso

for row in db.execute("""
    SELECT bbl, job_type, estimated_cost
    FROM permits
    WHERE action_date >= ?
""", [dob_cutoff]):
    if row['bbl'] not in target_bbls:
        continue
    p = dob_permits[row['bbl']]
    p['count'] += 1
    try:
        cost = float(row['estimated_cost'] or 0)
    except (ValueError, TypeError):
        cost = 0
    if cost > p['max_cost']:
        p['max_cost'] = cost
    jt = (row['job_type'] or '').upper()
    if 'A' in jt:  # Alteration
        p['has_alteration'] = True

print(f"  {len(dob_permits):,} buildings with DOB permits")

# ═══════════════════════════════════════════════════════════════════════════
# STEP 7: HPD Litigation — strong signal (6-17x lift by type)
# ═══════════════════════════════════════════════════════════════════════════
print("[7/9] Loading HPD litigation...")

hpd_lit = defaultdict(lambda: {'count': 0, 'types': set()})

for row in db.execute("""
    SELECT bbl, case_type FROM litigation
    WHERE opened_date >= ?
""", [cutoff_iso]):
    if row['bbl'] not in target_bbls:
        continue
    h = hpd_lit[row['bbl']]
    h['count'] += 1
    if row['case_type']:
        h['types'].add(row['case_type'])

print(f"  {len(hpd_lit):,} buildings with HPD litigation")

# ═══════════════════════════════════════════════════════════════════════════
# STEP 8: ECB Violations, Marshal Evictions, Management Changes
# ═══════════════════════════════════════════════════════════════════════════
print("[8/9] Loading ECB, evictions, management changes...")

# ECB violations (dates now normalized to ISO format)
ecb = defaultdict(lambda: {'count': 0, 'unpaid': 0.0})
for row in db.execute("""
    SELECT bbl, balance_due FROM violations
    WHERE issue_date >= ?
""", [cutoff_iso]):
    if row['bbl'] not in target_bbls:
        continue
    e = ecb[row['bbl']]
    e['count'] += 1
    try:
        e['unpaid'] += float(row['balance_due'] or 0)
    except (ValueError, TypeError):
        pass

# Marshal evictions (6.3x lift)
evictions = defaultdict(int)
for row in db.execute("""
    SELECT bbl, COUNT(*) as cnt FROM evictions
    WHERE executed_date >= ?
    GROUP BY bbl
""", [cutoff_iso]):
    if row['bbl'] in target_bbls:
        evictions[row['bbl']] = row['cnt']

# Management / ownership changes — detect ACTUAL changes, not routine re-registration
mgmt_changes = {}
owner_history = defaultdict(set)
latest_owner = {}
for row in db.execute("""
    SELECT bbl, company, registered_date
    FROM contacts
    WHERE role = 'owner' AND company IS NOT NULL
    ORDER BY registered_date ASC
"""):
    if row['bbl'] not in target_bbls:
        continue
    bbl = row['bbl']
    name = (row['company'] or '').strip().upper()
    if name:
        owner_history[bbl].add(name)
        latest_owner[bbl] = name

for bbl in owner_history:
    if len(owner_history[bbl]) >= 2:
        mgmt_changes[bbl] = True

print(f"  {len(ecb):,} ECB | {len(evictions):,} evictions | {len(mgmt_changes):,} actual owner changes")

# ═══════════════════════════════════════════════════════════════════════════
# STEP 9: Score every building
# ═══════════════════════════════════════════════════════════════════════════
print("[9/9] Scoring buildings...\n")

scored = []
for bbl, b in buildings.items():
    units = b['units_residential']
    sqft = b['avg_unit_sqft'] or 0

    # ── GEM SCORE (0-100): How good is this building? ─────────────────

    gem = 0

    # Size (0-25)
    if sqft >= 2000: gem += 25
    elif sqft >= 1500: gem += 22
    elif sqft >= 1200: gem += 18
    elif sqft >= 1000: gem += 15
    elif sqft >= 800: gem += 10
    elif sqft >= 600: gem += 5

    # Maintenance (0-25): fewer complaints per unit = better
    cpr = complaint_counts.get(bbl, 0) / units
    rcpr = recent_complaints.get(bbl, 0) / units
    if cpr == 0: gem += 25
    elif cpr < 1: gem += 22
    elif cpr < 3: gem += 18
    elif cpr < 5: gem += 12
    elif cpr < 10: gem += 5
    if rcpr == 0: gem += 5  # bonus for clean recent record

    # Quiet (0-15)
    npr = noise_counts.get(bbl, 0) / units
    if npr == 0: gem += 15
    elif npr < 0.5: gem += 10
    elif npr < 1: gem += 5

    # Character (0-15)
    yr = b['year_built'] or 0
    if 1880 <= yr <= 1945: gem += 8   # prewar
    elif yr >= 2015: gem += 6          # new construction
    if 6 <= units <= 30: gem += 4      # boutique
    if (b['num_floors'] or 0) >= 6: gem += 3

    # Value (0-15): rent stabilized
    is_stab = bbl in rent_stab and rent_stab[bbl]['stab_units'] > 0
    if is_stab:
        gem += 10
        if sqft >= 1000: gem += 5

    gem = min(gem, 100)

    # ── AVAILABILITY SCORE — weighted by validated lift ────────────────

    signals = []       # list of (signal_name, lift, detail_string)
    signal_count = 0   # number of distinct signal categories firing

    # DOB Permits — 16-18x lift (strongest single signal)
    dp = dob_permits.get(bbl)
    if dp:
        cost = dp['max_cost']
        if cost >= 200000:
            signals.append(('DOB permit ($200K+)', 17.7, f"${cost:,.0f} max cost"))
        elif cost >= 50000:
            signals.append(('DOB permit ($50-200K)', 10.2, f"${cost:,.0f} max cost"))
        elif cost < 10000 and cost > 0:
            signals.append(('DOB permit (<$10K)', 10.5, f"${cost:,.0f} — cosmetic pre-sale?"))
        elif dp['has_alteration']:
            signals.append(('DOB alteration', 16.4, f"{dp['count']} permits"))
        else:
            signals.append(('DOB permit', 8.0, f"{dp['count']} permits"))
        signal_count += 1

    # HPD Litigation — 6-17x lift
    hl = hpd_lit.get(bbl)
    if hl:
        types = hl['types']
        if 'TENANT ACTION' in types:
            signals.append(('HPD tenant action', 8.3, f"{hl['count']} cases"))
        elif types:
            signals.append(('HPD litigation', 6.4, ', '.join(sorted(types))))
        else:
            signals.append(('HPD litigation', 6.4, f"{hl['count']} cases"))
        signal_count += 1

    # 311 Distress Complaints — 7-8x lift (normalized by units)
    dc = distress_311.get(bbl, 0)
    dc_per_unit = dc / units if units > 0 else 0
    if dc >= 5 and dc_per_unit >= 0.1:
        signals.append(('311 distress (heavy)', 7.9, f"{dc} complaints ({dc_per_unit:.1f}/unit)"))
        signal_count += 1
    elif dc >= 3 and dc_per_unit >= 0.05:
        signals.append(('311 distress', 7.6, f"{dc} complaints ({dc_per_unit:.1f}/unit)"))
        signal_count += 1

    # Marshal Evictions — 6.3x lift
    ev = evictions.get(bbl, 0)
    if ev > 0:
        signals.append(('Marshal eviction', 6.3, f"{ev} evictions"))
        signal_count += 1

    # ECB Violations with unpaid fines
    ec = ecb.get(bbl)
    if ec and ec['unpaid'] > 0:
        signals.append(('ECB unpaid fines', 5.0, f"${ec['unpaid']:,.0f} owed"))
        signal_count += 1

    # ACRIS Agreements — 4.1x lift
    ac = acris.get(bbl)
    if ac:
        if ac['agreements'] > 0:
            signals.append(('ACRIS agreement', 4.1, f"{ac['agreements']} agreements"))
            signal_count += 1
        if ac['satisfactions'] > 0:
            signals.append(('Mortgage satisfaction', 3.0, f"{ac['satisfactions']} payoffs"))
            signal_count += 1

    # Corporate ownership change — 3-4x lift (actual owner name change, not re-registration)
    if bbl in mgmt_changes:
        signals.append(('Owner changed', 3.3, 'corporate owner name differs in HPD records'))
        signal_count += 1

    # ── Compute availability score from signal convergence ────────────

    if not signals:
        avail = 0
        convergence = 'none'
    else:
        sorted_sigs = sorted(signals, key=lambda x: x[1], reverse=True)
        raw_lift = sum(s[1] for s in sorted_sigs[:3])

        if signal_count >= 3:
            multiplier = 1.5
            convergence = 'strong'
        elif signal_count == 2:
            multiplier = 1.2
            convergence = 'moderate'
        else:
            multiplier = 1.0
            convergence = 'single'

        avail = min(100, round((raw_lift * multiplier / 50) * 100))

    # ── Assemble result ───────────────────────────────────────────────

    traded = len(ac['units_traded']) if ac else 0
    turnover_pct = round(traded / units * 100, 1) if units > 0 else 0

    scored.append({
        'bbl': bbl,
        'address': b['address'],
        'zip': b['zipcode'],
        'borough': b['borough'],
        'built': b['year_built'],
        'floors': b['num_floors'],
        'units': units,
        'avg_sqft': sqft,
        'owner': (b['owner_name'] or '')[:40],
        # gem components
        'complaints_per_unit': round(cpr, 1),
        'recent_complaints_per_unit': round(rcpr, 1),
        'noise_per_unit': round(npr, 1),
        'rent_stabilized': is_stab,
        'stab_units': rent_stab.get(bbl, {}).get('stab_units', 0),
        # availability components
        'signal_count': signal_count,
        'convergence': convergence,
        'signals': [(s[0], s[2]) for s in sorted(signals, key=lambda x: x[1], reverse=True)],
        'units_traded': traded,
        'turnover_pct': turnover_pct,
        'last_sale_date': ac['last_sale_date'] if ac else None,
        'last_sale_amt': ac['last_sale_amt'] if ac else 0,
        # scores
        'gem_score': gem,
        'avail_score': avail,
        'combined': gem + avail,
    })

# ═══════════════════════════════════════════════════════════════════════════
# RESULTS
# ═══════════════════════════════════════════════════════════════════════════

scored.sort(key=lambda x: x['combined'], reverse=True)

# ── Top 50 overall ────────────────────────────────────────────────────────
print("=" * 130)
print("  TOP 50 — Combined Score (Gem + Availability)")
print("=" * 130)
print(f"{'Address':<32} {'Zip':<6} {'Yr':<5} {'Fl':<3} {'Un':<4} {'SqFt':<5} "
      f"{'Cmp/U':<6} {'Stab':<5} "
      f"{'Sig#':<4} {'Conv':<9} "
      f"{'GEM':<5} {'AVAIL':<6} {'TOT':<5} {'Top Signal':<30}")
print("-" * 130)

for s in scored[:50]:
    stab = 'YES' if s['rent_stabilized'] else ''
    top_sig = s['signals'][0][0] if s['signals'] else '-'
    print(f"{s['address']:<32} {s['zip']:<6} {s['built'] or '?':<5} "
          f"{s['floors'] or '?':<3} {s['units']:<4} {s['avg_sqft']:<5} "
          f"{s['complaints_per_unit']:<6} {stab:<5} "
          f"{s['signal_count']:<4} {s['convergence']:<9} "
          f"{s['gem_score']:<5} {s['avail_score']:<6} {s['combined']:<5} "
          f"{top_sig:<30}")

# ── Diamonds: high quality + strong convergence ──────────────────────────
print()
print("=" * 130)
print("  DIAMONDS — Gem >= 60 AND Signal Convergence (2+ signals)")
print("=" * 130)

diamonds = [s for s in scored if s['gem_score'] >= 60 and s['signal_count'] >= 2]
diamonds.sort(key=lambda x: x['combined'], reverse=True)

print(f"{'Address':<32} {'Zip':<6} {'Yr':<5} {'Un':<4} {'SqFt':<5} "
      f"{'GEM':<5} {'AVAIL':<6} {'Sig#':<4} {'Why available?':<60}")
print("-" * 130)

for s in diamonds[:40]:
    why = '; '.join(f"{name}: {detail}" for name, detail in s['signals'][:3])
    print(f"{s['address']:<32} {s['zip']:<6} {s['built'] or '?':<5} "
          f"{s['units']:<4} {s['avg_sqft']:<5} "
          f"{s['gem_score']:<5} {s['avail_score']:<6} {s['signal_count']:<4} "
          f"{why:<60}")

# ── Hottest: strongest convergence regardless of gem ─────────────────────
print()
print("=" * 130)
print("  HOTTEST — Strongest Availability Signal Convergence (3+ signals)")
print("=" * 130)

hottest = [s for s in scored if s['signal_count'] >= 3]
hottest.sort(key=lambda x: x['avail_score'], reverse=True)

print(f"{'Address':<32} {'Zip':<6} {'Yr':<5} {'Un':<4} "
      f"{'GEM':<5} {'AVAIL':<6} {'Sig#':<4} {'Signals':<70}")
print("-" * 130)

for s in hottest[:30]:
    sig_str = ' + '.join(name for name, _ in s['signals'][:4])
    print(f"{s['address']:<32} {s['zip']:<6} {s['built'] or '?':<5} "
          f"{s['units']:<4} "
          f"{s['gem_score']:<5} {s['avail_score']:<6} {s['signal_count']:<4} "
          f"{sig_str:<70}")

# ── Summary ───────────────────────────────────────────────────────────────
print(f"\n{'='*70}")
print(f"  SUMMARY")
print(f"{'='*70}")
print(f"  Buildings scored:        {len(scored):,}")
print(f"  Gem >= 70:               {sum(1 for s in scored if s['gem_score'] >= 70):,}")
print(f"  Any availability signal: {sum(1 for s in scored if s['signal_count'] > 0):,}")
print(f"  2+ signals (convergence):{sum(1 for s in scored if s['signal_count'] >= 2):,}")
print(f"  3+ signals (hot):        {sum(1 for s in scored if s['signal_count'] >= 3):,}")
print(f"  Diamonds (gem>=60 & 2+): {len(diamonds):,}")

# ── Save results ──────────────────────────────────────────────────────────
output = OUTPUT_FILE or "/Users/pjump/Desktop/projects/vayo/results.json"
with open(output, 'w') as f:
    json.dump(scored, f, indent=2, default=str)
print(f"\n  Results saved to: {output}")

# ── Write to building_scores table ───────────────────────────────────────
print(f"\n  Writing scores to building_scores table...", end=' ', flush=True)

db.execute("""
    CREATE TABLE IF NOT EXISTS building_scores (
        bbl INTEGER PRIMARY KEY,
        gem_score INTEGER NOT NULL,
        avail_score INTEGER NOT NULL,
        combined INTEGER NOT NULL,
        signal_count INTEGER NOT NULL,
        convergence TEXT,
        rent_stabilized INTEGER NOT NULL DEFAULT 0,
        stab_units INTEGER NOT NULL DEFAULT 0,
        signals TEXT,
        complaints_per_unit REAL,
        units_traded INTEGER,
        turnover_pct REAL,
        last_sale_date TEXT,
        last_sale_amt REAL,
        scored_at TEXT DEFAULT (datetime('now'))
    )
""")
db.execute("CREATE INDEX IF NOT EXISTS idx_scores_gem ON building_scores(gem_score)")
db.execute("CREATE INDEX IF NOT EXISTS idx_scores_avail ON building_scores(avail_score)")
db.execute("CREATE INDEX IF NOT EXISTS idx_scores_combined ON building_scores(combined)")
db.execute("CREATE INDEX IF NOT EXISTS idx_scores_rs ON building_scores(rent_stabilized)")

batch = []
for s in scored:
    batch.append((
        s['bbl'], s['gem_score'], s['avail_score'], s['combined'],
        s['signal_count'], s['convergence'],
        1 if s['rent_stabilized'] else 0,
        s.get('stab_units', 0),
        json.dumps(s['signals']) if s['signals'] else None,
        s.get('complaints_per_unit', 0),
        s.get('units_traded', 0),
        s.get('turnover_pct', 0),
        s.get('last_sale_date'),
        s.get('last_sale_amt', 0),
    ))

db.executemany("""
    INSERT OR REPLACE INTO building_scores
    (bbl, gem_score, avail_score, combined, signal_count, convergence,
     rent_stabilized, stab_units, signals, complaints_per_unit,
     units_traded, turnover_pct, last_sale_date, last_sale_amt)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""", batch)
db.commit()
print(f"done. {len(batch):,} rows written.")

db.close()

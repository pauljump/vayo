#!/usr/bin/env python3
"""
VAYO Apartment Finder Prototype
================================
Score every residential building in a target area on two dimensions:
1. GEM SCORE: How desirable is this building? (quality, size, quiet, value)
2. AVAILABILITY SCORE: How likely is a unit to become available soon?

Uses only public data to surface what StreetEasy can't show you.
"""

import sqlite3
import json
from collections import defaultdict

DB = "/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"

# Target area zip codes
# Gramercy / Flatiron / Union Square / East Village / Chelsea
TARGET_ZIPS = ['10003', '10010', '10011', '10016', '10001', '10009', '10002', '10014', '10012']
MIN_UNITS = 4  # Focus on multi-unit buildings

db = sqlite3.connect(DB)
db.row_factory = sqlite3.Row

print("=" * 70)
print("  VAYO APARTMENT FINDER — Gramercy & Surroundings")
print("=" * 70)

# ============================================================================
# STEP 1: Load all target buildings from PLUTO
# ============================================================================
print("\n[1/6] Loading buildings...")
buildings = {}
for row in db.execute("""
    SELECT CAST(bbl AS INTEGER) as bbl, address, zipcode, borough,
           yearbuilt, CAST(numfloors AS INTEGER) as numfloors,
           unitsres, unitstotal, bldgclass, ownername,
           resarea, bldgarea, assesstot, zonedist1, lotarea
    FROM pluto
    WHERE zipcode IN ({})
    AND unitsres >= ?
    AND resarea > 0
""".format(','.join('?' * len(TARGET_ZIPS))), TARGET_ZIPS + [MIN_UNITS]):
    b = dict(row)
    b['avg_sqft'] = round(b['resarea'] / b['unitsres']) if b['unitsres'] > 0 else 0
    b['assessed_per_unit'] = round(b['assesstot'] / b['unitsres']) if b['unitsres'] > 0 and b['assesstot'] else 0
    buildings[b['bbl']] = b

print(f"  {len(buildings)} buildings loaded")

# ============================================================================
# STEP 2: HPD Complaint rates (quality signal)
# ============================================================================
print("[2/6] Loading complaint history...")
# complaints use BIN, so bridge via buildings table
complaint_counts = defaultdict(int)
for row in db.execute("""
    SELECT CAST(bl.bbl AS INTEGER) as bbl, COUNT(*) as cnt
    FROM complaints c
    JOIN buildings bl ON bl.bin = c.bin
    WHERE bl.bbl IS NOT NULL
    GROUP BY CAST(bl.bbl AS INTEGER)
"""):
    if row['bbl'] in buildings:
        complaint_counts[row['bbl']] = row['cnt']

# Recent complaints (last 2 years = active problems)
recent_complaints = defaultdict(int)
for row in db.execute("""
    SELECT CAST(bl.bbl AS INTEGER) as bbl, COUNT(*) as cnt
    FROM complaints c
    JOIN buildings bl ON bl.bin = c.bin
    WHERE bl.bbl IS NOT NULL
    AND c.received_date >= '2024-01-01'
    GROUP BY CAST(bl.bbl AS INTEGER)
"""):
    if row['bbl'] in buildings:
        recent_complaints[row['bbl']] = row['cnt']

print(f"  {len(complaint_counts)} buildings with complaints")

# ============================================================================
# STEP 3: 311 noise complaints (livability signal)
# ============================================================================
print("[3/6] Loading 311 noise data...")
noise_counts = defaultdict(int)
for row in db.execute("""
    SELECT CAST(bbl AS INTEGER) as bbl, COUNT(*) as cnt
    FROM nyc_311_complete
    WHERE complaint_type LIKE '%Noise%'
    AND bbl IS NOT NULL
    GROUP BY CAST(bbl AS INTEGER)
"""):
    if row['bbl'] in buildings:
        noise_counts[row['bbl']] = row['cnt']

print(f"  {len(noise_counts)} buildings with noise complaints")

# ============================================================================
# STEP 4: Rent stabilization (value signal)
# ============================================================================
print("[4/6] Loading rent stabilization...")
rent_stab = {}
for row in db.execute("""
    SELECT CAST(bbl AS INTEGER) as bbl, MAX(uc_2017) as stab_2017, MAX(est_2017) as est_2017
    FROM rent_stabilization
    WHERE bbl IS NOT NULL
    GROUP BY CAST(bbl AS INTEGER)
"""):
    if row['bbl'] in buildings:
        rent_stab[row['bbl']] = {
            'stab_units': row['stab_2017'] or 0,
            'est_units': row['est_2017'] or 0
        }

print(f"  {len(rent_stab)} buildings with rent stabilization data")

# ============================================================================
# STEP 5: ACRIS transaction signals (availability prediction)
# ============================================================================
print("[5/6] Loading ACRIS signals...")

# Get all ACRIS activity for target area buildings
# We need to match by boro+block since ACRIS uses condo lot BBLs
acris_signals = defaultdict(lambda: {
    'recent_deeds': 0, 'recent_mortgages': 0, 'recent_satisfactions': 0,
    'lis_pendens': 0, 'estate_transfers': 0, 'llc_buyers': 0,
    'last_sale_date': None, 'last_sale_amount': 0,
    'trust_transfers': 0, 'units_traded_3yr': set()
})

# Build boro_block -> PLUTO BBL mapping for our target buildings
block_to_bbl = defaultdict(list)
for bbl, b in buildings.items():
    bbl_str = str(bbl)
    if len(bbl_str) == 10:
        block_to_bbl[bbl_str[:6]].append(bbl)

# Query ACRIS for target blocks
target_blocks = list(block_to_bbl.keys())
print(f"  Scanning {len(target_blocks)} blocks...")

# Process in chunks
chunk_size = 500
for i in range(0, len(target_blocks), chunk_size):
    chunk = target_blocks[i:i+chunk_size]
    placeholders = ','.join('?' * len(chunk))

    for row in db.execute(f"""
        SELECT
            r.borough || SUBSTR('00000' || r.block, -5, 5) as boro_block,
            r.unit,
            m.doc_type,
            m.recorded_datetime,
            m.document_amt,
            p.name as party_name,
            p.party_type
        FROM acris_real_property r
        JOIN acris_master m ON m.document_id = r.document_id
        LEFT JOIN acris_parties p ON p.document_id = r.document_id
        WHERE r.borough || SUBSTR('00000' || r.block, -5, 5) IN ({placeholders})
        AND m.recorded_datetime >= '2022-01-01'
    """.format(placeholders=placeholders), chunk):

        bb = row['boro_block']
        matched_bbls = block_to_bbl.get(bb, [])
        if not matched_bbls:
            continue
        # Assign to the building with most units (best guess for condos)
        target_bbl = max(matched_bbls, key=lambda x: buildings[x]['unitsres'])
        sig = acris_signals[target_bbl]

        doc_type = row['doc_type'] or ''
        recorded = row['recorded_datetime'] or ''
        party = (row['party_name'] or '').upper()
        party_type = row['party_type'] or ''
        amt_str = row['document_amt'] or '0'
        try:
            amt = float(amt_str)
        except:
            amt = 0

        unit = row['unit'] or ''

        if doc_type == 'DEED':
            sig['recent_deeds'] += 1
            if unit:
                sig['units_traded_3yr'].add(unit)
            if recorded > (sig['last_sale_date'] or ''):
                sig['last_sale_date'] = recorded
                sig['last_sale_amount'] = amt
            if 'LLC' in party and party_type == '2':
                sig['llc_buyers'] += 1
            if 'ESTATE' in party or 'EXECUTOR' in party or 'ADMINISTRATOR' in party:
                sig['estate_transfers'] += 1
            if 'TRUST' in party or 'TRUSTEE' in party or 'TTEE' in party:
                sig['trust_transfers'] += 1

        elif doc_type == 'MTGE':
            sig['recent_mortgages'] += 1
        elif doc_type == 'SAT':
            sig['recent_satisfactions'] += 1
        elif doc_type == 'LPEN':
            sig['lis_pendens'] += 1
        elif doc_type in ('AL&R', 'AALR'):
            sig['recent_mortgages'] += 1

print(f"  {len(acris_signals)} buildings with ACRIS activity")

# ============================================================================
# STEP 6: Score every building
# ============================================================================
print("[6/6] Scoring buildings...\n")

scored = []
for bbl, b in buildings.items():
    units = b['unitsres']

    # --- GEM SCORE (0-100) ---
    gem = 0

    # Size score (0-25): bigger avg sqft = better
    sqft = b['avg_sqft']
    if sqft >= 2000: gem += 25
    elif sqft >= 1500: gem += 22
    elif sqft >= 1200: gem += 18
    elif sqft >= 1000: gem += 15
    elif sqft >= 800: gem += 10
    elif sqft >= 600: gem += 5

    # Maintenance score (0-25): fewer complaints per unit = better
    cpr = complaint_counts.get(bbl, 0) / units
    rcpr = recent_complaints.get(bbl, 0) / units
    if cpr == 0: gem += 25
    elif cpr < 1: gem += 22
    elif cpr < 3: gem += 18
    elif cpr < 5: gem += 12
    elif cpr < 10: gem += 5

    # Bonus for zero recent complaints
    if rcpr == 0: gem += 5

    # Quiet score (0-15): fewer noise complaints = better
    npr = noise_counts.get(bbl, 0) / units
    if npr == 0: gem += 15
    elif npr < 0.5: gem += 10
    elif npr < 1: gem += 5

    # Character score (0-15): prewar, good height, small building
    if b['yearbuilt'] and 1880 <= b['yearbuilt'] <= 1945: gem += 8  # prewar
    elif b['yearbuilt'] and b['yearbuilt'] >= 2015: gem += 6  # new construction
    if 6 <= units <= 30: gem += 4  # boutique size
    if b['numfloors'] and b['numfloors'] >= 6: gem += 3  # good height

    # Value score (0-15): rent stabilized in expensive area = hidden value
    if bbl in rent_stab and rent_stab[bbl]['stab_units'] > 0:
        gem += 10
        if sqft >= 1000: gem += 5  # large + stabilized = jackpot

    # --- AVAILABILITY SCORE (0-100) ---
    avail = 0
    sig = acris_signals.get(bbl, {
        'recent_deeds': 0, 'recent_mortgages': 0, 'recent_satisfactions': 0,
        'lis_pendens': 0, 'estate_transfers': 0, 'llc_buyers': 0,
        'last_sale_date': None, 'last_sale_amount': 0,
        'trust_transfers': 0, 'units_traded_3yr': set()
    })

    # Turnover rate (0-30): what % of units traded recently
    traded = len(sig['units_traded_3yr'])
    turnover_pct = traded / units if units > 0 else 0
    if turnover_pct >= 0.3: avail += 30
    elif turnover_pct >= 0.2: avail += 25
    elif turnover_pct >= 0.1: avail += 18
    elif turnover_pct > 0: avail += 10

    # Mortgage satisfactions (0-20): paid off mortgage often precedes sale
    if sig['recent_satisfactions'] > 2: avail += 20
    elif sig['recent_satisfactions'] > 0: avail += 10

    # Estate/trust signals (0-20): death/estate planning = upcoming availability
    if sig['estate_transfers'] > 0: avail += 20
    elif sig['trust_transfers'] > 0: avail += 15

    # Lis pendens (0-15): financial distress = forced sale
    if sig['lis_pendens'] > 0: avail += 15

    # LLC activity (0-10): investors buy/sell more readily
    if sig['llc_buyers'] > 0: avail += 10

    # Recent deed volume (0-5): active building = more likely to have openings
    if sig['recent_deeds'] > 3: avail += 5

    scored.append({
        'bbl': bbl,
        'address': b['address'],
        'zip': b['zipcode'],
        'built': b['yearbuilt'],
        'floors': b['numfloors'],
        'units': units,
        'avg_sqft': sqft,
        'owner': (b['ownername'] or '')[:35],
        'complaints_per_unit': round(cpr, 1),
        'recent_complaints_per_unit': round(rcpr, 1),
        'noise_per_unit': round(npr, 1),
        'rent_stabilized': bbl in rent_stab and rent_stab[bbl]['stab_units'] > 0,
        'stab_units': rent_stab.get(bbl, {}).get('stab_units', 0),
        'units_traded_3yr': traded,
        'turnover_pct': round(turnover_pct * 100, 1),
        'estate_signals': sig['estate_transfers'] + sig['trust_transfers'],
        'lis_pendens': sig['lis_pendens'],
        'mortgage_sats': sig['recent_satisfactions'],
        'gem_score': min(gem, 100),
        'avail_score': min(avail, 100),
        'combined_score': min(gem, 100) + min(avail, 100),
        'last_sale_date': sig['last_sale_date'],
        'last_sale_amt': sig['last_sale_amount'],
    })

# ============================================================================
# RESULTS
# ============================================================================

# Sort by combined score
scored.sort(key=lambda x: x['combined_score'], reverse=True)

print("=" * 120)
print("  TOP 50 HIDDEN GEMS — Sorted by Gem Score + Availability Score")
print("=" * 120)
print(f"{'Address':<30} {'Zip':<6} {'Built':<5} {'Fl':<3} {'Un':<4} {'AvgSF':<6} "
      f"{'Cmp/U':<6} {'Noise':<6} {'Stab':<5} "
      f"{'Trd3Y':<6} {'Est':<4} {'LisP':<5} "
      f"{'GEM':<5} {'AVAIL':<6} {'TOTAL':<6} {'Owner':<35}")
print("-" * 120)

for s in scored[:50]:
    stab = 'YES' if s['rent_stabilized'] else ''
    print(f"{s['address']:<30} {s['zip']:<6} {s['built'] or '?':<5} {s['floors'] or '?':<3} "
          f"{s['units']:<4} {s['avg_sqft']:<6} "
          f"{s['complaints_per_unit']:<6} {s['noise_per_unit']:<6} {stab:<5} "
          f"{s['units_traded_3yr']:<6} {s['estate_signals']:<4} {s['lis_pendens']:<5} "
          f"{s['gem_score']:<5} {s['avail_score']:<6} {s['combined_score']:<6} "
          f"{s['owner']:<35}")

# High gem, high availability = diamonds
print("\n" + "=" * 120)
print("  DIAMONDS: High Gem Score (>=60) AND High Availability (>=30)")
print("=" * 120)
diamonds = [s for s in scored if s['gem_score'] >= 60 and s['avail_score'] >= 30]
diamonds.sort(key=lambda x: x['combined_score'], reverse=True)

print(f"{'Address':<30} {'Zip':<6} {'Built':<5} {'Un':<4} {'AvgSF':<6} "
      f"{'Cmp/U':<6} {'Stab':<5} {'Trd3Y':<6} {'Estate':<7} "
      f"{'GEM':<5} {'AVAIL':<6} {'Why Available?':<40}")
print("-" * 120)

for s in diamonds[:30]:
    stab = 'YES' if s['rent_stabilized'] else ''
    reasons = []
    if s['units_traded_3yr'] > 0:
        reasons.append(f"{s['turnover_pct']}% turnover")
    if s['estate_signals'] > 0:
        reasons.append("estate/trust activity")
    if s['lis_pendens'] > 0:
        reasons.append("lis pendens (distress)")
    if s['mortgage_sats'] > 0:
        reasons.append(f"{s['mortgage_sats']} mortgages satisfied")
    reason_str = '; '.join(reasons) if reasons else 'building quality only'

    print(f"{s['address']:<30} {s['zip']:<6} {s['built'] or '?':<5} "
          f"{s['units']:<4} {s['avg_sqft']:<6} "
          f"{s['complaints_per_unit']:<6} {stab:<5} {s['units_traded_3yr']:<6} "
          f"{s['estate_signals']:<7} "
          f"{s['gem_score']:<5} {s['avail_score']:<6} {reason_str:<40}")

# Summary stats
print(f"\n--- SUMMARY ---")
print(f"Total buildings scored: {len(scored)}")
print(f"Gem score >= 70: {sum(1 for s in scored if s['gem_score'] >= 70)}")
print(f"Avail score >= 30: {sum(1 for s in scored if s['avail_score'] >= 30)}")
print(f"Diamonds (gem>=60 & avail>=30): {len(diamonds)}")

# Save full results as JSON for later use
output = "/Users/pjump/Desktop/projects/vayo/gramercy_gems.json"
with open(output, 'w') as f:
    json.dump(scored, f, indent=2, default=str)
print(f"\nFull results saved to: {output}")

db.close()

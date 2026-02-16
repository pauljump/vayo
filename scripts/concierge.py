#!/usr/bin/env python3
"""
VAYO — NYC Apartment Concierge
================================
Interactive conversational interface to the Vayo scoring engine.
Feels like talking to someone who knows every building in NYC.

Usage:
    python3 scripts/concierge.py
"""

import sqlite3
import json
import sys
import readline
from collections import defaultdict
from datetime import datetime, timedelta

DB = "/Users/pjump/Desktop/projects/vayo/vayo_clean.db"

# ── Database Setup ─────────────────────────────────────────────────────────

db = sqlite3.connect(DB)
db.row_factory = sqlite3.Row

LOOKBACK_MONTHS = 24
cutoff_iso = (datetime.now() - timedelta(days=LOOKBACK_MONTHS * 30)).strftime('%Y-%m-%d')

# ── Neighborhood / ZIP mapping ─────────────────────────────────────────────

NEIGHBORHOODS = {
    'gramercy': ['10003', '10010'],
    'east village': ['10003', '10009'],
    'west village': ['10014', '10011'],
    'chelsea': ['10001', '10011'],
    'flatiron': ['10010', '10003'],
    'union square': ['10003'],
    'lower east side': ['10002'],
    'les': ['10002'],
    'soho': ['10012', '10013'],
    'noho': ['10012', '10003'],
    'nolita': ['10012'],
    'tribeca': ['10013', '10007'],
    'greenwich village': ['10003', '10011', '10014', '10012'],
    'the village': ['10003', '10011', '10014', '10012'],
    'midtown': ['10016', '10017', '10018', '10019', '10020', '10022', '10036'],
    'murray hill': ['10016'],
    'kips bay': ['10016', '10010'],
    'stuyvesant': ['10009', '10003'],
    'stuy town': ['10009', '10010'],
    'alphabet city': ['10009'],
    'upper east side': ['10021', '10028', '10065', '10075', '10128'],
    'ues': ['10021', '10028', '10065', '10075', '10128'],
    'upper west side': ['10023', '10024', '10025'],
    'uws': ['10023', '10024', '10025'],
    'harlem': ['10026', '10027', '10029', '10030', '10035', '10037', '10039'],
    'williamsburg': ['11206', '11211', '11249'],
    'bushwick': ['11206', '11221', '11237'],
    'bed stuy': ['11205', '11206', '11216', '11221', '11233'],
    'park slope': ['11215', '11217'],
    'prospect heights': ['11217', '11238'],
    'crown heights': ['11213', '11216', '11225', '11233', '11238'],
    'cobble hill': ['11201'],
    'boerum hill': ['11201', '11217'],
    'brooklyn heights': ['11201'],
    'dumbo': ['11201'],
    'fort greene': ['11205', '11217'],
    'clinton hill': ['11205', '11238'],
    'greenpoint': ['11222'],
    'astoria': ['11102', '11103', '11105', '11106'],
    'long island city': ['11101', '11109'],
    'lic': ['11101', '11109'],
    'jackson heights': ['11372', '11373'],
    'sunnyside': ['11104'],
    'woodside': ['11377'],
}

# ── Scoring Functions ──────────────────────────────────────────────────────

def score_building(b, data):
    """Score a single building. Returns dict with scores and signal details."""
    bbl = b['bbl']
    units = b['units_residential']
    sqft = b['avg_unit_sqft'] or 0

    # GEM SCORE
    gem = 0
    gem_reasons = []

    if sqft >= 2000: gem += 25; gem_reasons.append(f"{sqft:,} sqft avg — very spacious")
    elif sqft >= 1500: gem += 22; gem_reasons.append(f"{sqft:,} sqft avg — spacious")
    elif sqft >= 1200: gem += 18; gem_reasons.append(f"{sqft:,} sqft avg — good size")
    elif sqft >= 1000: gem += 15; gem_reasons.append(f"{sqft:,} sqft avg — decent")
    elif sqft >= 800: gem += 10; gem_reasons.append(f"{sqft:,} sqft avg")
    elif sqft >= 600: gem += 5

    cpr = data['complaints'].get(bbl, 0) / units if units > 0 else 0
    rcpr = data['recent_complaints'].get(bbl, 0) / units if units > 0 else 0
    if cpr == 0: gem += 25; gem_reasons.append("zero complaints ever")
    elif cpr < 1: gem += 22; gem_reasons.append(f"very clean record ({cpr:.1f} complaints/unit)")
    elif cpr < 3: gem += 18; gem_reasons.append(f"good maintenance ({cpr:.1f} complaints/unit)")
    elif cpr < 5: gem += 12
    elif cpr < 10: gem += 5
    if rcpr == 0: gem += 5

    npr = data['noise'].get(bbl, 0) / units if units > 0 else 0
    if npr == 0: gem += 15; gem_reasons.append("zero noise complaints")
    elif npr < 0.5: gem += 10; gem_reasons.append("quiet building")
    elif npr < 1: gem += 5

    yr = b['year_built'] or 0
    if 1880 <= yr <= 1945:
        gem += 8; gem_reasons.append(f"prewar ({yr})")
    elif yr >= 2015:
        gem += 6; gem_reasons.append(f"new construction ({yr})")
    if 6 <= units <= 30:
        gem += 4; gem_reasons.append(f"boutique ({units} units)")
    if (b['num_floors'] or 0) >= 6: gem += 3

    is_stab = bbl in data['rent_stab'] and data['rent_stab'][bbl] > 0
    if is_stab:
        gem += 10
        gem_reasons.append(f"rent stabilized ({data['rent_stab'][bbl]} units)")
        if sqft >= 1000: gem += 5
    gem = min(gem, 100)

    # AVAILABILITY SIGNALS
    signals = []
    signal_count = 0

    dp = data['dob_permits'].get(bbl)
    if dp:
        cost = dp['max_cost']
        if cost >= 200000:
            signals.append(('DOB permit ($200K+)', 17.7, f"${cost:,.0f} renovation — major work, likely unit turnover"))
        elif cost >= 50000:
            signals.append(('DOB permit ($50-200K)', 10.2, f"${cost:,.0f} renovation"))
        elif cost < 10000 and cost > 0:
            signals.append(('DOB permit (<$10K)', 10.5, f"${cost:,.0f} — cosmetic work, possible pre-listing prep"))
        elif dp['has_alt']:
            signals.append(('DOB alteration', 16.4, f"{dp['count']} alteration permits filed"))
        else:
            signals.append(('DOB permit', 8.0, f"{dp['count']} permits"))
        signal_count += 1

    hl = data['hpd_lit'].get(bbl)
    if hl:
        if 'TENANT ACTION' in hl['types']:
            signals.append(('HPD tenant action', 8.3, f"tenant lawsuit — {hl['count']} cases. Building under legal pressure."))
        elif hl['types']:
            signals.append(('HPD litigation', 6.4, f"{', '.join(sorted(hl['types']))} — {hl['count']} cases"))
        else:
            signals.append(('HPD litigation', 6.4, f"{hl['count']} cases"))
        signal_count += 1

    dc = data['distress_311'].get(bbl, 0)
    dc_per_unit = dc / units if units > 0 else 0
    if dc >= 5 and dc_per_unit >= 0.1:
        signals.append(('311 distress complaints', 7.9, f"{dc} complaints ({dc_per_unit:.1f}/unit) — heat, water, plumbing issues"))
        signal_count += 1
    elif dc >= 3 and dc_per_unit >= 0.05:
        signals.append(('311 distress complaints', 7.6, f"{dc} complaints — building may be deteriorating"))
        signal_count += 1

    ev = data['evictions'].get(bbl, 0)
    if ev > 0:
        signals.append(('Marshal eviction', 6.3, f"{ev} eviction(s) — forced vacancy creates openings"))
        signal_count += 1

    ec = data['ecb'].get(bbl)
    if ec and ec > 0:
        signals.append(('ECB unpaid fines', 5.0, f"${ec:,.0f} in unpaid fines — financial pressure on owner"))
        signal_count += 1

    ac = data['acris'].get(bbl)
    if ac:
        if ac['agreements'] > 0:
            signals.append(('ACRIS agreement', 4.1, f"{ac['agreements']} pre-sale agreements filed"))
            signal_count += 1
        if ac['satisfactions'] > 0:
            signals.append(('Mortgage satisfied', 3.0, f"{ac['satisfactions']} mortgage(s) paid off — often precedes sale"))
            signal_count += 1

    mc = data['owner_changed'].get(bbl)
    if mc:
        signals.append(('Ownership changed', 3.3, "corporate owner name changed in HPD records"))
        signal_count += 1

    # Compute availability score
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

    traded = len(ac['units_traded']) if ac else 0

    return {
        'bbl': bbl,
        'address': b['address'],
        'zip': b['zipcode'],
        'borough': b['borough'],
        'built': b['year_built'],
        'floors': b['num_floors'],
        'units': units,
        'avg_sqft': sqft,
        'owner': (b['owner_name'] or '')[:50],
        'gem_score': gem,
        'gem_reasons': gem_reasons,
        'avail_score': avail,
        'convergence': convergence,
        'signal_count': signal_count,
        'signals': [(s[0], s[2]) for s in sorted(signals, key=lambda x: x[1], reverse=True)],
        'combined': gem + avail,
        'rent_stabilized': is_stab,
        'stab_units': data['rent_stab'].get(bbl, 0),
        'units_traded': traded,
        'last_sale_date': ac['last_sale_date'] if ac else None,
        'last_sale_amt': ac['last_sale_amt'] if ac else 0,
        'complaints_per_unit': round(cpr, 1),
    }


def load_data(target_bbls):
    """Load all signal data for a set of target BBLs using temp table for fast JOINs."""
    data = {
        'complaints': defaultdict(int),
        'recent_complaints': defaultdict(int),
        'noise': defaultdict(int),
        'distress_311': defaultdict(int),
        'rent_stab': {},
        'acris': defaultdict(lambda: {
            'deeds': 0, 'mortgages': 0, 'satisfactions': 0,
            'agreements': 0, 'units_traded': set(),
            'last_sale_date': None, 'last_sale_amt': 0,
        }),
        'dob_permits': {},
        'hpd_lit': {},
        'ecb': {},
        'evictions': defaultdict(int),
        'owner_changed': {},
    }

    # Create temp table for fast JOINs against indexed BBL columns
    db.execute("DROP TABLE IF EXISTS _target_bbls")
    db.execute("CREATE TEMP TABLE _target_bbls (bbl INTEGER PRIMARY KEY)")
    db.executemany("INSERT INTO _target_bbls VALUES (?)", [(b,) for b in target_bbls])

    for row in db.execute("SELECT c.bbl, COUNT(*) as cnt FROM hpd_complaints c JOIN _target_bbls t ON c.bbl=t.bbl GROUP BY c.bbl"):
        data['complaints'][row['bbl']] = row['cnt']

    for row in db.execute("SELECT c.bbl, COUNT(*) as cnt FROM hpd_complaints c JOIN _target_bbls t ON c.bbl=t.bbl WHERE c.received_date >= ? GROUP BY c.bbl", [cutoff_iso]):
        data['recent_complaints'][row['bbl']] = row['cnt']

    for row in db.execute("SELECT s.bbl, COUNT(*) as cnt FROM service_requests s JOIN _target_bbls t ON s.bbl=t.bbl WHERE s.complaint_type LIKE '%Noise%' GROUP BY s.bbl"):
        data['noise'][row['bbl']] = row['cnt']

    for row in db.execute("""
        SELECT s.bbl, COUNT(*) as cnt FROM service_requests s
        JOIN _target_bbls t ON s.bbl=t.bbl
        WHERE s.created_date >= ?
        AND (s.complaint_type LIKE '%HEAT%' OR s.complaint_type LIKE '%WATER%'
             OR s.complaint_type LIKE '%PLUMBING%' OR s.complaint_type LIKE '%UNSANITARY%'
             OR s.complaint_type LIKE '%PAINT%' OR s.complaint_type LIKE '%PLASTER%')
        GROUP BY s.bbl
    """, [cutoff_iso]):
        data['distress_311'][row['bbl']] = row['cnt']

    for row in db.execute("SELECT r.bbl, MAX(r.stabilized_units) as su FROM rent_stabilization r JOIN _target_bbls t ON r.bbl=t.bbl GROUP BY r.bbl"):
        data['rent_stab'][row['bbl']] = row['su'] or 0

    for row in db.execute("SELECT a.bbl, a.doc_type, a.recorded_date, a.amount, a.buyer, a.unit FROM sales a JOIN _target_bbls t ON a.bbl=t.bbl WHERE a.recorded_date >= ?", [cutoff_iso]):
        ac = data['acris'][row['bbl']]
        dt = row['doc_type'] or ''
        if dt == 'DEED':
            ac['deeds'] += 1
            if row['unit']: ac['units_traded'].add(row['unit'])
            try: amt = float(row['amount'] or 0)
            except: amt = 0
            rec = row['recorded_date'] or ''
            if rec > (ac['last_sale_date'] or ''):
                ac['last_sale_date'] = rec
                ac['last_sale_amt'] = amt
        elif dt == 'MTGE': ac['mortgages'] += 1
        elif dt == 'SAT': ac['satisfactions'] += 1
        elif dt == 'AGMT': ac['agreements'] += 1

    dob_max = db.execute("SELECT MAX(action_date) FROM permits").fetchone()[0] or ''
    dob_cutoff = cutoff_iso if dob_max >= cutoff_iso else '2000-01-01'
    dob_permits = defaultdict(lambda: {'count': 0, 'max_cost': 0, 'has_alt': False})
    for row in db.execute("SELECT d.bbl, d.job_type, d.estimated_cost FROM permits d JOIN _target_bbls t ON d.bbl=t.bbl WHERE d.action_date >= ?", [dob_cutoff]):
        p = dob_permits[row['bbl']]
        p['count'] += 1
        try: cost = float(row['estimated_cost'] or 0)
        except: cost = 0
        if cost > p['max_cost']: p['max_cost'] = cost
        if 'A' in (row['job_type'] or '').upper(): p['has_alt'] = True
    data['dob_permits'] = dict(dob_permits)

    hpd_lit = defaultdict(lambda: {'count': 0, 'types': set()})
    for row in db.execute("SELECT h.bbl, h.case_type FROM litigation h JOIN _target_bbls t ON h.bbl=t.bbl WHERE h.opened_date >= ?", [cutoff_iso]):
        h = hpd_lit[row['bbl']]
        h['count'] += 1
        if row['case_type']: h['types'].add(row['case_type'])
    data['hpd_lit'] = dict(hpd_lit)

    ecb = defaultdict(float)
    for row in db.execute("SELECT e.bbl, e.balance_due FROM violations e JOIN _target_bbls t ON e.bbl=t.bbl WHERE e.issue_date >= ?", [cutoff_iso]):
        try: ecb[row['bbl']] += float(row['balance_due'] or 0)
        except: pass
    data['ecb'] = {k: v for k, v in ecb.items() if v > 0}

    for row in db.execute("SELECT m.bbl, COUNT(*) as cnt FROM evictions m JOIN _target_bbls t ON m.bbl=t.bbl WHERE m.executed_date >= ? GROUP BY m.bbl", [cutoff_iso]):
        data['evictions'][row['bbl']] = row['cnt']

    owner_history = defaultdict(set)
    for row in db.execute("SELECT bc.bbl, bc.company FROM contacts bc JOIN _target_bbls t ON bc.bbl=t.bbl WHERE bc.role='owner' AND bc.company IS NOT NULL"):
        name = (row['company'] or '').strip().upper()
        if name: owner_history[row['bbl']].add(name)
    data['owner_changed'] = {bbl: True for bbl, names in owner_history.items() if len(names) >= 2}

    db.execute("DROP TABLE IF EXISTS _target_bbls")
    return data


def search_buildings(zips, min_units=4):
    """Load buildings for given zip codes."""
    placeholders = ','.join('?' * len(zips))
    rows = db.execute(f"""
        SELECT bbl, address, zipcode, borough, year_built, num_floors,
               units_residential, owner_name, avg_unit_sqft
        FROM buildings
        WHERE zipcode IN ({placeholders}) AND units_residential >= ? AND residential_area > 0
    """, zips + [min_units]).fetchall()
    return {r['bbl']: dict(r) for r in rows}


def lookup_building(query):
    """Find a building by address fragment or BBL."""
    query = query.strip()
    if query.isdigit() and len(query) == 10:
        row = db.execute("SELECT * FROM buildings WHERE bbl = ?", [int(query)]).fetchone()
        if row: return {row['bbl']: dict(row)}
    # Address search
    rows = db.execute(
        "SELECT bbl, address, zipcode, borough, year_built, num_floors, units_residential, owner_name, avg_unit_sqft FROM buildings WHERE address LIKE ? AND units_residential > 0 LIMIT 20",
        [f"%{query.upper()}%"]
    ).fetchall()
    return {r['bbl']: dict(r) for r in rows}


# ── Display Functions ──────────────────────────────────────────────────────

def bar(score, width=20):
    filled = round(score / 100 * width)
    return '█' * filled + '░' * (width - filled)

def show_building_detail(s):
    """Show a detailed building report."""
    print()
    print(f"  ┌{'─'*68}┐")
    print(f"  │  {s['address']:<64} │")
    print(f"  │  {s['borough'] or ''} {s['zip']}  ·  Built {s['built'] or '?'}  ·  {s['floors'] or '?'} floors  ·  {s['units']} units{' '*20}│"[:71] + "│")
    print(f"  │  Owner: {s['owner']:<58}│")
    print(f"  ├{'─'*68}┤")
    print(f"  │                                                                    │")
    print(f"  │  Quality    {bar(s['gem_score'])} {s['gem_score']:>3}/100           │")
    for r in s['gem_reasons'][:4]:
        print(f"  │    · {r:<62}│")
    print(f"  │                                                                    │")
    print(f"  │  Availability {bar(s['avail_score'])} {s['avail_score']:>3}/100  ({s['convergence']})    │"[:71] + "│")
    if s['signals']:
        for name, detail in s['signals'][:5]:
            line = f"{name}: {detail}"
            if len(line) > 60: line = line[:57] + "..."
            print(f"  │    ▸ {line:<62}│")
    else:
        print(f"  │    No availability signals detected                              │")
    print(f"  │                                                                    │")

    if s['rent_stabilized']:
        print(f"  │  ★ Rent stabilized: {s['stab_units']} units                                   │"[:71] + "│")
    if s['last_sale_date']:
        amt = f"${s['last_sale_amt']:,.0f}" if s['last_sale_amt'] else "undisclosed"
        print(f"  │  Last sale: {s['last_sale_date'][:10]} for {amt:<36}│"[:71] + "│")

    print(f"  └{'─'*68}┘")
    print()


def show_recommendations(scored, count=10, label="RECOMMENDATIONS"):
    """Show a ranked list of buildings."""
    print(f"\n  {label}")
    print(f"  {'─'*70}")
    for i, s in enumerate(scored[:count], 1):
        stab = " ★RS" if s['rent_stabilized'] else ""
        sig_summary = f"{s['signal_count']} signals" if s['signal_count'] > 0 else "watching"
        top_sig = s['signals'][0][0] if s['signals'] else ""
        print(f"  {i:>2}. {s['address']:<30} {s['zip']}  "
              f"Q:{s['gem_score']:<3} A:{s['avail_score']:<3} "
              f"({sig_summary}){stab}")
        if top_sig:
            print(f"      └─ {top_sig}")
    print()


# ── Main Loop ──────────────────────────────────────────────────────────────

def main():
    print()
    print("  ╔══════════════════════════════════════════════════════════╗")
    print("  ║                                                        ║")
    print("  ║   VAYO — NYC Apartment Concierge                       ║")
    print("  ║                                                        ║")
    print("  ║   I know every residential building in New York City.   ║")
    print("  ║   Tell me what you're looking for.                      ║")
    print("  ║                                                        ║")
    print("  ╚══════════════════════════════════════════════════════════╝")
    print()
    print("  Commands:")
    print("    search <neighborhood>    Find buildings in an area")
    print("    look at <address>        Deep dive on a specific building")
    print("    diamonds                 Show top gems with availability signals")
    print("    hottest                  Strongest convergence (most likely to open)")
    print("    help                     Show all commands")
    print("    quit                     Exit")
    print()

    current_zips = None
    current_buildings = None
    current_data = None
    current_scored = None

    while True:
        try:
            raw = input("  vayo> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n")
            break

        if not raw:
            continue

        cmd = raw.lower()

        if cmd in ('quit', 'exit', 'q'):
            break

        elif cmd == 'help':
            print("""
  Commands:
    search <neighborhood>     Score all buildings in a neighborhood
                              Examples: "search east village", "search 10003"
    look at <address>         Detailed report on a building
                              Example: "look at 43 5 avenue"
    diamonds                  Gem >= 60 + 2+ availability signals
    hottest                   3+ converging signals (strongest predictions)
    top                       Top 20 by combined score
    quality                   Top 20 by gem score (best buildings)
    prewar                    Best prewar buildings in current area
    stabilized                Rent stabilized buildings with signals
    help                      Show this help
    quit                      Exit
""")

        elif cmd.startswith('search '):
            query = cmd[7:].strip()
            # Check if it's a zip code
            if query.isdigit() and len(query) == 5:
                zips = [query]
            elif ',' in query:
                zips = [z.strip() for z in query.split(',')]
            elif query in NEIGHBORHOODS:
                zips = NEIGHBORHOODS[query]
            else:
                # Fuzzy match neighborhood names
                matches = [k for k in NEIGHBORHOODS if query in k]
                if matches:
                    zips = []
                    for m in matches:
                        zips.extend(NEIGHBORHOODS[m])
                    zips = list(set(zips))
                else:
                    print(f"\n  Don't recognize '{query}'. Try a neighborhood name or zip code.")
                    print(f"  Known neighborhoods: {', '.join(sorted(NEIGHBORHOODS.keys()))}")
                    continue

            current_zips = zips
            print(f"\n  Scanning zip codes: {', '.join(zips)}...")
            current_buildings = search_buildings(zips)
            print(f"  Loading signals for {len(current_buildings):,} buildings...", end=' ', flush=True)
            target_bbls = set(current_buildings.keys())
            current_data = load_data(target_bbls)
            print("done.")
            print("  Scoring...", end=' ', flush=True)
            current_scored = []
            for bbl, b in current_buildings.items():
                current_scored.append(score_building(b, current_data))
            current_scored.sort(key=lambda x: x['combined'], reverse=True)
            print("done.")

            # Summary
            with_signals = sum(1 for s in current_scored if s['signal_count'] > 0)
            convergent = sum(1 for s in current_scored if s['signal_count'] >= 2)
            hot = sum(1 for s in current_scored if s['signal_count'] >= 3)
            gems = sum(1 for s in current_scored if s['gem_score'] >= 60)
            diamonds = sum(1 for s in current_scored if s['gem_score'] >= 60 and s['signal_count'] >= 2)

            print(f"\n  ┌────────────────────────────────────┐")
            print(f"  │  {len(current_scored):>5,} buildings scored             │")
            print(f"  │  {gems:>5,} high quality (gem >= 60)      │")
            print(f"  │  {with_signals:>5,} showing availability signals  │")
            print(f"  │  {convergent:>5,} with 2+ signals (convergent)  │")
            print(f"  │  {hot:>5,} with 3+ signals (hot)          │")
            print(f"  │  {diamonds:>5,} diamonds (quality + signals)  │")
            print(f"  └────────────────────────────────────┘")

            show_recommendations(current_scored, 10, "TOP 10 OVERALL")

        elif cmd.startswith('look at ') or cmd.startswith('show '):
            query = raw[8:] if cmd.startswith('look at ') else raw[5:]
            results = lookup_building(query)
            if not results:
                print(f"\n  No building found matching '{query}'")
                continue
            if len(results) > 1 and len(results) <= 10:
                print(f"\n  Found {len(results)} matches:")
                for bbl, b in results.items():
                    print(f"    {b['address']}, {b['zipcode']} ({b['units_residential']} units) — BBL {bbl}")
                print(f"\n  Showing details for all {len(results)}...")
            elif len(results) > 10:
                print(f"\n  Found {len(results)} matches — showing first 5:")
                results = dict(list(results.items())[:5])

            target_bbls = set(results.keys())
            bld_data = load_data(target_bbls)
            for bbl, b in results.items():
                s = score_building(b, bld_data)
                show_building_detail(s)

        elif cmd == 'diamonds':
            if not current_scored:
                print("\n  Run 'search <neighborhood>' first.")
                continue
            diamonds = [s for s in current_scored if s['gem_score'] >= 60 and s['signal_count'] >= 2]
            diamonds.sort(key=lambda x: x['combined'], reverse=True)
            if not diamonds:
                print("\n  No diamonds found in this area. Try a broader search.")
            else:
                show_recommendations(diamonds, 20, f"DIAMONDS — {len(diamonds)} found (quality >= 60 + 2+ signals)")

        elif cmd == 'hottest':
            if not current_scored:
                print("\n  Run 'search <neighborhood>' first.")
                continue
            hot = [s for s in current_scored if s['signal_count'] >= 3]
            hot.sort(key=lambda x: x['avail_score'], reverse=True)
            if not hot:
                print("\n  No buildings with 3+ signals in this area.")
            else:
                show_recommendations(hot, 20, f"HOTTEST — {len(hot)} buildings with 3+ converging signals")

        elif cmd in ('top', 'top 20', 'rankings'):
            if not current_scored:
                print("\n  Run 'search <neighborhood>' first.")
                continue
            show_recommendations(current_scored, 20, "TOP 20 BY COMBINED SCORE")

        elif cmd == 'quality':
            if not current_scored:
                print("\n  Run 'search <neighborhood>' first.")
                continue
            by_gem = sorted(current_scored, key=lambda x: x['gem_score'], reverse=True)
            show_recommendations(by_gem, 20, "TOP 20 BY QUALITY")

        elif cmd == 'prewar':
            if not current_scored:
                print("\n  Run 'search <neighborhood>' first.")
                continue
            prewar = [s for s in current_scored if s['built'] and 1880 <= s['built'] <= 1945]
            prewar.sort(key=lambda x: x['combined'], reverse=True)
            show_recommendations(prewar, 20, f"BEST PREWAR BUILDINGS — {len(prewar)} total")

        elif cmd == 'stabilized':
            if not current_scored:
                print("\n  Run 'search <neighborhood>' first.")
                continue
            stab = [s for s in current_scored if s['rent_stabilized'] and s['signal_count'] > 0]
            stab.sort(key=lambda x: x['combined'], reverse=True)
            show_recommendations(stab, 20, f"RENT STABILIZED WITH SIGNALS — {len(stab)} buildings")

        else:
            # Try to interpret as a neighborhood search
            if cmd in NEIGHBORHOODS or any(cmd in k for k in NEIGHBORHOODS):
                # Re-run as search
                raw = f"search {raw}"
                cmd = raw.lower()
                matches = [k for k in NEIGHBORHOODS if cmd[7:] in k]
                if matches:
                    zips = []
                    for m in matches:
                        zips.extend(NEIGHBORHOODS[m])
                    zips = list(set(zips))
                    current_zips = zips
                    print(f"\n  Scanning: {', '.join(matches)} (zips: {', '.join(zips)})...")
                    current_buildings = search_buildings(zips)
                    print(f"  Loading signals for {len(current_buildings):,} buildings...", end=' ', flush=True)
                    target_bbls = set(current_buildings.keys())
                    current_data = load_data(target_bbls)
                    print("done.")
                    print("  Scoring...", end=' ', flush=True)
                    current_scored = []
                    for bbl, b in current_buildings.items():
                        current_scored.append(score_building(b, current_data))
                    current_scored.sort(key=lambda x: x['combined'], reverse=True)
                    print("done.")
                    with_signals = sum(1 for s in current_scored if s['signal_count'] > 0)
                    diamonds = sum(1 for s in current_scored if s['gem_score'] >= 60 and s['signal_count'] >= 2)
                    print(f"\n  {len(current_scored):,} buildings | {with_signals:,} with signals | {diamonds:,} diamonds")
                    show_recommendations(current_scored, 10, "TOP 10 OVERALL")
            else:
                print(f"\n  Not sure what you mean. Type 'help' for commands.")

    print("  Goodbye.\n")

if __name__ == '__main__':
    main()

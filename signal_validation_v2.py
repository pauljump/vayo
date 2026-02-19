#!/usr/bin/env python3
"""
Signal Validation v2 - Availability signals predicting DEED transfers
Optimized: pre-load DEED dates into memory for fast lookups
"""

import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timedelta

DB_PATH = "/Users/pjump/Desktop/projects/vayo/vayo_clean.db"

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA cache_size=-2000000")
    conn.execute("PRAGMA temp_store=MEMORY")
    return conn

def fmt_pct(n, d):
    if d == 0: return "0.0% (0/0)"
    return f"{100*n/d:.1f}% ({n:,}/{d:,})"

def lift(rate, baseline):
    if baseline == 0: return "N/A"
    return f"{rate/baseline:.2f}x"

def print_header(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")

def print_result(label, hits, total, baseline_rate):
    if total == 0:
        print(f"  {label}: NO DATA")
        return
    rate = hits / total
    print(f"  {label}: {fmt_pct(hits, total)}  |  lift = {lift(rate, baseline_rate)}")

def parse_date(s):
    """Parse ISO date string to datetime.date"""
    if not s: return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except:
        return None

def parse_date_mdy(s):
    """Parse MM/DD/YYYY date string"""
    if not s: return None
    try:
        return datetime.strptime(s[:10], "%m/%d/%Y").date()
    except:
        return None

def has_deed_within(deed_dates_by_bbl, bbl, signal_date, months):
    """Check if any DEED on this BBL within N months after signal_date"""
    if bbl not in deed_dates_by_bbl:
        return False
    cutoff = signal_date + timedelta(days=months * 30)
    for dd in deed_dates_by_bbl[bbl]:
        if signal_date <= dd <= cutoff:
            return True
    return False


def main():
    print("Signal Validation v2 - Availability Signals -> DEED Transfers")
    print(f"Database: {DB_PATH}")
    print(f"Started at: {time.strftime('%H:%M:%S')}")

    conn = get_conn()

    # ============================================================
    # PRE-LOAD: All DEED transactions into memory (by BBL -> list of dates)
    # ============================================================
    print("\n  Pre-loading DEED transactions into memory...")
    t0 = time.time()
    deed_dates = defaultdict(list)
    cursor = conn.execute("""
        SELECT bbl, document_date FROM acris_transactions 
        WHERE doc_type = 'DEED' AND document_date IS NOT NULL
    """)
    deed_count = 0
    for bbl, ddate in cursor:
        d = parse_date(ddate)
        if d:
            deed_dates[bbl].append(d)
            deed_count += 1
    print(f"  Loaded {deed_count:,} DEED records across {len(deed_dates):,} BBLs in {time.time()-t0:.1f}s")

    # Pre-load residential BBLs
    print("  Loading residential BBLs...")
    res_bbls = set(r[0] for r in conn.execute("SELECT bbl FROM buildings WHERE unitsres > 0").fetchall())
    print(f"  {len(res_bbls):,} residential BBLs")

    # ============================================================
    # BASELINE
    # ============================================================
    print_header("BASELINE: Random 12-month DEED rate for residential BBLs")
    total_res = len(res_bbls)
    
    d2022_start = datetime(2022, 1, 1).date()
    d2022_mid = datetime(2022, 7, 1).date()
    d2023_start = datetime(2023, 1, 1).date()
    d2023_mid = datetime(2023, 7, 1).date()

    deed_6m = sum(1 for bbl in res_bbls if any(d2022_start <= d < d2022_mid for d in deed_dates.get(bbl, [])))
    deed_12m = sum(1 for bbl in res_bbls if any(d2022_start <= d < d2023_start for d in deed_dates.get(bbl, [])))
    deed_18m = sum(1 for bbl in res_bbls if any(d2022_start <= d < d2023_mid for d in deed_dates.get(bbl, [])))

    b6 = deed_6m / total_res
    b12 = deed_12m / total_res
    b18 = deed_18m / total_res

    print(f"  Total residential BBLs: {total_res:,}")
    print(f"  6-month baseline (Jan-Jun 2022):  {fmt_pct(deed_6m, total_res)}")
    print(f"  12-month baseline (2022):          {fmt_pct(deed_12m, total_res)}")
    print(f"  18-month baseline (2022-mid2023):  {fmt_pct(deed_18m, total_res)}")

    # ============================================================
    # ANALYSIS 1: HPD Litigation
    # ============================================================
    print_header("ANALYSIS 1: HPD Litigation -> Deed Transfers")
    t0 = time.time()

    # Load litigation data
    lit_data = conn.execute("""
        SELECT bbl, casetype, MIN(caseopendate) as first_case
        FROM hpd_litigation
        WHERE caseopendate >= '2022-01-01' AND caseopendate < '2024-01-01'
        GROUP BY bbl, casetype
    """).fetchall()

    # Per BBL: earliest case
    bbl_first_case = {}
    bbl_casetypes = defaultdict(set)
    casetype_bbls = defaultdict(dict)  # casetype -> {bbl: first_date}
    for bbl, ct, dstr in lit_data:
        if bbl not in res_bbls: continue
        d = parse_date(dstr)
        if not d: continue
        bbl_casetypes[bbl].add(ct)
        if bbl not in bbl_first_case or d < bbl_first_case[bbl]:
            bbl_first_case[bbl] = d
        if bbl not in casetype_bbls[ct] or d < casetype_bbls[ct][bbl]:
            casetype_bbls[ct][bbl] = d

    print(f"  {len(bbl_first_case):,} residential BBLs with HPD litigation in 2022-2023")
    
    # Case type counts
    for ct in sorted(casetype_bbls.keys(), key=lambda x: -len(casetype_bbls[x])):
        print(f"    {ct}: {len(casetype_bbls[ct]):,} BBLs")

    print(f"\n  --- Overall (all case types) ---")
    for months, label, baseline in [(6, "6mo", b6), (12, "12mo", b12), (18, "18mo", b18)]:
        hits = sum(1 for bbl, d in bbl_first_case.items() if has_deed_within(deed_dates, bbl, d, months))
        print_result(f"DEED within {label}", hits, len(bbl_first_case), baseline)

    print(f"\n  --- By case type (12-month window) ---")
    for ct in sorted(casetype_bbls.keys(), key=lambda x: -len(casetype_bbls[x]))[:8]:
        ct_bbls = casetype_bbls[ct]
        hits = sum(1 for bbl, d in ct_bbls.items() if has_deed_within(deed_dates, bbl, d, 12))
        print_result(f"  {ct}", hits, len(ct_bbls), b12)

    print(f"  Computed in {time.time()-t0:.1f}s")

    # ============================================================
    # ANALYSIS 2: ECB Violations
    # ============================================================
    print_header("ANALYSIS 2: ECB Violations -> Deed Transfers")
    t0 = time.time()

    ecb_data = conn.execute("""
        SELECT bbl, severity, issue_date, balance_due
        FROM ecb_violations
        WHERE issue_date >= '2022-01-01' AND issue_date < '2024-01-01'
    """).fetchall()

    ecb_bbl_first = {}
    ecb_by_severity = defaultdict(dict)
    ecb_unpaid_bbls = {}
    ecb_paid_bbls = {}
    for bbl, sev, dstr, bal in ecb_data:
        if bbl not in res_bbls: continue
        d = parse_date(dstr)
        if not d: continue
        if bbl not in ecb_bbl_first or d < ecb_bbl_first[bbl]:
            ecb_bbl_first[bbl] = d
        if sev:
            if bbl not in ecb_by_severity[sev] or d < ecb_by_severity[sev][bbl]:
                ecb_by_severity[sev][bbl] = d
        if bal and float(bal) > 0:
            if bbl not in ecb_unpaid_bbls or d < ecb_unpaid_bbls[bbl]:
                ecb_unpaid_bbls[bbl] = d
        else:
            if bbl not in ecb_paid_bbls or d < ecb_paid_bbls[bbl]:
                ecb_paid_bbls[bbl] = d

    print(f"  {len(ecb_bbl_first):,} residential BBLs with ECB violations in 2022-2023")

    print(f"\n  --- Overall ---")
    for months, label, baseline in [(6, "6mo", b6), (12, "12mo", b12)]:
        hits = sum(1 for bbl, d in ecb_bbl_first.items() if has_deed_within(deed_dates, bbl, d, months))
        print_result(f"DEED within {label}", hits, len(ecb_bbl_first), baseline)

    print(f"\n  --- By severity (12-month window) ---")
    for sev in sorted(ecb_by_severity.keys(), key=lambda x: -len(ecb_by_severity[x])):
        bbls = ecb_by_severity[sev]
        if len(bbls) < 50: continue
        hits = sum(1 for bbl, d in bbls.items() if has_deed_within(deed_dates, bbl, d, 12))
        print_result(f"  {sev} ({len(bbls):,} BBLs)", hits, len(bbls), b12)

    print(f"\n  --- Unpaid balance vs paid (12mo) ---")
    for label, bbls_dict in [("Unpaid balance", ecb_unpaid_bbls), ("No balance due", ecb_paid_bbls)]:
        hits = sum(1 for bbl, d in bbls_dict.items() if has_deed_within(deed_dates, bbl, d, 12))
        print_result(f"  {label}", hits, len(bbls_dict), b12)

    print(f"  Computed in {time.time()-t0:.1f}s")

    # ============================================================
    # ANALYSIS 3: Management Changes
    # ============================================================
    print_header("ANALYSIS 3: Management / Contact Changes -> Deed Transfers")
    t0 = time.time()

    mgmt_data = conn.execute("""
        SELECT bbl, contact_type, MIN(registration_date) as reg_date
        FROM building_contacts
        WHERE registration_date >= '2022-01-01' AND registration_date < '2024-01-01'
        GROUP BY bbl, contact_type
    """).fetchall()

    mgmt_bbl_first = {}
    mgmt_by_type = defaultdict(dict)
    for bbl, ct, dstr in mgmt_data:
        if bbl not in res_bbls: continue
        d = parse_date(dstr)
        if not d: continue
        if bbl not in mgmt_bbl_first or d < mgmt_bbl_first[bbl]:
            mgmt_bbl_first[bbl] = d
        if ct:
            if bbl not in mgmt_by_type[ct] or d < mgmt_by_type[ct][bbl]:
                mgmt_by_type[ct][bbl] = d

    print(f"  {len(mgmt_bbl_first):,} residential BBLs with new registrations in 2022-2023")

    print(f"\n  --- Overall ---")
    for months, label, baseline in [(6, "6mo", b6), (12, "12mo", b12)]:
        hits = sum(1 for bbl, d in mgmt_bbl_first.items() if has_deed_within(deed_dates, bbl, d, months))
        print_result(f"DEED within {label}", hits, len(mgmt_bbl_first), baseline)

    print(f"\n  --- By contact_type (12-month window) ---")
    for ct in sorted(mgmt_by_type.keys(), key=lambda x: -len(mgmt_by_type[x]))[:6]:
        bbls = mgmt_by_type[ct]
        if len(bbls) < 50: continue
        hits = sum(1 for bbl, d in bbls.items() if has_deed_within(deed_dates, bbl, d, 12))
        print_result(f"  {ct} ({len(bbls):,} BBLs)", hits, len(bbls), b12)

    print(f"  Computed in {time.time()-t0:.1f}s")

    # ============================================================
    # ANALYSIS 4: Certificates of Occupancy
    # ============================================================
    print_header("ANALYSIS 4: Certificates of Occupancy -> Deed Transfers")
    total_co = conn.execute("SELECT COUNT(*) FROM certificates_of_occupancy").fetchone()[0]
    with_date = conn.execute(
        "SELECT COUNT(*) FROM certificates_of_occupancy WHERE co_issue_date IS NOT NULL AND co_issue_date <> ''"
    ).fetchone()[0]
    print(f"  Total CO records: {total_co:,}")
    print(f"  With issue date: {with_date:,}")
    print(f"  WARNING: co_issue_date is entirely NULL/empty.")
    print(f"  DATA QUALITY TOO POOR - skipping CO signal analysis.")

    # ============================================================
    # ANALYSIS 5: 311 Distress Complaints
    # ============================================================
    print_header("ANALYSIS 5: 311 Distress Complaints -> Deed Transfers")
    print("  (Using service_requests_311 table)")
    t0 = time.time()

    distress_types = ["HEAT/HOT WATER", "UNSANITARY CONDITION", "PLUMBING",
                      "WATER LEAK", "PAINT/PLASTER"]
    non_distress_types = ["Noise - Residential", "Illegal Parking", "Blocked Driveway"]

    print(f"\n  --- Distress complaint types (12-month window) ---")
    for ctype in distress_types:
        rows = conn.execute("""
            SELECT bbl, MIN(date(created_date)) as first_complaint
            FROM service_requests_311
            WHERE complaint_type = ?
              AND created_date >= '2022-01-01' AND created_date < '2024-01-01'
              AND bbl IS NOT NULL AND bbl > 0
            GROUP BY bbl
        """, (ctype,)).fetchall()
        bbls_dict = {}
        for bbl, dstr in rows:
            if bbl not in res_bbls: continue
            d = parse_date(dstr)
            if d: bbls_dict[bbl] = d
        hits = sum(1 for bbl, d in bbls_dict.items() if has_deed_within(deed_dates, bbl, d, 12))
        print_result(f"  {ctype}", hits, len(bbls_dict), b12)

    print(f"\n  --- Non-distress types for comparison (12mo) ---")
    for ctype in non_distress_types:
        rows = conn.execute("""
            SELECT bbl, MIN(date(created_date)) as first_complaint
            FROM service_requests_311
            WHERE complaint_type = ?
              AND created_date >= '2022-01-01' AND created_date < '2024-01-01'
              AND bbl IS NOT NULL AND bbl > 0
            GROUP BY bbl
        """, (ctype,)).fetchall()
        bbls_dict = {}
        for bbl, dstr in rows:
            if bbl not in res_bbls: continue
            d = parse_date(dstr)
            if d: bbls_dict[bbl] = d
        hits = sum(1 for bbl, d in bbls_dict.items() if has_deed_within(deed_dates, bbl, d, 12))
        print_result(f"  {ctype}", hits, len(bbls_dict), b12)

    # High-volume distress
    print(f"\n  --- High-volume distress (3+ complaints on same BBL, 12mo) ---")
    for ctype in ["HEAT/HOT WATER", "UNSANITARY CONDITION"]:
        rows = conn.execute("""
            SELECT bbl, MIN(date(created_date)) as first_complaint, COUNT(*) as cnt
            FROM service_requests_311
            WHERE complaint_type = ?
              AND created_date >= '2022-01-01' AND created_date < '2024-01-01'
              AND bbl IS NOT NULL AND bbl > 0
            GROUP BY bbl
            HAVING cnt >= 3
        """, (ctype,)).fetchall()
        bbls_dict = {}
        for bbl, dstr, _ in rows:
            if bbl not in res_bbls: continue
            d = parse_date(dstr)
            if d: bbls_dict[bbl] = d
        hits = sum(1 for bbl, d in bbls_dict.items() if has_deed_within(deed_dates, bbl, d, 12))
        print_result(f"  3+ {ctype}", hits, len(bbls_dict), b12)

    # 5+ complaints of any distress type
    print(f"\n  --- 5+ total distress complaints (any type) on same BBL (12mo) ---")
    rows = conn.execute("""
        SELECT bbl, MIN(date(created_date)) as first_complaint, COUNT(*) as cnt
        FROM service_requests_311
        WHERE complaint_type IN ('HEAT/HOT WATER', 'UNSANITARY CONDITION', 'PLUMBING', 'WATER LEAK', 'PAINT/PLASTER')
          AND created_date >= '2022-01-01' AND created_date < '2024-01-01'
          AND bbl IS NOT NULL AND bbl > 0
        GROUP BY bbl
        HAVING cnt >= 5
    """).fetchall()
    bbls_dict = {}
    for bbl, dstr, _ in rows:
        if bbl not in res_bbls: continue
        d = parse_date(dstr)
        if d: bbls_dict[bbl] = d
    hits = sum(1 for bbl, d in bbls_dict.items() if has_deed_within(deed_dates, bbl, d, 12))
    print_result(f"  5+ any distress complaints", hits, len(bbls_dict), b12)

    print(f"  Computed in {time.time()-t0:.1f}s")

    # ============================================================
    # ANALYSIS 6: DOB Permit Cost Buckets
    # ============================================================
    print_header("ANALYSIS 6: DOB Permit Cost Buckets -> Deed Transfers")
    t0 = time.time()

    perm_data = conn.execute("""
        SELECT bbl, latest_action_date, initial_cost
        FROM dob_permits
        WHERE substr(latest_action_date,7,4) IN ('2022','2023')
          AND initial_cost IS NOT NULL AND initial_cost <> ''
    """).fetchall()

    # Parse and bucket
    cost_buckets = {"< $10K": {}, "$10K-$50K": {}, "$50K-$200K": {}, "$200K+": {}}
    for bbl, dstr, cost_str in perm_data:
        if bbl not in res_bbls: continue
        d = parse_date_mdy(dstr)
        if not d: continue
        try:
            cost = float(cost_str.replace("$", "").replace(",", ""))
        except:
            continue
        if cost < 10000: bucket = "< $10K"
        elif cost < 50000: bucket = "$10K-$50K"
        elif cost < 200000: bucket = "$50K-$200K"
        else: bucket = "$200K+"
        if bbl not in cost_buckets[bucket] or d < cost_buckets[bucket][bbl]:
            cost_buckets[bucket][bbl] = d

    print(f"  --- By initial cost bucket (12-month window) ---")
    for label in ["< $10K", "$10K-$50K", "$50K-$200K", "$200K+"]:
        bbls = cost_buckets[label]
        hits = sum(1 for bbl, d in bbls.items() if has_deed_within(deed_dates, bbl, d, 12))
        print_result(f"  {label} ({len(bbls):,} BBLs)", hits, len(bbls), b12)

    print(f"  Computed in {time.time()-t0:.1f}s")

    # ============================================================
    # ANALYSIS 7: STACKED SIGNALS
    # ============================================================
    print_header("ANALYSIS 7: STACKED SIGNALS (Combinations)")
    t0 = time.time()

    # For stacked analysis, use broader window: any DEED 2022-2024
    d2025_start = datetime(2025, 1, 1).date()

    # Signal sets (BBLs)
    sig_lit = set(bbl_first_case.keys())
    sig_ecb = set(ecb_bbl_first.keys())
    sig_mgmt = set(mgmt_bbl_first.keys())

    # 311 heat signal
    heat_rows = conn.execute("""
        SELECT bbl, MIN(date(created_date))
        FROM service_requests_311
        WHERE complaint_type = 'HEAT/HOT WATER'
          AND created_date >= '2022-01-01' AND created_date < '2024-01-01'
          AND bbl IS NOT NULL AND bbl > 0
        GROUP BY bbl
    """).fetchall()
    sig_heat = {}
    for bbl, dstr in heat_rows:
        if bbl in res_bbls:
            d = parse_date(dstr)
            if d: sig_heat[bbl] = d
    sig_heat_set = set(sig_heat.keys())

    # DOB permit signal
    sig_permit = set()
    for bucket in cost_buckets.values():
        sig_permit.update(bucket.keys())

    print(f"  Signal BBL counts:")
    print(f"    HPD Litigation:    {len(sig_lit):,}")
    print(f"    ECB Violations:    {len(sig_ecb):,}")
    print(f"    Management Change: {len(sig_mgmt):,}")
    print(f"    311 HEAT/HOT WATER:{len(sig_heat_set):,}")
    print(f"    DOB Permits:       {len(sig_permit):,}")

    # Count signals per BBL
    signal_count = defaultdict(int)
    all_signal_bbls = sig_lit | sig_ecb | sig_mgmt | sig_heat_set | sig_permit
    for bbl in res_bbls:
        cnt = 0
        if bbl in sig_lit: cnt += 1
        if bbl in sig_ecb: cnt += 1
        if bbl in sig_mgmt: cnt += 1
        if bbl in sig_heat_set: cnt += 1
        if bbl in sig_permit: cnt += 1
        signal_count[bbl] = cnt

    # Distribution
    count_dist = defaultdict(int)
    for cnt in signal_count.values():
        count_dist[cnt] += 1

    print(f"\n  --- Signal count distribution ---")
    for sc in sorted(count_dist.keys()):
        print(f"    {sc} signals: {count_dist[sc]:,} BBLs")

    # DEED rate by signal count (using fixed window 2022-2024)
    print(f"\n  --- DEED rate (any DEED in 2022-2024) by signal count ---")
    for sc in sorted(count_dist.keys()):
        bbls_with_sc = [bbl for bbl, c in signal_count.items() if c == sc]
        hits = sum(1 for bbl in bbls_with_sc if any(d2022_start <= d < d2025_start for d in deed_dates.get(bbl, [])))
        print_result(f"  {sc} signals", hits, len(bbls_with_sc), b12)

    # Specific 2-signal combos
    print(f"\n  --- Top 2-signal combinations (DEED in 2022-2024) ---")
    combos = [
        ("Litigation + ECB", sig_lit & sig_ecb),
        ("Litigation + Heat311", sig_lit & sig_heat_set),
        ("Litigation + MgmtChange", sig_lit & sig_mgmt),
        ("ECB + Heat311", sig_ecb & sig_heat_set),
        ("ECB + MgmtChange", sig_ecb & sig_mgmt),
        ("ECB + Permit", sig_ecb & sig_permit),
        ("Heat311 + MgmtChange", sig_heat_set & sig_mgmt),
        ("Permit + MgmtChange", sig_permit & sig_mgmt),
        ("Litigation + Permit", sig_lit & sig_permit),
    ]
    for label, combo_bbls in combos:
        if len(combo_bbls) < 20: continue
        hits = sum(1 for bbl in combo_bbls if any(d2022_start <= d < d2025_start for d in deed_dates.get(bbl, [])))
        print_result(f"  {label} ({len(combo_bbls):,} BBLs)", hits, len(combo_bbls), b12)

    # Triple combos
    print(f"\n  --- Top 3-signal combinations (DEED in 2022-2024) ---")
    triples = [
        ("Litigation + ECB + Heat311", sig_lit & sig_ecb & sig_heat_set),
        ("Litigation + ECB + MgmtChange", sig_lit & sig_ecb & sig_mgmt),
        ("ECB + Heat311 + MgmtChange", sig_ecb & sig_heat_set & sig_mgmt),
        ("Litigation + Heat311 + MgmtChange", sig_lit & sig_heat_set & sig_mgmt),
        ("Litigation + ECB + Permit", sig_lit & sig_ecb & sig_permit),
        ("ECB + Heat311 + Permit", sig_ecb & sig_heat_set & sig_permit),
        ("Litigation + Heat311 + Permit", sig_lit & sig_heat_set & sig_permit),
    ]
    for label, combo_bbls in triples:
        if len(combo_bbls) < 10: continue
        hits = sum(1 for bbl in combo_bbls if any(d2022_start <= d < d2025_start for d in deed_dates.get(bbl, [])))
        print_result(f"  {label} ({len(combo_bbls):,} BBLs)", hits, len(combo_bbls), b12)

    # Single signal baselines (same methodology)
    print(f"\n  --- Single signal only (no other signals) DEED 2022-2024 ---")
    singles = [
        ("Litigation only", sig_lit),
        ("ECB only", sig_ecb),
        ("MgmtChange only", sig_mgmt),
        ("Heat311 only", sig_heat_set),
        ("Permit only", sig_permit),
    ]
    for label, sig_set in singles:
        alone = [bbl for bbl in sig_set if signal_count[bbl] == 1]
        if len(alone) < 10: continue
        hits = sum(1 for bbl in alone if any(d2022_start <= d < d2025_start for d in deed_dates.get(bbl, [])))
        print_result(f"  {label} ({len(alone):,} BBLs)", hits, len(alone), b12)

    print(f"  Computed in {time.time()-t0:.1f}s")

    # ============================================================
    # SUMMARY
    # ============================================================
    print_header("SUMMARY")
    print(f"  Completed at: {time.strftime('%H:%M:%S')}")
    print(f"  12-month baseline DEED rate: {100*b12:.2f}%")
    print(f"")
    print(f"  Lift interpretation:")
    print(f"    > 1.5x = actionable signal")
    print(f"    > 2.0x = strong signal")
    print(f"    > 5.0x = very strong signal")
    print(f"  Stacked signals should show multiplicative lift.")

    conn.close()

if __name__ == "__main__":
    main()

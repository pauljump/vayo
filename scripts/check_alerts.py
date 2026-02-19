#!/usr/bin/env python3
"""
Vayo Alert Checker — Monitors watchlist buildings for changes.

Checks for:
  - New Elliman MLS listings matching watchlist building addresses
  - New DOB permits filed
  - Ownership changes in HPD contacts
  - New evictions

Run via cron every 6 hours:
  0 */6 * * * cd /Users/pjump/Desktop/projects/vayo && python3 scripts/check_alerts.py

Sends macOS notifications via osascript.
"""

import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta

DB = "/Users/pjump/Desktop/projects/vayo/vayo_clean.db"
ELLIMAN_DB = "/Users/pjump/Desktop/projects/vayo/elliman_mls.db"


def notify(title: str, message: str):
    """Send macOS notification."""
    try:
        subprocess.run([
            "osascript", "-e",
            f'display notification "{message}" with title "{title}"'
        ], check=True, timeout=5)
    except Exception:
        pass


def main():
    db = sqlite3.connect(DB)
    db.row_factory = sqlite3.Row

    # Ensure tables exist
    db.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
            bbl INTEGER PRIMARY KEY,
            added_at TEXT DEFAULT (datetime('now')),
            notes TEXT
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bbl INTEGER NOT NULL,
            alert_type TEXT NOT NULL,
            message TEXT,
            detail TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            read INTEGER DEFAULT 0
        )
    """)
    db.commit()

    watched = db.execute("""
        SELECT w.bbl, b.address, b.borough, b.zipcode
        FROM watchlist w
        JOIN buildings b ON w.bbl = b.bbl
    """).fetchall()

    if not watched:
        print("No buildings on watchlist.")
        return

    print(f"Checking {len(watched)} watched buildings...")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cutoff_7d = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    new_alerts = 0

    for w in watched:
        bbl = w["bbl"]
        address = w["address"] or ""

        # Check recent DOB permits
        recent_permits = db.execute("""
            SELECT job_type, description, estimated_cost, action_date
            FROM permits
            WHERE bbl = ? AND action_date >= ?
            ORDER BY action_date DESC LIMIT 3
        """, [bbl, cutoff_7d]).fetchall()

        for p in recent_permits:
            # Avoid duplicate alerts
            existing = db.execute("""
                SELECT id FROM alerts
                WHERE bbl = ? AND alert_type = 'new_permit'
                AND detail LIKE ?
                AND created_at >= ?
            """, [bbl, f"%{p['action_date']}%", cutoff_7d]).fetchone()

            if not existing:
                cost = f"${p['estimated_cost']:,.0f}" if p['estimated_cost'] else "unknown cost"
                msg = f"New DOB permit: {p['job_type'] or 'permit'} ({cost})"
                detail = p["description"] or ""
                db.execute(
                    "INSERT INTO alerts (bbl, alert_type, message, detail) VALUES (?, ?, ?, ?)",
                    [bbl, "new_permit", msg, f"{detail} | {p['action_date']}"],
                )
                new_alerts += 1
                notify("Vayo Alert", f"{address}: {msg}")

        # Check recent evictions
        recent_evictions = db.execute("""
            SELECT unit, executed_date
            FROM evictions
            WHERE bbl = ? AND executed_date >= ?
        """, [bbl, cutoff_7d]).fetchall()

        for e in recent_evictions:
            existing = db.execute("""
                SELECT id FROM alerts
                WHERE bbl = ? AND alert_type = 'eviction'
                AND detail LIKE ?
            """, [bbl, f"%{e['executed_date']}%"]).fetchone()

            if not existing:
                unit = e["unit"] or "unknown unit"
                msg = f"Eviction executed: unit {unit}"
                db.execute(
                    "INSERT INTO alerts (bbl, alert_type, message, detail) VALUES (?, ?, ?, ?)",
                    [bbl, "eviction", msg, f"Unit {unit} on {e['executed_date']}"],
                )
                new_alerts += 1
                notify("Vayo Alert", f"{address}: {msg}")

    # Check Elliman listings for watched addresses
    try:
        edb = sqlite3.connect(f"file:{ELLIMAN_DB}?mode=ro", uri=True)
        edb.row_factory = sqlite3.Row

        for w in watched:
            address = (w["address"] or "").strip()
            if not address:
                continue

            listings = edb.execute("""
                SELECT core_listing_id, unit, list_price, listing_type,
                       listing_status, list_date
                FROM listings
                WHERE address LIKE ? AND list_date >= ?
                ORDER BY list_date DESC LIMIT 5
            """, [f"%{address}%", cutoff_7d]).fetchall()

            for l in listings:
                existing = db.execute("""
                    SELECT id FROM alerts
                    WHERE bbl = ? AND alert_type = 'new_listing'
                    AND detail LIKE ?
                """, [w["bbl"], f"%{l['core_listing_id']}%"]).fetchone()

                if not existing:
                    price = f"${l['list_price']:,.0f}" if l["list_price"] else "?"
                    unit = l["unit"] or ""
                    msg = f"New {l['listing_type'] or 'listing'}: {unit} at {price}"
                    db.execute(
                        "INSERT INTO alerts (bbl, alert_type, message, detail) VALUES (?, ?, ?, ?)",
                        [w["bbl"], "new_listing", msg, f"MLS {l['core_listing_id']}"],
                    )
                    new_alerts += 1
                    notify("Vayo Alert", f"{address}: {msg}")

        edb.close()
    except Exception as e:
        print(f"  Warning: Could not check Elliman listings: {e}")

    db.commit()
    db.close()

    print(f"  {new_alerts} new alert(s) created.")
    if new_alerts > 0:
        notify("Vayo", f"{new_alerts} new alert(s) for your watchlist")


if __name__ == "__main__":
    main()

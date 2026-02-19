"""
Database connections and query functions for Vayo API.
Read-only access to vayo_clean.db, elliman_mls.db, se_listings.db.
"""

import sqlite3
import os
from collections import defaultdict
from datetime import datetime, timedelta
from contextlib import contextmanager

DATA_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DB_PATHS = {
    "main": os.path.join(DATA_DIR, "vayo_clean.db"),
    "elliman": os.path.join(DATA_DIR, "elliman_mls.db"),
    "se": os.path.join(DATA_DIR, "se_listings.db"),
}

LOOKBACK_MONTHS = 24


def _cutoff_iso():
    return (datetime.now() - timedelta(days=LOOKBACK_MONTHS * 30)).strftime("%Y-%m-%d")


def get_db(name: str = "main") -> sqlite3.Connection:
    path = DB_PATHS[name]
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


@contextmanager
def get_main_db():
    conn = get_db("main")
    try:
        yield conn
    finally:
        conn.close()


# ── Building queries ─────────────────────────────────────────────────────

def get_building(bbl: int) -> dict | None:
    with get_main_db() as db:
        row = db.execute("SELECT * FROM buildings WHERE bbl = ?", [bbl]).fetchone()
        if row:
            return dict(row)
    return None


def search_buildings_by_address(query: str, limit: int = 20) -> list[dict]:
    with get_main_db() as db:
        rows = db.execute(
            """SELECT bbl, address, zipcode, borough, year_built, num_floors,
                      units_residential, units_total, owner_name, avg_unit_sqft,
                      building_class
               FROM buildings
               WHERE address LIKE ? AND units_residential > 0
               ORDER BY units_residential DESC
               LIMIT ?""",
            [f"%{query.upper()}%", limit],
        ).fetchall()
        return [dict(r) for r in rows]


def search_buildings_filtered(
    borough: str | None = None,
    zipcode: str | None = None,
    min_units: int = 4,
    min_gem: int | None = None,
    rent_stabilized_only: bool = False,
    limit: int = 200,
    offset: int = 0,
    sort_by: str = "gem_score",
    sort_dir: str = "desc",
) -> dict:
    """Search buildings with filters. Uses building_scores table if available."""
    with get_main_db() as db:
        # Check if building_scores table exists
        has_scores = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='building_scores'"
        ).fetchone()

        if has_scores:
            return _search_with_scores(
                db, borough, zipcode, min_units, min_gem,
                rent_stabilized_only, limit, offset, sort_by, sort_dir,
            )
        else:
            return _search_without_scores(
                db, borough, zipcode, min_units, rent_stabilized_only,
                limit, offset,
            )


def _search_with_scores(db, borough, zipcode, min_units, min_gem,
                         rent_stabilized_only, limit, offset, sort_by, sort_dir):
    where = ["b.units_residential >= ?", "b.residential_area > 0"]
    params: list = [min_units]

    if borough:
        where.append("b.borough = ?")
        params.append(borough.upper()[:2])
    if zipcode:
        where.append("b.zipcode = ?")
        params.append(zipcode)
    if min_gem is not None:
        where.append("s.gem_score >= ?")
        params.append(min_gem)
    if rent_stabilized_only:
        where.append("s.rent_stabilized = 1")

    allowed_sorts = {"gem_score", "avail_score", "combined", "units_residential", "year_built"}
    if sort_by not in allowed_sorts:
        sort_by = "gem_score"
    direction = "DESC" if sort_dir.lower() == "desc" else "ASC"

    count_q = f"""
        SELECT COUNT(*) FROM buildings b
        JOIN building_scores s ON b.bbl = s.bbl
        WHERE {' AND '.join(where)}
    """
    total = db.execute(count_q, params).fetchone()[0]

    q = f"""
        SELECT b.bbl, b.address, b.zipcode, b.borough, b.year_built, b.num_floors,
               b.units_residential, b.owner_name, b.avg_unit_sqft, b.building_class,
               s.gem_score, s.avail_score, s.combined, s.signal_count,
               s.convergence, s.rent_stabilized, s.stab_units, s.signals
        FROM buildings b
        JOIN building_scores s ON b.bbl = s.bbl
        WHERE {' AND '.join(where)}
        ORDER BY s.{sort_by} {direction}
        LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])
    rows = db.execute(q, params).fetchall()
    return {"total": total, "buildings": [dict(r) for r in rows]}


def _search_without_scores(db, borough, zipcode, min_units,
                            rent_stabilized_only, limit, offset):
    where = ["units_residential >= ?", "residential_area > 0"]
    params: list = [min_units]

    if borough:
        where.append("borough = ?")
        params.append(borough.upper()[:2])
    if zipcode:
        where.append("zipcode = ?")
        params.append(zipcode)

    q = f"""
        SELECT bbl, address, zipcode, borough, year_built, num_floors,
               units_residential, owner_name, avg_unit_sqft, building_class
        FROM buildings
        WHERE {' AND '.join(where)}
        ORDER BY units_residential DESC
        LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])
    rows = db.execute(q, params).fetchall()
    total = len(rows)  # approximate
    return {"total": total, "buildings": [dict(r) for r in rows]}


# ── Building detail queries ──────────────────────────────────────────────

def get_complaints(bbl: int, limit: int = 100, offset: int = 0) -> list[dict]:
    with get_main_db() as db:
        rows = db.execute(
            """SELECT category, subcategory, status, received_date, severity, unit
               FROM hpd_complaints WHERE bbl = ?
               ORDER BY received_date DESC LIMIT ? OFFSET ?""",
            [bbl, limit, offset],
        ).fetchall()
        return [dict(r) for r in rows]


def get_complaint_stats(bbl: int) -> dict:
    with get_main_db() as db:
        total = db.execute(
            "SELECT COUNT(*) FROM hpd_complaints WHERE bbl = ?", [bbl]
        ).fetchone()[0]
        by_year = db.execute(
            """SELECT substr(received_date, 1, 4) as year, COUNT(*) as cnt
               FROM hpd_complaints WHERE bbl = ? AND received_date IS NOT NULL
               GROUP BY year ORDER BY year""",
            [bbl],
        ).fetchall()
        by_category = db.execute(
            """SELECT category, COUNT(*) as cnt
               FROM hpd_complaints WHERE bbl = ?
               GROUP BY category ORDER BY cnt DESC LIMIT 10""",
            [bbl],
        ).fetchall()
        return {
            "total": total,
            "by_year": [dict(r) for r in by_year],
            "by_category": [dict(r) for r in by_category],
        }


def get_sales(bbl: int) -> list[dict]:
    with get_main_db() as db:
        rows = db.execute(
            """SELECT document_id, doc_type, document_date, recorded_date,
                      amount, seller, buyer, unit
               FROM sales WHERE bbl = ?
               ORDER BY recorded_date DESC""",
            [bbl],
        ).fetchall()
        return [dict(r) for r in rows]


def get_permits(bbl: int) -> list[dict]:
    with get_main_db() as db:
        rows = db.execute(
            """SELECT job_type, description, status, action_date,
                      estimated_cost, existing_units, proposed_units
               FROM permits WHERE bbl = ?
               ORDER BY action_date DESC""",
            [bbl],
        ).fetchall()
        return [dict(r) for r in rows]


def get_violations(bbl: int) -> list[dict]:
    with get_main_db() as db:
        rows = db.execute(
            """SELECT severity, violation_type, issue_date, status,
                      description, penalty, balance_due
               FROM violations WHERE bbl = ?
               ORDER BY issue_date DESC""",
            [bbl],
        ).fetchall()
        return [dict(r) for r in rows]


def get_contacts(bbl: int) -> list[dict]:
    with get_main_db() as db:
        rows = db.execute(
            """SELECT role, company, first_name, last_name, registered_date
               FROM contacts WHERE bbl = ?
               ORDER BY registered_date DESC""",
            [bbl],
        ).fetchall()
        return [dict(r) for r in rows]


def get_litigation(bbl: int) -> list[dict]:
    with get_main_db() as db:
        rows = db.execute(
            """SELECT case_type, opened_date, status
               FROM litigation WHERE bbl = ?
               ORDER BY opened_date DESC""",
            [bbl],
        ).fetchall()
        return [dict(r) for r in rows]


def get_evictions(bbl: int) -> list[dict]:
    with get_main_db() as db:
        rows = db.execute(
            """SELECT address, unit, executed_date, borough, zipcode
               FROM evictions WHERE bbl = ?
               ORDER BY executed_date DESC""",
            [bbl],
        ).fetchall()
        return [dict(r) for r in rows]


def get_rent_stabilization(bbl: int) -> dict | None:
    with get_main_db() as db:
        row = db.execute(
            """SELECT stabilized_units, year, has_421a, has_j51, is_coop_condo
               FROM rent_stabilization WHERE bbl = ?
               ORDER BY year DESC LIMIT 1""",
            [bbl],
        ).fetchone()
        if row:
            return dict(row)
    return None


def get_dob_complaints(bbl: int) -> list[dict]:
    with get_main_db() as db:
        rows = db.execute(
            """SELECT unit, category, disposition, disposition_date, description
               FROM dob_complaints WHERE bbl = ?
               ORDER BY disposition_date DESC LIMIT 100""",
            [bbl],
        ).fetchall()
        return [dict(r) for r in rows]


def get_service_requests(bbl: int, limit: int = 50) -> list[dict]:
    with get_main_db() as db:
        rows = db.execute(
            """SELECT complaint_type, descriptor, created_date, status, resolution
               FROM service_requests WHERE bbl = ?
               ORDER BY created_date DESC LIMIT ?""",
            [bbl, limit],
        ).fetchall()
        return [dict(r) for r in rows]


# ── Scoring ──────────────────────────────────────────────────────────────

def get_building_score(bbl: int) -> dict | None:
    with get_main_db() as db:
        has_scores = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='building_scores'"
        ).fetchone()
        if not has_scores:
            return None
        row = db.execute("SELECT * FROM building_scores WHERE bbl = ?", [bbl]).fetchone()
        if row:
            return dict(row)
    return None


# ── Elliman MLS ──────────────────────────────────────────────────────────

def get_elliman_listings(
    borough: str | None = None,
    min_price: float | None = None,
    max_price: float | None = None,
    bedrooms: int | None = None,
    listing_type: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    try:
        db = get_db("elliman")
    except Exception:
        return []

    where = ["1=1"]
    params: list = []

    if borough:
        where.append("borough = ?")
        params.append(borough)
    if min_price is not None:
        where.append("list_price >= ?")
        params.append(min_price)
    if max_price is not None:
        where.append("list_price <= ?")
        params.append(max_price)
    if bedrooms is not None:
        where.append("bedrooms = ?")
        params.append(bedrooms)
    if listing_type:
        where.append("listing_type = ?")
        params.append(listing_type)

    rows = db.execute(
        f"""SELECT core_listing_id, address, unit, city, zip, neighborhood,
                   borough, latitude, longitude, list_price, bedrooms,
                   bathrooms_total, living_area_sqft, listing_type,
                   listing_status, list_date, home_type
            FROM listings
            WHERE {' AND '.join(where)}
            ORDER BY list_date DESC
            LIMIT ? OFFSET ?""",
        params + [limit, offset],
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]


def get_elliman_listings_for_building(address: str) -> list[dict]:
    """Find Elliman listings matching a building address."""
    try:
        db = get_db("elliman")
    except Exception:
        return []

    rows = db.execute(
        """SELECT core_listing_id, address, unit, list_price, close_price,
                  bedrooms, bathrooms_total, living_area_sqft, listing_type,
                  listing_status, list_date, close_date, price_per_sqft,
                  public_remarks
           FROM listings
           WHERE address LIKE ?
           ORDER BY list_date DESC LIMIT 50""",
        [f"%{address.upper()}%"],
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]


# ── StreetEasy ───────────────────────────────────────────────────────────

def get_se_building(address: str) -> dict | None:
    """Find a StreetEasy building by address match."""
    try:
        db = get_db("se")
    except Exception:
        return None

    row = db.execute(
        """SELECT slug, address, total_units, stories, year_built,
                  neighborhood, building_type, pet_policy, amenities
           FROM buildings
           WHERE address LIKE ? AND status = 'ok'
           LIMIT 1""",
        [f"%{address}%"],
    ).fetchone()
    db.close()
    if row:
        return dict(row)
    return None


def get_se_units(slug: str) -> list[dict]:
    try:
        db = get_db("se")
    except Exception:
        return []

    rows = db.execute(
        """SELECT unit, listing_type, date, price, price_numeric,
                  status, beds, baths, sqft, availability
           FROM unit_summary
           WHERE building_slug = ?
           ORDER BY date DESC""",
        [slug],
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]


# ── Watchlist ────────────────────────────────────────────────────────────

def _ensure_watchlist_tables(db):
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


def get_watchlist() -> list[dict]:
    conn = sqlite3.connect(DB_PATHS["main"])
    conn.row_factory = sqlite3.Row
    _ensure_watchlist_tables(conn)
    rows = conn.execute("""
        SELECT w.bbl, w.added_at, w.notes,
               b.address, b.borough, b.zipcode, b.year_built,
               b.units_residential, b.owner_name
        FROM watchlist w
        JOIN buildings b ON w.bbl = b.bbl
        ORDER BY w.added_at DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def add_to_watchlist(bbl: int, notes: str | None = None) -> bool:
    conn = sqlite3.connect(DB_PATHS["main"])
    _ensure_watchlist_tables(conn)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO watchlist (bbl, notes) VALUES (?, ?)",
            [bbl, notes],
        )
        conn.commit()
        return True
    finally:
        conn.close()


def remove_from_watchlist(bbl: int) -> bool:
    conn = sqlite3.connect(DB_PATHS["main"])
    _ensure_watchlist_tables(conn)
    try:
        conn.execute("DELETE FROM watchlist WHERE bbl = ?", [bbl])
        conn.commit()
        return True
    finally:
        conn.close()


def get_alerts(limit: int = 50, unread_only: bool = False) -> list[dict]:
    conn = sqlite3.connect(DB_PATHS["main"])
    conn.row_factory = sqlite3.Row
    _ensure_watchlist_tables(conn)
    where = "WHERE a.read = 0" if unread_only else ""
    rows = conn.execute(f"""
        SELECT a.id, a.bbl, a.alert_type, a.message, a.detail,
               a.created_at, a.read,
               b.address, b.borough
        FROM alerts a
        JOIN buildings b ON a.bbl = b.bbl
        {where}
        ORDER BY a.created_at DESC
        LIMIT ?
    """, [limit]).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def mark_alert_read(alert_id: int):
    conn = sqlite3.connect(DB_PATHS["main"])
    _ensure_watchlist_tables(conn)
    conn.execute("UPDATE alerts SET read = 1 WHERE id = ?", [alert_id])
    conn.commit()
    conn.close()

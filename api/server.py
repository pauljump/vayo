"""
Vayo API — FastAPI server for NYC building data.

Routes:
  /api/buildings/search?q=...         Address search
  /api/buildings?borough=&zip=&...    Filtered building list
  /api/buildings/{bbl}                Building detail
  /api/buildings/{bbl}/score          Building scores
  /api/buildings/{bbl}/complaints     HPD complaints
  /api/buildings/{bbl}/complaint-stats Complaint statistics
  /api/buildings/{bbl}/sales          ACRIS sales
  /api/buildings/{bbl}/permits        DOB permits
  /api/buildings/{bbl}/violations     ECB violations
  /api/buildings/{bbl}/contacts       HPD contacts
  /api/buildings/{bbl}/litigation     HPD litigation
  /api/buildings/{bbl}/evictions      Marshal evictions
  /api/buildings/{bbl}/rent-stab      Rent stabilization
  /api/buildings/{bbl}/listings       Elliman + SE listings
  /api/listings                       Elliman MLS listings
  /api/watchlist                      Watchlist CRUD
  /api/alerts                         Alert feed

Start: uvicorn api.server:app --reload --port 8000
"""

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from . import db

app = FastAPI(title="Vayo API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Building Search ──────────────────────────────────────────────────────

@app.get("/api/buildings/search")
def search_buildings(q: str = Query(..., min_length=2)):
    results = db.search_buildings_by_address(q)
    return {"results": results}


@app.get("/api/buildings")
def list_buildings(
    borough: str | None = None,
    zip: str | None = None,
    min_units: int = 4,
    min_gem: int | None = None,
    rent_stabilized: bool = False,
    limit: int = Query(200, le=1000),
    offset: int = 0,
    sort: str = "gem_score",
    dir: str = "desc",
):
    return db.search_buildings_filtered(
        borough=borough,
        zipcode=zip,
        min_units=min_units,
        min_gem=min_gem,
        rent_stabilized_only=rent_stabilized,
        limit=limit,
        offset=offset,
        sort_by=sort,
        sort_dir=dir,
    )


# ── Building Detail ──────────────────────────────────────────────────────

@app.get("/api/buildings/{bbl}")
def get_building(bbl: int):
    building = db.get_building(bbl)
    if not building:
        raise HTTPException(status_code=404, detail="Building not found")
    return building


@app.get("/api/buildings/{bbl}/score")
def get_building_score(bbl: int):
    score = db.get_building_score(bbl)
    if not score:
        return {"message": "Scores not computed yet. Run the scoring batch job."}
    return score


@app.get("/api/buildings/{bbl}/complaints")
def get_complaints(bbl: int, limit: int = 100, offset: int = 0):
    return db.get_complaints(bbl, limit, offset)


@app.get("/api/buildings/{bbl}/complaint-stats")
def get_complaint_stats(bbl: int):
    return db.get_complaint_stats(bbl)


@app.get("/api/buildings/{bbl}/sales")
def get_sales(bbl: int):
    return db.get_sales(bbl)


@app.get("/api/buildings/{bbl}/permits")
def get_permits(bbl: int):
    return db.get_permits(bbl)


@app.get("/api/buildings/{bbl}/violations")
def get_violations(bbl: int):
    return db.get_violations(bbl)


@app.get("/api/buildings/{bbl}/contacts")
def get_contacts(bbl: int):
    return db.get_contacts(bbl)


@app.get("/api/buildings/{bbl}/litigation")
def get_litigation(bbl: int):
    return db.get_litigation(bbl)


@app.get("/api/buildings/{bbl}/evictions")
def get_evictions(bbl: int):
    return db.get_evictions(bbl)


@app.get("/api/buildings/{bbl}/rent-stab")
def get_rent_stab(bbl: int):
    result = db.get_rent_stabilization(bbl)
    if not result:
        return {"message": "No rent stabilization data"}
    return result


@app.get("/api/buildings/{bbl}/dob-complaints")
def get_dob_complaints(bbl: int):
    return db.get_dob_complaints(bbl)


@app.get("/api/buildings/{bbl}/311")
def get_311(bbl: int, limit: int = 50):
    return db.get_service_requests(bbl, limit)


@app.get("/api/buildings/{bbl}/listings")
def get_listings_for_building(bbl: int):
    """Get Elliman + StreetEasy listings for a building."""
    building = db.get_building(bbl)
    if not building:
        raise HTTPException(status_code=404, detail="Building not found")

    address = building.get("address", "")
    elliman = db.get_elliman_listings_for_building(address) if address else []

    se_building = db.get_se_building(address) if address else None
    se_units = []
    if se_building:
        se_units = db.get_se_units(se_building["slug"])

    return {
        "elliman": elliman,
        "streeteasy": {"building": se_building, "units": se_units},
    }


# ── Elliman MLS ──────────────────────────────────────────────────────────

@app.get("/api/listings")
def list_elliman(
    borough: str | None = None,
    min_price: float | None = None,
    max_price: float | None = None,
    bedrooms: int | None = None,
    type: str | None = None,
    limit: int = Query(50, le=200),
    offset: int = 0,
):
    return db.get_elliman_listings(
        borough=borough,
        min_price=min_price,
        max_price=max_price,
        bedrooms=bedrooms,
        listing_type=type,
        limit=limit,
        offset=offset,
    )


# ── Watchlist ────────────────────────────────────────────────────────────

class WatchlistAdd(BaseModel):
    bbl: int
    notes: str | None = None


@app.get("/api/watchlist")
def get_watchlist():
    return db.get_watchlist()


@app.post("/api/watchlist")
def add_to_watchlist(item: WatchlistAdd):
    building = db.get_building(item.bbl)
    if not building:
        raise HTTPException(status_code=404, detail="Building not found")
    db.add_to_watchlist(item.bbl, item.notes)
    return {"ok": True, "bbl": item.bbl}


@app.delete("/api/watchlist/{bbl}")
def remove_from_watchlist(bbl: int):
    db.remove_from_watchlist(bbl)
    return {"ok": True}


# ── Alerts ───────────────────────────────────────────────────────────────

@app.get("/api/alerts")
def get_alerts(limit: int = 50, unread_only: bool = False):
    return db.get_alerts(limit, unread_only)


@app.post("/api/alerts/{alert_id}/read")
def mark_alert_read(alert_id: int):
    db.mark_alert_read(alert_id)
    return {"ok": True}

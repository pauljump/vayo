"""Pydantic response models for Vayo API."""

from pydantic import BaseModel


class BuildingSummary(BaseModel):
    bbl: int
    address: str | None
    zipcode: str | None
    borough: str | None
    year_built: int | None
    num_floors: int | None
    units_residential: int | None
    owner_name: str | None
    avg_unit_sqft: int | None
    building_class: str | None


class BuildingDetail(BuildingSummary):
    units_total: int | None
    zoning: str | None
    assessed_total: float | None
    lot_area: int | None
    building_area: int | None
    residential_area: int | None
    commercial_area: int | None
    assessed_per_unit: int | None


class BuildingScore(BaseModel):
    bbl: int
    gem_score: int
    avail_score: int
    combined: int
    signal_count: int
    convergence: str | None
    rent_stabilized: bool
    stab_units: int
    signals: str | None  # JSON string


class Complaint(BaseModel):
    category: str | None
    subcategory: str | None
    status: str | None
    received_date: str | None
    severity: str | None
    unit: str | None


class Sale(BaseModel):
    document_id: str | None
    doc_type: str | None
    document_date: str | None
    recorded_date: str | None
    amount: float | None
    seller: str | None
    buyer: str | None
    unit: str | None


class Permit(BaseModel):
    job_type: str | None
    description: str | None
    status: str | None
    action_date: str | None
    estimated_cost: float | None
    existing_units: int | None
    proposed_units: int | None


class Violation(BaseModel):
    severity: str | None
    violation_type: str | None
    issue_date: str | None
    status: str | None
    description: str | None
    penalty: float | None
    balance_due: float | None


class Contact(BaseModel):
    role: str | None
    company: str | None
    first_name: str | None
    last_name: str | None
    registered_date: str | None


class Litigation(BaseModel):
    case_type: str | None
    opened_date: str | None
    status: str | None


class WatchlistItem(BaseModel):
    bbl: int
    added_at: str | None
    notes: str | None
    address: str | None
    borough: str | None
    zipcode: str | None
    year_built: int | None
    units_residential: int | None
    owner_name: str | None


class Alert(BaseModel):
    id: int
    bbl: int
    alert_type: str
    message: str | None
    detail: str | None
    created_at: str | None
    read: int
    address: str | None
    borough: str | None

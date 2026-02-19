export interface Building {
  bbl: number;
  address: string | null;
  zipcode: string | null;
  borough: string | null;
  year_built: number | null;
  num_floors: number | null;
  units_residential: number | null;
  units_total: number | null;
  owner_name: string | null;
  avg_unit_sqft: number | null;
  building_class: string | null;
  zoning?: string | null;
  assessed_total?: number | null;
  lot_area?: number | null;
  building_area?: number | null;
  residential_area?: number | null;
  commercial_area?: number | null;
  assessed_per_unit?: number | null;
}

export interface BuildingScore {
  bbl: number;
  gem_score: number;
  avail_score: number;
  combined: number;
  signal_count: number;
  convergence: string | null;
  rent_stabilized: boolean | number;
  stab_units: number;
  signals: string | null;
}

export interface Complaint {
  category: string | null;
  subcategory: string | null;
  status: string | null;
  received_date: string | null;
  severity: string | null;
  unit: string | null;
}

export interface ComplaintStats {
  total: number;
  by_year: { year: string; cnt: number }[];
  by_category: { category: string | null; cnt: number }[];
}

export interface Sale {
  document_id: string | null;
  doc_type: string | null;
  document_date: string | null;
  recorded_date: string | null;
  amount: number | null;
  seller: string | null;
  buyer: string | null;
  unit: string | null;
}

export interface Permit {
  job_type: string | null;
  description: string | null;
  status: string | null;
  action_date: string | null;
  estimated_cost: number | null;
  existing_units: number | null;
  proposed_units: number | null;
}

export interface Violation {
  severity: string | null;
  violation_type: string | null;
  issue_date: string | null;
  status: string | null;
  description: string | null;
  penalty: number | null;
  balance_due: number | null;
}

export interface Contact {
  role: string | null;
  company: string | null;
  first_name: string | null;
  last_name: string | null;
  registered_date: string | null;
}

export interface Litigation {
  case_type: string | null;
  opened_date: string | null;
  status: string | null;
}

export interface WatchlistItem {
  bbl: number;
  added_at: string | null;
  notes: string | null;
  address: string | null;
  borough: string | null;
  zipcode: string | null;
  year_built: number | null;
  units_residential: number | null;
  owner_name: string | null;
}

export interface Alert {
  id: number;
  bbl: number;
  alert_type: string;
  message: string | null;
  detail: string | null;
  created_at: string | null;
  read: number;
  address: string | null;
  borough: string | null;
}

export interface EllimanListing {
  core_listing_id: string;
  address: string | null;
  unit: string | null;
  list_price: number | null;
  close_price: number | null;
  bedrooms: number | null;
  bathrooms_total: number | null;
  living_area_sqft: number | null;
  listing_type: string | null;
  listing_status: string | null;
  list_date: string | null;
  close_date: string | null;
  price_per_sqft: number | null;
  neighborhood: string | null;
  borough: string | null;
}

export const BOROUGH_MAP: Record<string, string> = {
  MN: "Manhattan",
  BK: "Brooklyn",
  BX: "Bronx",
  QN: "Queens",
  SI: "Staten Island",
};

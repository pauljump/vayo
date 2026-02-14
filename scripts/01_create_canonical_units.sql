-- ============================================================================
-- CREATE CANONICAL UNITS TABLE
-- ============================================================================
-- This is the master table for all residential units in NYC
-- We'll populate it from multiple sources, then deduplicate

DROP TABLE IF EXISTS canonical_units;

CREATE TABLE canonical_units (
    -- Primary identifier
    unit_id TEXT PRIMARY KEY,  -- Format: BBL-UNIT or BIN-UNIT

    -- Location identifiers
    bbl TEXT,          -- Borough-Block-Lot
    bin TEXT,          -- Building Identification Number
    borough TEXT,      -- 1=MN, 2=BX, 3=BK, 4=QN, 5=SI
    block TEXT,
    lot TEXT,
    unit_number TEXT,  -- e.g., "3A", "1401", "PH-B"

    -- Address
    full_address TEXT,
    street_number TEXT,
    street_name TEXT,

    -- Unit characteristics (when known)
    bedrooms INTEGER,
    bathrooms REAL,
    square_feet INTEGER,
    floor INTEGER,

    -- Property type
    ownership_type TEXT,  -- 'condo', 'coop', 'rental', 'unknown'
    property_type TEXT,   -- From ACRIS property_type

    -- Metadata
    source_systems TEXT,  -- JSON array of where we found this unit
    confidence_score REAL DEFAULT 0.5,
    verified BOOLEAN DEFAULT 0,

    -- Counts (for quick reference)
    transaction_count INTEGER DEFAULT 0,
    complaint_count INTEGER DEFAULT 0,
    violation_count INTEGER DEFAULT 0,

    -- Timestamps
    first_discovered_date DATE,
    last_seen_date DATE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for fast lookups
CREATE INDEX idx_canonical_units_bbl ON canonical_units(bbl);
CREATE INDEX idx_canonical_units_bin ON canonical_units(bin);
CREATE INDEX idx_canonical_units_address ON canonical_units(full_address);
CREATE INDEX idx_canonical_units_unit_number ON canonical_units(unit_number);
CREATE INDEX idx_canonical_units_ownership ON canonical_units(ownership_type);

-- ============================================================================
-- TEMPORARY STAGING TABLES
-- ============================================================================

-- Stage 1: Extract units from ACRIS
DROP TABLE IF EXISTS units_from_acris;
CREATE TABLE units_from_acris AS
SELECT DISTINCT
    -- Create BBL from components (SQLite compatible padding)
    CASE
        WHEN borough IS NOT NULL AND block IS NOT NULL AND lot IS NOT NULL
        THEN borough ||
             SUBSTR('00000' || TRIM(block), -5, 5) ||
             SUBSTR('0000' || TRIM(lot), -4, 4)
        ELSE NULL
    END as bbl,
    NULL as bin,  -- ACRIS doesn't have BIN
    borough,
    block,
    lot,
    unit as unit_number,
    street_number,
    street_name,
    TRIM(COALESCE(street_number, '') || ' ' || COALESCE(street_name, '')) as full_address,
    property_type,
    'ACRIS' as source
FROM acris_real_property
WHERE unit IS NOT NULL
    AND unit != ''
    AND borough IS NOT NULL;

CREATE INDEX idx_units_acris_bbl ON units_from_acris(bbl);

-- Stage 2: Extract units from HPD complaints
DROP TABLE IF EXISTS units_from_hpd;
CREATE TABLE units_from_hpd AS
SELECT DISTINCT
    -- Construct BBL from borough, block, lot
    CASE
        WHEN boro_id IS NOT NULL AND block IS NOT NULL AND lot IS NOT NULL
        THEN boro_id ||
             SUBSTR('00000' || TRIM(block), -5, 5) ||
             SUBSTR('0000' || TRIM(lot), -4, 4)
        ELSE NULL
    END as bbl,
    bin,
    boro_id as borough,
    block,
    lot,
    apartment as unit_number,
    house_number as street_number,
    street_name,
    TRIM(COALESCE(house_number, '') || ' ' || COALESCE(street_name, '')) as full_address,
    NULL as property_type,
    'HPD_COMPLAINTS' as source
FROM complaints
WHERE apartment IS NOT NULL
    AND apartment != ''
    AND apartment != 'BLDG'  -- Exclude building-level complaints
    AND bin IS NOT NULL;

CREATE INDEX idx_units_hpd_bbl ON units_from_hpd(bbl);
CREATE INDEX idx_units_hpd_bin ON units_from_hpd(bin);

-- Stage 3: Extract units from DOB permits (where dwelling unit info exists)
DROP TABLE IF EXISTS units_from_dob;
CREATE TABLE units_from_dob AS
SELECT DISTINCT
    CASE
        WHEN borough IS NOT NULL AND block IS NOT NULL AND lot IS NOT NULL
        THEN borough ||
             SUBSTR('00000' || TRIM(block), -5, 5) ||
             SUBSTR('0000' || TRIM(lot), -4, 4)
        ELSE NULL
    END as bbl,
    bin,
    borough,
    block,
    lot,
    NULL as unit_number,  -- DOB permits don't list individual units
    house_number as street_number,
    street_name,
    TRIM(COALESCE(house_number, '') || ' ' || COALESCE(street_name, '')) as full_address,
    NULL as property_type,
    CAST(proposed_dwelling_units AS INTEGER) as unit_count,
    'DOB_PERMITS' as source
FROM dob_permits
WHERE proposed_dwelling_units IS NOT NULL
    AND proposed_dwelling_units != ''
    AND CAST(proposed_dwelling_units AS INTEGER) > 0;

CREATE INDEX idx_units_dob_bbl ON units_from_dob(bbl);
CREATE INDEX idx_units_dob_bin ON units_from_dob(bin);

SELECT 'Stage 1 complete: Units extracted to staging tables';
SELECT 'ACRIS units: ' || COUNT(*) FROM units_from_acris;
SELECT 'HPD units: ' || COUNT(*) FROM units_from_hpd;
SELECT 'DOB permits: ' || COUNT(*) FROM units_from_dob;

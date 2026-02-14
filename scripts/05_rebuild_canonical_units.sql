-- ============================================================================
-- REBUILD CANONICAL UNITS WITH PROPER BBL/BIN NORMALIZATION
-- ============================================================================

-- Step 0: Backup existing canonical_units
DROP TABLE IF EXISTS canonical_units_backup;
CREATE TABLE canonical_units_backup AS SELECT * FROM canonical_units;

-- Step 1: Drop and recreate canonical_units with clean schema
DROP TABLE IF EXISTS canonical_units;
CREATE TABLE canonical_units (
    unit_id TEXT PRIMARY KEY,
    bbl TEXT,
    bin TEXT,
    borough TEXT,
    unit_number TEXT,
    full_address TEXT,
    ownership_type TEXT,
    source_systems TEXT,
    confidence_score REAL DEFAULT 0.5,
    verified BOOLEAN DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_canonical_units_bbl ON canonical_units(bbl);
CREATE INDEX idx_canonical_units_bin ON canonical_units(bin);
CREATE INDEX idx_canonical_units_unit_number ON canonical_units(unit_number);
CREATE INDEX idx_canonical_bbl_unit ON canonical_units(bbl, unit_number);
CREATE INDEX idx_canonical_bin_unit ON canonical_units(bin, unit_number);

-- ============================================================================
-- STEP 2: Insert ACRIS units (highest quality)
-- ============================================================================

INSERT OR IGNORE INTO canonical_units (
    unit_id, bbl, bin, borough, unit_number, full_address,
    ownership_type, source_systems, confidence_score, verified
)
SELECT
    bbl || '-' || unit_number as unit_id,
    bbl,
    NULL as bin,
    borough,
    unit_number,
    full_address,
    CASE
        WHEN property_type LIKE '%CONDO%' THEN 'condo'
        WHEN property_type LIKE '%COOP%' THEN 'coop'
        ELSE 'rental'
    END as ownership_type,
    '["ACRIS"]' as source_systems,
    0.9 as confidence_score,
    1 as verified
FROM units_from_acris
WHERE bbl IS NOT NULL AND unit_number IS NOT NULL;

SELECT 'Inserted ' || changes() || ' ACRIS units';

-- ============================================================================
-- STEP 3: Insert HPD units (by BIN)
-- ============================================================================

INSERT OR IGNORE INTO canonical_units (
    unit_id, bbl, bin, borough, unit_number,
    source_systems, confidence_score, verified
)
SELECT
    bin || '-' || unit_number as unit_id,
    NULL as bbl,
    bin,
    borough,
    unit_number,
    '["HPD"]' as source_systems,
    0.7 as confidence_score,
    0 as verified
FROM units_from_hpd
WHERE bin IS NOT NULL AND unit_number IS NOT NULL
AND bin || '-' || unit_number NOT IN (SELECT unit_id FROM canonical_units);

SELECT 'Inserted ' || changes() || ' HPD units';

-- ============================================================================
-- STEP 4: Insert text-mined units (DOB, ECB, 311)
-- ============================================================================
-- These have BBLs like "BRONX0231600001" that need normalization

-- Helper: normalize borough+block+lot format BBLs
-- Format: "BRONX0231600001" -> "2002316-00001" (borough+5-digit block+5-digit lot)
INSERT OR IGNORE INTO canonical_units (
    unit_id, bbl, bin, unit_number, full_address,
    source_systems, confidence_score, verified
)
SELECT
    CASE
        WHEN t.bbl IS NOT NULL THEN t.bbl || '-' || t.unit_number
        WHEN t.bin IS NOT NULL THEN t.bin || '-' || t.unit_number
        ELSE t.source || '-' || t.source_id || '-' || t.unit_number
    END as unit_id,
    t.bbl,
    t.bin,
    t.unit_number,
    t.address as full_address,
    '["TEXT_MINED_' || t.source || '"]' as source_systems,
    0.6 as confidence_score,
    0 as verified
FROM text_mined_units t
WHERE t.unit_number IS NOT NULL
AND (t.bbl IS NOT NULL OR t.bin IS NOT NULL OR t.source_id IS NOT NULL);

SELECT 'Inserted ' || changes() || ' text-mined units';

-- ============================================================================
-- STEP 5: Fill in missing BINs from buildings table
-- ============================================================================

UPDATE canonical_units
SET bin = (
    SELECT b.bin
    FROM buildings b
    WHERE b.bbl = canonical_units.bbl
    LIMIT 1
)
WHERE canonical_units.bin IS NULL
AND canonical_units.bbl IS NOT NULL
AND EXISTS (SELECT 1 FROM buildings b WHERE b.bbl = canonical_units.bbl);

SELECT 'Filled BINs for ' || changes() || ' units';

-- ============================================================================
-- STEP 6: Normalize BBLs to match PLUTO format
-- ============================================================================
-- PLUTO uses: 1000010010 (10 digits, no decimals)
-- Our sources use various formats

-- Create a normalized BBL view for matching
DROP VIEW IF EXISTS canonical_units_normalized;
CREATE VIEW canonical_units_normalized AS
SELECT
    unit_id,
    -- Normalize BBL to 10-digit format matching PLUTO
    CASE
        WHEN bbl IS NOT NULL AND LENGTH(bbl) = 10 THEN bbl
        WHEN bbl IS NOT NULL AND LENGTH(bbl) > 10 THEN SUBSTR(bbl, 1, 10)
        ELSE NULL
    END as bbl_normalized,
    bbl,
    bin,
    borough,
    unit_number,
    full_address,
    ownership_type,
    source_systems,
    confidence_score
FROM canonical_units;

-- ============================================================================
-- FINAL STATS
-- ============================================================================

SELECT '';
SELECT '=== REBUILD COMPLETE ===';
SELECT '';
SELECT 'Total units:' as metric, COUNT(*) as count FROM canonical_units;
SELECT 'Units with BBL:' as metric, COUNT(*) as count FROM canonical_units WHERE bbl IS NOT NULL;
SELECT 'Units with BIN:' as metric, COUNT(*) as count FROM canonical_units WHERE bin IS NOT NULL;
SELECT 'Units with both BBL+BIN:' as metric, COUNT(*) as count FROM canonical_units WHERE bbl IS NOT NULL AND bin IS NOT NULL;
SELECT '';
SELECT 'By source:' as metric;
SELECT 'ACRIS:' as source, COUNT(*) as count FROM canonical_units WHERE source_systems LIKE '%ACRIS%';
SELECT 'HPD:' as source, COUNT(*) as count FROM canonical_units WHERE source_systems LIKE '%HPD%';
SELECT 'Text mined:' as source, COUNT(*) as count FROM canonical_units WHERE source_systems LIKE '%TEXT_MINED%';
SELECT '';
SELECT 'Unique units by BBL+unit:' as metric, COUNT(DISTINCT bbl || '-' || unit_number) as count FROM canonical_units WHERE bbl IS NOT NULL;
SELECT 'Unique units by BIN+unit:' as metric, COUNT(DISTINCT bin || '-' || unit_number) as count FROM canonical_units WHERE bin IS NOT NULL;

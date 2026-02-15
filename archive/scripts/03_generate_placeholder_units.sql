-- ============================================================================
-- GENERATE PLACEHOLDER UNITS FOR BUILDINGS
-- ============================================================================
-- For buildings where we know the total unit count but haven't discovered
-- individual unit numbers yet, create placeholder unit records
--
-- Strategy:
-- 1. Find buildings with num_units > 0
-- 2. Count how many units we've already discovered for that building
-- 3. If discovered < num_units, create placeholders for the gap
-- ============================================================================

DROP TABLE IF EXISTS placeholder_units;

CREATE TABLE placeholder_units (
    unit_id TEXT PRIMARY KEY,
    bbl TEXT,
    bin TEXT,
    building_address TEXT,
    unit_number TEXT,
    unit_sequence INTEGER,
    is_placeholder BOOLEAN DEFAULT 1,
    source TEXT DEFAULT 'INFERRED_FROM_BUILDING_RECORD',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- Find buildings with unit count gaps
-- ============================================================================

DROP TABLE IF EXISTS building_unit_gaps;

CREATE TABLE building_unit_gaps AS
SELECT
    b.bin,
    b.bbl,
    b.address,
    b.borough,
    b.num_units as total_units,
    COALESCE(
        (SELECT COUNT(*)
         FROM canonical_units cu
         WHERE cu.bbl = b.bbl
            OR cu.bin = b.bin
        ), 0
    ) as discovered_units,
    b.num_units - COALESCE(
        (SELECT COUNT(*)
         FROM canonical_units cu
         WHERE cu.bbl = b.bbl
            OR cu.bin = b.bin
        ), 0
    ) as missing_units
FROM buildings b
WHERE b.num_units > 0
    AND b.num_units - COALESCE(
        (SELECT COUNT(*)
         FROM canonical_units cu
         WHERE cu.bbl = b.bbl
            OR cu.bin = b.bin
        ), 0
    ) > 0
ORDER BY missing_units DESC;

SELECT 'Found ' || COUNT(*) || ' buildings with missing unit records' FROM building_unit_gaps;
SELECT 'Total missing units: ' || SUM(missing_units) FROM building_unit_gaps;

-- ============================================================================
-- Generate placeholder units
-- ============================================================================
-- Note: We'll create placeholders with unit_number as "UNIT_001", "UNIT_002", etc.
-- These will get replaced when we discover actual unit numbers from listings

-- SQLite doesn't have great loop support, so we'll use a recursive CTE
-- to generate sequences

DROP TABLE IF EXISTS number_sequence;

CREATE TABLE number_sequence AS
WITH RECURSIVE cnt(n) AS (
    SELECT 1
    UNION ALL
    SELECT n+1 FROM cnt
    WHERE n < (SELECT MAX(missing_units) FROM building_unit_gaps WHERE missing_units < 1000)
)
SELECT n FROM cnt;

-- Now insert placeholder units
INSERT INTO placeholder_units (
    unit_id,
    bbl,
    bin,
    building_address,
    unit_number,
    unit_sequence
)
SELECT
    COALESCE(g.bin, g.bbl) || '-PLACEHOLDER-' || SUBSTR('0000' || n.n, -4, 4) as unit_id,
    g.bbl,
    g.bin,
    g.address,
    'UNIT_' || SUBSTR('0000' || n.n, -4, 4) as unit_number,
    n.n as unit_sequence
FROM building_unit_gaps g
JOIN number_sequence n ON n.n <= g.missing_units
WHERE g.missing_units < 1000  -- Safety limit: only for buildings with < 1000 units
    AND g.missing_units > 0;

SELECT 'Created ' || COUNT(*) || ' placeholder units' FROM placeholder_units;

-- ============================================================================
-- Summary stats on placeholders
-- ============================================================================

SELECT 'PLACEHOLDER GENERATION COMPLETE';
SELECT '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━';
SELECT 'Placeholder units created: ' || COUNT(*) FROM placeholder_units;
SELECT 'Buildings with placeholders: ' || COUNT(DISTINCT bin) FROM placeholder_units;
SELECT '';
SELECT 'Sample placeholders (first 10):';
SELECT bin, building_address, unit_number
FROM placeholder_units
LIMIT 10;

-- ============================================================================
-- Note: We DON'T insert these into canonical_units yet
-- ============================================================================
-- These placeholders will be inserted into canonical_units later
-- once we've exhausted all discovery methods (listings, permits, etc.)
-- They're kept separate so we can track which units are "real" vs "inferred"

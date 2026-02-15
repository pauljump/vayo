-- ============================================================================
-- CALCULATE REAL COVERAGE AGAINST PLUTO
-- ============================================================================

-- Step 1: Count truly unique units (deduplicating across BBL and BIN)
SELECT '=== UNIQUE UNIT COUNT ===' as report;

WITH unique_by_bbl AS (
    SELECT DISTINCT bbl, unit_number
    FROM canonical_units
    WHERE bbl IS NOT NULL AND bbl != ''
),
unique_by_bin AS (
    SELECT DISTINCT bin, unit_number
    FROM canonical_units
    WHERE bin IS NOT NULL AND bin != ''
    AND (bbl IS NULL OR bbl = '')  -- Only count if not already counted by BBL
),
all_unique AS (
    SELECT bbl || '-' || unit_number as identifier FROM unique_by_bbl
    UNION
    SELECT bin || '-' || unit_number as identifier FROM unique_by_bin
)
SELECT 'Truly unique units:' as metric, COUNT(*) as count FROM all_unique;

-- Step 2: Match our units to PLUTO buildings
SELECT '';
SELECT '=== PLUTO MATCHING ===' as report;

-- Normalize PLUTO BBLs (remove decimals)
SELECT 'Total PLUTO buildings with units:' as metric, COUNT(*) as count
FROM pluto
WHERE unitsres > 0;

SELECT 'Total units in PLUTO:' as metric, SUM(unitsres) as count
FROM pluto
WHERE unitsres > 0;

-- Count how many PLUTO BBLs we have units for
SELECT 'PLUTO buildings we have units for:' as metric, COUNT(DISTINCT p.bbl) as count
FROM pluto p
WHERE p.unitsres > 0
AND CAST(p.bbl AS INTEGER) IN (
    SELECT CAST(bbl AS INTEGER)
    FROM canonical_units
    WHERE bbl IS NOT NULL AND bbl != ''
);

-- Step 3: Calculate coverage by comparing unique units to PLUTO
SELECT '';
SELECT '=== COVERAGE CALCULATION ===' as report;

-- This is an approximation since we can't perfectly match individual units to PLUTO
-- PLUTO gives us building-level unit counts, we have individual units
WITH our_units AS (
    SELECT COUNT(DISTINCT bbl || '-' || unit_number) as count
    FROM canonical_units
    WHERE bbl IS NOT NULL AND bbl != ''
),
pluto_total AS (
    SELECT SUM(unitsres) as count
    FROM pluto
    WHERE unitsres > 0
)
SELECT
    (SELECT count FROM our_units) as discovered_units,
    (SELECT count FROM pluto_total) as pluto_total_units,
    ROUND(100.0 * (SELECT count FROM our_units) / (SELECT count FROM pluto_total), 1) || '%' as coverage_pct;

-- Step 4: Coverage by borough
SELECT '';
SELECT '=== COVERAGE BY BOROUGH ===' as report;

-- Map borough codes
WITH borough_coverage AS (
    SELECT
        CASE p.borough
            WHEN 'MN' THEN 1
            WHEN 'BX' THEN 2
            WHEN 'BK' THEN 3
            WHEN 'QN' THEN 4
            WHEN 'SI' THEN 5
        END as boro_code,
        p.borough,
        SUM(p.unitsres) as pluto_units,
        (
            SELECT COUNT(DISTINCT unit_id)
            FROM canonical_units c
            WHERE c.bbl IS NOT NULL
            AND SUBSTR(c.bbl, 1, 1) = CAST(CASE p.borough
                WHEN 'MN' THEN 1
                WHEN 'BX' THEN 2
                WHEN 'BK' THEN 3
                WHEN 'QN' THEN 4
                WHEN 'SI' THEN 5
            END AS TEXT)
        ) as discovered_units
    FROM pluto p
    WHERE p.unitsres > 0
    GROUP BY p.borough
)
SELECT
    borough,
    pluto_units,
    discovered_units,
    ROUND(100.0 * discovered_units / pluto_units, 1) || '%' as coverage
FROM borough_coverage
ORDER BY pluto_units DESC;

-- Step 5: What's missing - building size analysis
SELECT '';
SELECT '=== MISSING UNITS BY BUILDING SIZE ===' as report;

SELECT
    CASE
        WHEN p.unitsres = 1 THEN '1 unit'
        WHEN p.unitsres BETWEEN 2 AND 5 THEN '2-5 units'
        WHEN p.unitsres BETWEEN 6 AND 20 THEN '6-20 units'
        WHEN p.unitsres BETWEEN 21 AND 50 THEN '21-50 units'
        WHEN p.unitsres BETWEEN 51 AND 200 THEN '51-200 units'
        ELSE '200+ units'
    END as building_size,
    COUNT(*) as num_buildings,
    SUM(p.unitsres) as total_units_in_pluto
FROM pluto p
WHERE p.unitsres > 0
AND CAST(p.bbl AS INTEGER) NOT IN (
    SELECT DISTINCT CAST(bbl AS INTEGER)
    FROM canonical_units
    WHERE bbl IS NOT NULL AND bbl != ''
)
GROUP BY 1
ORDER BY SUM(p.unitsres) DESC;

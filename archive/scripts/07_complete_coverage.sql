-- ============================================================================
-- COMPLETE NYC COVERAGE v2: PLUTO-anchored, with condo lot mapping
-- ============================================================================
-- Fix: ACRIS condo units have per-unit lot numbers (1001-7499) that don't
-- match PLUTO's building-level BBL. Map them via boro+block.
-- ============================================================================

-- Step 1: Create normalized PLUTO reference
DROP TABLE IF EXISTS pluto_norm;
CREATE TABLE pluto_norm AS
SELECT
    CAST(bbl AS INTEGER) as bbl,
    CAST(CAST(bbl AS INTEGER) AS TEXT) as bbl_text,
    SUBSTR(CAST(CAST(bbl AS INTEGER) AS TEXT), 1, 6) as boro_block,
    borough,
    address,
    zipcode,
    bldgclass,
    landuse,
    yearbuilt,
    numfloors,
    unitsres,
    unitstotal,
    ownername,
    assesstot,
    zonedist1
FROM pluto
WHERE unitsres > 0;

CREATE INDEX idx_pn_bbl ON pluto_norm(bbl);
CREATE INDEX idx_pn_bb ON pluto_norm(boro_block);
SELECT 'PLUTO: ' || COUNT(*) || ' buildings, ' || SUM(unitsres) || ' units' FROM pluto_norm;

-- Step 2: Normalize canonical_units BBLs and map condos
DROP TABLE IF EXISTS cu_mapped;
CREATE TABLE cu_mapped AS
SELECT
    c.unit_id,
    c.bbl as original_bbl,
    c.bin,
    c.borough as original_borough,
    c.unit_number,
    c.full_address,
    c.ownership_type,
    c.source_systems,
    c.confidence_score,
    -- Try direct BBL match first
    COALESCE(
        -- Direct match
        (SELECT p.bbl FROM pluto_norm p WHERE p.bbl = CAST(c.bbl AS INTEGER) LIMIT 1),
        -- Condo: match via boro+block to the building with most units
        (SELECT p.bbl FROM pluto_norm p
         WHERE p.boro_block = SUBSTR(c.bbl, 1, 6)
         AND LENGTH(c.bbl) = 10
         AND CAST(SUBSTR(c.bbl, 7, 4) AS INTEGER) >= 1000
         ORDER BY p.unitsres DESC
         LIMIT 1)
    ) as matched_pluto_bbl
FROM canonical_units c
WHERE c.bbl IS NOT NULL
  AND LENGTH(c.bbl) = 10
  AND c.bbl NOT IN ('0000000000');

CREATE INDEX idx_cum_mpb ON cu_mapped(matched_pluto_bbl);
CREATE INDEX idx_cum_ob ON cu_mapped(original_bbl);

SELECT 'Mapped ' || COUNT(*) || ' units total' FROM cu_mapped;
SELECT 'Direct BBL match: ' || COUNT(*) FROM cu_mapped WHERE matched_pluto_bbl = CAST(original_bbl AS INTEGER);
SELECT 'Condo block match: ' || COUNT(*) FROM cu_mapped WHERE matched_pluto_bbl IS NOT NULL AND matched_pluto_bbl != CAST(original_bbl AS INTEGER);
SELECT 'Unmatched: ' || COUNT(*) FROM cu_mapped WHERE matched_pluto_bbl IS NULL;

-- Step 3: Count discovered units per PLUTO building (after mapping)
DROP TABLE IF EXISTS bldg_discovered;
CREATE TABLE bldg_discovered AS
SELECT
    matched_pluto_bbl as bbl,
    COUNT(*) as discovered
FROM cu_mapped
WHERE matched_pluto_bbl IS NOT NULL
GROUP BY matched_pluto_bbl;

CREATE INDEX idx_bd_bbl ON bldg_discovered(bbl);

-- Also count BIN-based units from canonical_units
-- (units with BIN but no BBL or unmatched BBL)
DROP TABLE IF EXISTS bin_to_pluto;
CREATE TABLE bin_to_pluto AS
SELECT DISTINCT
    b.bin,
    CAST(p.bbl AS INTEGER) as pluto_bbl
FROM buildings b
JOIN pluto_norm p ON p.bbl = CAST(b.bbl AS INTEGER)
WHERE b.bin IS NOT NULL AND b.bbl IS NOT NULL;

CREATE INDEX idx_b2p_bin ON bin_to_pluto(bin);

-- Count BIN-matched units not already counted
INSERT OR REPLACE INTO bldg_discovered (bbl, discovered)
SELECT
    COALESCE(bd.bbl, bp.pluto_bbl) as bbl,
    COALESCE(bd.discovered, 0) + bin_count.cnt as discovered
FROM (
    SELECT bp2.pluto_bbl, COUNT(*) as cnt
    FROM canonical_units c
    JOIN bin_to_pluto bp2 ON bp2.bin = c.bin
    WHERE c.bbl IS NULL OR LENGTH(c.bbl) != 10 OR c.bbl = '0000000000'
    GROUP BY bp2.pluto_bbl
) bin_count
JOIN bin_to_pluto bp ON bp.pluto_bbl = bin_count.pluto_bbl
LEFT JOIN bldg_discovered bd ON bd.bbl = bin_count.pluto_bbl
GROUP BY COALESCE(bd.bbl, bp.pluto_bbl);

SELECT 'Buildings with discovered data: ' || COUNT(*) FROM bldg_discovered;

-- Step 4: Build gap analysis
DROP TABLE IF EXISTS building_gaps;
CREATE TABLE building_gaps AS
SELECT
    p.bbl,
    p.bbl_text,
    p.borough,
    p.address,
    p.zipcode,
    p.bldgclass,
    p.yearbuilt,
    p.numfloors,
    p.unitsres as expected_units,
    COALESCE(d.discovered, 0) as discovered_units,
    CASE
        WHEN COALESCE(d.discovered, 0) >= p.unitsres THEN 0
        ELSE p.unitsres - COALESCE(d.discovered, 0)
    END as gap
FROM pluto_norm p
LEFT JOIN bldg_discovered d ON d.bbl = p.bbl;

SELECT '';
SELECT '=== GAP ANALYSIS ===';
SELECT 'Fully covered (discovered >= expected): ' || COUNT(*) FROM building_gaps WHERE gap = 0;
SELECT 'Partially covered: ' || COUNT(*) FROM building_gaps WHERE gap > 0 AND discovered_units > 0;
SELECT 'Zero coverage: ' || COUNT(*) FROM building_gaps WHERE discovered_units = 0;
SELECT 'Total placeholder units needed: ' || SUM(gap) FROM building_gaps WHERE gap > 0;

-- Step 5: Build the final table
DROP TABLE IF EXISTS all_nyc_units;
CREATE TABLE all_nyc_units (
    unit_id TEXT PRIMARY KEY,
    bbl TEXT NOT NULL,
    borough TEXT,
    address TEXT,
    zipcode TEXT,
    unit_number TEXT,
    is_placeholder BOOLEAN DEFAULT 0,
    source_systems TEXT,
    confidence_score REAL DEFAULT 0.5,
    ownership_type TEXT,
    bldgclass TEXT,
    yearbuilt INTEGER,
    numfloors REAL
);

-- 5a: Insert discovered units matched to PLUTO
INSERT INTO all_nyc_units (
    unit_id, bbl, borough, address, zipcode, unit_number,
    is_placeholder, source_systems, confidence_score, ownership_type,
    bldgclass, yearbuilt, numfloors
)
SELECT
    m.unit_id,
    CAST(m.matched_pluto_bbl AS TEXT),
    p.borough,
    COALESCE(m.full_address, p.address),
    p.zipcode,
    m.unit_number,
    0,
    m.source_systems,
    m.confidence_score,
    m.ownership_type,
    p.bldgclass,
    p.yearbuilt,
    p.numfloors
FROM cu_mapped m
JOIN pluto_norm p ON p.bbl = m.matched_pluto_bbl
WHERE m.matched_pluto_bbl IS NOT NULL;

SELECT '5a: ' || COUNT(*) || ' discovered units (BBL-matched)' FROM all_nyc_units WHERE is_placeholder = 0;

-- 5b: Insert BIN-matched discovered units
INSERT OR IGNORE INTO all_nyc_units (
    unit_id, bbl, borough, address, zipcode, unit_number,
    is_placeholder, source_systems, confidence_score, ownership_type,
    bldgclass, yearbuilt, numfloors
)
SELECT
    c.unit_id,
    CAST(bp.pluto_bbl AS TEXT),
    p.borough,
    COALESCE(c.full_address, p.address),
    p.zipcode,
    c.unit_number,
    0,
    c.source_systems,
    c.confidence_score,
    c.ownership_type,
    p.bldgclass,
    p.yearbuilt,
    p.numfloors
FROM canonical_units c
JOIN bin_to_pluto bp ON bp.bin = c.bin
JOIN pluto_norm p ON p.bbl = bp.pluto_bbl
WHERE (c.bbl IS NULL OR LENGTH(c.bbl) != 10 OR c.bbl = '0000000000');

SELECT '5b: ' || changes() || ' BIN-matched units added';

-- 5c: Generate placeholders
DROP TABLE IF EXISTS num_seq;
CREATE TABLE num_seq AS
WITH RECURSIVE cnt(n) AS (
    SELECT 1 UNION ALL SELECT n + 1 FROM cnt WHERE n < 22000
)
SELECT n FROM cnt;

INSERT INTO all_nyc_units (
    unit_id, bbl, borough, address, zipcode, unit_number,
    is_placeholder, source_systems, confidence_score,
    bldgclass, yearbuilt, numfloors
)
SELECT
    g.bbl_text || '-PH-' || SUBSTR('00000' || s.n, -5, 5),
    g.bbl_text,
    g.borough,
    g.address,
    g.zipcode,
    'UNIT_' || SUBSTR('00000' || (g.discovered_units + s.n), -5, 5),
    1,
    '["PLUTO_INFERRED"]',
    0.3,
    g.bldgclass,
    g.yearbuilt,
    g.numfloors
FROM building_gaps g
JOIN num_seq s ON s.n <= g.gap
WHERE g.gap > 0;

SELECT '5c: ' || COUNT(*) || ' placeholder units generated' FROM all_nyc_units WHERE is_placeholder = 1;

-- Step 6: Create indexes
CREATE INDEX idx_anu_bbl ON all_nyc_units(bbl);
CREATE INDEX idx_anu_borough ON all_nyc_units(borough);
CREATE INDEX idx_anu_zip ON all_nyc_units(zipcode);
CREATE INDEX idx_anu_placeholder ON all_nyc_units(is_placeholder);
CREATE INDEX idx_anu_bldgclass ON all_nyc_units(bldgclass);

-- Step 7: Final report
SELECT '';
SELECT '════════════════════════════════════════════════════';
SELECT '  ALL NYC UNITS - COMPLETE COVERAGE REPORT v2';
SELECT '════════════════════════════════════════════════════';
SELECT '';
SELECT 'Total units: ' || COUNT(*) FROM all_nyc_units;
SELECT 'Real (discovered): ' || COUNT(*) FROM all_nyc_units WHERE is_placeholder = 0;
SELECT 'Placeholder (inferred): ' || COUNT(*) FROM all_nyc_units WHERE is_placeholder = 1;
SELECT 'Unique buildings: ' || COUNT(DISTINCT bbl) FROM all_nyc_units;
SELECT '';
SELECT '--- BY BOROUGH ---';
SELECT
    borough,
    COUNT(*) as total_units,
    SUM(CASE WHEN is_placeholder = 0 THEN 1 ELSE 0 END) as real_units,
    ROUND(100.0 * SUM(CASE WHEN is_placeholder = 0 THEN 1 ELSE 0 END) / COUNT(*), 1) || '%' as real_pct
FROM all_nyc_units
GROUP BY borough
ORDER BY total_units DESC;

SELECT '';
SELECT '--- VS PLUTO TARGET ---';
WITH our_totals AS (
    SELECT borough, COUNT(*) as total FROM all_nyc_units GROUP BY borough
),
pluto_totals AS (
    SELECT borough, SUM(unitsres) as total FROM pluto_norm GROUP BY borough
)
SELECT
    p.borough,
    p.total as pluto_units,
    o.total as our_units,
    ROUND(100.0 * o.total / p.total, 1) || '%' as coverage
FROM pluto_totals p
JOIN our_totals o ON o.borough = p.borough
ORDER BY p.total DESC;

SELECT '';
SELECT '--- BY SOURCE ---';
SELECT
    CASE
        WHEN source_systems LIKE '%ACRIS%' THEN 'ACRIS'
        WHEN source_systems LIKE '%HPD%' THEN 'HPD'
        WHEN source_systems LIKE '%TEXT_MINED%' THEN 'Text Mining'
        WHEN source_systems LIKE '%PLUTO%' THEN 'PLUTO Inferred'
        ELSE 'Other'
    END as source,
    COUNT(*) as units
FROM all_nyc_units
GROUP BY 1
ORDER BY units DESC;

-- Cleanup temp tables
DROP TABLE IF EXISTS pluto_norm;
DROP TABLE IF EXISTS cu_mapped;
DROP TABLE IF EXISTS bldg_discovered;
DROP TABLE IF EXISTS bin_to_pluto;
DROP TABLE IF EXISTS building_gaps;
DROP TABLE IF EXISTS num_seq;

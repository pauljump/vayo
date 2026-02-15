-- ============================================================================
-- COMPLETE NYC COVERAGE v3: Fast, PLUTO-anchored
-- ============================================================================
-- Approach: Use cu_mapped (already created) + direct gap fill from PLUTO
-- Skip expensive BIN cross-referencing
-- ============================================================================

-- Check if cu_mapped already exists from v2 partial run
SELECT 'cu_mapped rows: ' || COUNT(*) FROM cu_mapped;
SELECT 'all_nyc_units exists: ' || COUNT(*) FROM all_nyc_units;

-- If all_nyc_units is partially built, drop and restart
DROP TABLE IF EXISTS all_nyc_units;

-- Rebuild pluto_norm (was cleaned up)
DROP TABLE IF EXISTS pluto_norm;
CREATE TABLE pluto_norm AS
SELECT
    CAST(bbl AS INTEGER) as bbl,
    CAST(CAST(bbl AS INTEGER) AS TEXT) as bbl_text,
    borough,
    address,
    zipcode,
    bldgclass,
    yearbuilt,
    numfloors,
    unitsres,
    ownername,
    zonedist1
FROM pluto
WHERE unitsres > 0;
CREATE INDEX idx_pn_bbl ON pluto_norm(bbl);
SELECT 'pluto_norm: ' || COUNT(*) || ' buildings' FROM pluto_norm;

-- Count discovered per PLUTO building (using cu_mapped from v2)
DROP TABLE IF EXISTS bldg_disc;
CREATE TABLE bldg_disc AS
SELECT matched_pluto_bbl as bbl, COUNT(*) as discovered
FROM cu_mapped
WHERE matched_pluto_bbl IS NOT NULL
GROUP BY matched_pluto_bbl;
CREATE INDEX idx_bdisc ON bldg_disc(bbl);
SELECT 'Buildings with discovered units: ' || COUNT(*) FROM bldg_disc;

-- Gap analysis
DROP TABLE IF EXISTS gaps;
CREATE TABLE gaps AS
SELECT
    p.bbl, p.bbl_text, p.borough, p.address, p.zipcode,
    p.bldgclass, p.yearbuilt, p.numfloors, p.unitsres,
    COALESCE(d.discovered, 0) as discovered,
    CASE WHEN COALESCE(d.discovered, 0) >= p.unitsres THEN 0
         ELSE p.unitsres - COALESCE(d.discovered, 0) END as gap
FROM pluto_norm p
LEFT JOIN bldg_disc d ON d.bbl = p.bbl;

SELECT 'Fully covered: ' || COUNT(*) FROM gaps WHERE gap = 0;
SELECT 'Partially covered: ' || COUNT(*) FROM gaps WHERE gap > 0 AND discovered > 0;
SELECT 'Zero coverage: ' || COUNT(*) FROM gaps WHERE discovered = 0;
SELECT 'Placeholders needed: ' || SUM(gap) FROM gaps WHERE gap > 0;

-- Build the final table
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

-- Insert discovered units
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

SELECT 'Discovered units inserted: ' || COUNT(*) FROM all_nyc_units;

-- Generate number sequence
DROP TABLE IF EXISTS num_seq;
CREATE TABLE num_seq AS
WITH RECURSIVE cnt(n) AS (
    SELECT 1 UNION ALL SELECT n+1 FROM cnt WHERE n < 22000
) SELECT n FROM cnt;

-- Insert placeholders
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
    'UNIT_' || SUBSTR('00000' || (g.discovered + s.n), -5, 5),
    1,
    '["PLUTO_INFERRED"]',
    0.3,
    g.bldgclass,
    g.yearbuilt,
    g.numfloors
FROM gaps g
JOIN num_seq s ON s.n <= g.gap
WHERE g.gap > 0;

SELECT 'Placeholders inserted: ' || COUNT(*) FROM all_nyc_units WHERE is_placeholder = 1;

-- Indexes
CREATE INDEX idx_anu_bbl ON all_nyc_units(bbl);
CREATE INDEX idx_anu_borough ON all_nyc_units(borough);
CREATE INDEX idx_anu_zip ON all_nyc_units(zipcode);
CREATE INDEX idx_anu_ph ON all_nyc_units(is_placeholder);

-- Final report
SELECT '';
SELECT '════════════════════════════════════════════════════';
SELECT '  ALL NYC UNITS - COMPLETE COVERAGE';
SELECT '════════════════════════════════════════════════════';
SELECT '';
SELECT 'Total units: ' || COUNT(*) FROM all_nyc_units;
SELECT 'Real (discovered): ' || COUNT(*) FROM all_nyc_units WHERE is_placeholder = 0;
SELECT 'Placeholder (PLUTO-inferred): ' || COUNT(*) FROM all_nyc_units WHERE is_placeholder = 1;
SELECT 'Unique buildings: ' || COUNT(DISTINCT bbl) FROM all_nyc_units;
SELECT '';
SELECT '--- BY BOROUGH ---';
SELECT
    borough,
    COUNT(*) as total,
    SUM(CASE WHEN is_placeholder = 0 THEN 1 ELSE 0 END) as real,
    SUM(CASE WHEN is_placeholder = 1 THEN 1 ELSE 0 END) as placeholder,
    ROUND(100.0 * SUM(CASE WHEN is_placeholder = 0 THEN 1 ELSE 0 END) / COUNT(*), 1) || '%' as real_pct
FROM all_nyc_units
GROUP BY borough
ORDER BY total DESC;

SELECT '';
SELECT '--- VS PLUTO ---';
SELECT
    p.borough,
    SUM(p.unitsres) as pluto_target,
    (SELECT COUNT(*) FROM all_nyc_units a WHERE a.borough = p.borough) as our_total,
    ROUND(100.0 * (SELECT COUNT(*) FROM all_nyc_units a WHERE a.borough = p.borough) / SUM(p.unitsres), 1) || '%' as pct
FROM pluto_norm p
GROUP BY p.borough
ORDER BY SUM(p.unitsres) DESC;

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

-- Cleanup
DROP TABLE IF EXISTS pluto_norm;
DROP TABLE IF EXISTS bldg_disc;
DROP TABLE IF EXISTS gaps;
DROP TABLE IF EXISTS num_seq;

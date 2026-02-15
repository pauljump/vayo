#!/bin/bash
# ============================================================================
# Export data to a new, small database and build complete coverage there
# ============================================================================

set -e
BIG_DB="/Users/pjump/Desktop/projects/vayo/stuy-scrape-csv/stuytown.db"
NEW_DB="/Users/pjump/Desktop/projects/vayo/all_nyc_units.db"

echo "=== Step 1: Export PLUTO buildings ==="
rm -f "$NEW_DB"

# Export pluto (residential buildings only)
sqlite3 "$BIG_DB" "
ATTACH '$NEW_DB' AS new;
CREATE TABLE new.pluto AS
SELECT
    CAST(bbl AS INTEGER) as bbl,
    CAST(CAST(bbl AS INTEGER) AS TEXT) as bbl_text,
    SUBSTR(CAST(CAST(bbl AS INTEGER) AS TEXT), 1, 6) as boro_block,
    borough, address, zipcode, bldgclass, yearbuilt,
    numfloors, unitsres, unitstotal, ownername, zonedist1,
    assesstot, lotarea, bldgarea, resarea
FROM pluto WHERE unitsres > 0;
"
echo "PLUTO exported"

echo "=== Step 2: Export canonical_units ==="
sqlite3 "$BIG_DB" "
ATTACH '$NEW_DB' AS new;
CREATE TABLE new.canonical_units AS
SELECT unit_id, bbl, bin, borough, unit_number, full_address,
       ownership_type, source_systems, confidence_score, verified
FROM canonical_units
WHERE bbl IS NOT NULL AND LENGTH(bbl) = 10 AND bbl != '0000000000';
"
echo "canonical_units exported"

echo "=== Step 3: Build indexes and coverage in new DB ==="
sqlite3 -header -column "$NEW_DB" <<'SQLEOF'
-- Indexes
CREATE INDEX idx_pluto_bbl ON pluto(bbl);
CREATE INDEX idx_pluto_bb ON pluto(boro_block);
CREATE INDEX idx_cu_bbl ON canonical_units(bbl);

SELECT 'PLUTO buildings: ' || COUNT(*) FROM pluto;
SELECT 'Canonical units: ' || COUNT(*) FROM canonical_units;

-- Map canonical units to PLUTO BBLs (including condo lot mapping)
CREATE TABLE cu_mapped AS
SELECT
    c.unit_id,
    c.bbl as original_bbl,
    c.unit_number,
    c.full_address,
    c.ownership_type,
    c.source_systems,
    c.confidence_score,
    COALESCE(
        (SELECT p.bbl FROM pluto p WHERE p.bbl = CAST(c.bbl AS INTEGER) LIMIT 1),
        (SELECT p.bbl FROM pluto p
         WHERE p.boro_block = SUBSTR(c.bbl, 1, 6)
         AND CAST(SUBSTR(c.bbl, 7, 4) AS INTEGER) >= 1000
         ORDER BY p.unitsres DESC LIMIT 1)
    ) as matched_bbl
FROM canonical_units c;

CREATE INDEX idx_cum ON cu_mapped(matched_bbl);
SELECT 'Mapped: ' || COUNT(*) || ' total, ' ||
       SUM(CASE WHEN matched_bbl IS NOT NULL THEN 1 ELSE 0 END) || ' matched' FROM cu_mapped;

-- Count discovered per building
CREATE TABLE bldg_disc AS
SELECT matched_bbl as bbl, COUNT(*) as discovered
FROM cu_mapped WHERE matched_bbl IS NOT NULL
GROUP BY matched_bbl;
CREATE INDEX idx_bd ON bldg_disc(bbl);

-- Gap analysis
CREATE TABLE gaps AS
SELECT p.bbl, p.bbl_text, p.borough, p.address, p.zipcode,
       p.bldgclass, p.yearbuilt, p.numfloors, p.unitsres,
       COALESCE(d.discovered, 0) as discovered,
       CASE WHEN COALESCE(d.discovered, 0) >= p.unitsres THEN 0
            ELSE p.unitsres - COALESCE(d.discovered, 0) END as gap
FROM pluto p LEFT JOIN bldg_disc d ON d.bbl = p.bbl;

SELECT '';
SELECT 'Fully covered: ' || COUNT(*) FROM gaps WHERE gap = 0;
SELECT 'Partial: ' || COUNT(*) FROM gaps WHERE gap > 0 AND discovered > 0;
SELECT 'Zero: ' || COUNT(*) FROM gaps WHERE discovered = 0;
SELECT 'Placeholders needed: ' || SUM(gap) FROM gaps WHERE gap > 0;

-- Build final table
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

-- Insert discovered
INSERT INTO all_nyc_units (
    unit_id, bbl, borough, address, zipcode, unit_number,
    is_placeholder, source_systems, confidence_score, ownership_type,
    bldgclass, yearbuilt, numfloors
)
SELECT
    m.unit_id, CAST(m.matched_bbl AS TEXT), p.borough,
    COALESCE(m.full_address, p.address), p.zipcode, m.unit_number,
    0, m.source_systems, m.confidence_score, m.ownership_type,
    p.bldgclass, p.yearbuilt, p.numfloors
FROM cu_mapped m
JOIN pluto p ON p.bbl = m.matched_bbl
WHERE m.matched_bbl IS NOT NULL;

SELECT 'Discovered inserted: ' || COUNT(*) FROM all_nyc_units;

-- Number sequence for placeholders
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
    g.bbl_text, g.borough, g.address, g.zipcode,
    'UNIT_' || SUBSTR('00000' || (g.discovered + s.n), -5, 5),
    1, '["PLUTO_INFERRED"]', 0.3,
    g.bldgclass, g.yearbuilt, g.numfloors
FROM gaps g JOIN num_seq s ON s.n <= g.gap
WHERE g.gap > 0;

SELECT 'Placeholders inserted: ' || COUNT(*) FROM all_nyc_units WHERE is_placeholder = 1;

-- Indexes
CREATE INDEX idx_anu_bbl ON all_nyc_units(bbl);
CREATE INDEX idx_anu_borough ON all_nyc_units(borough);
CREATE INDEX idx_anu_zip ON all_nyc_units(zipcode);
CREATE INDEX idx_anu_ph ON all_nyc_units(is_placeholder);

-- Report
SELECT '';
SELECT '════════════════════════════════════════════════════';
SELECT '  ALL NYC UNITS - COMPLETE COVERAGE';
SELECT '════════════════════════════════════════════════════';
SELECT '';
SELECT 'Total units: ' || COUNT(*) FROM all_nyc_units;
SELECT 'Real (discovered): ' || COUNT(*) FROM all_nyc_units WHERE is_placeholder = 0;
SELECT 'Placeholder: ' || COUNT(*) FROM all_nyc_units WHERE is_placeholder = 1;
SELECT 'Unique buildings: ' || COUNT(DISTINCT bbl) FROM all_nyc_units;
SELECT '';
SELECT '--- BY BOROUGH ---';
SELECT
    borough,
    COUNT(*) as total,
    SUM(CASE WHEN is_placeholder = 0 THEN 1 ELSE 0 END) as real,
    ROUND(100.0 * SUM(CASE WHEN is_placeholder = 0 THEN 1 ELSE 0 END) / COUNT(*), 1) || '%' as real_pct
FROM all_nyc_units
GROUP BY borough ORDER BY total DESC;

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
FROM all_nyc_units GROUP BY 1 ORDER BY units DESC;

SELECT '';
SELECT 'Database size: ' || ROUND(page_count * page_size / 1048576.0, 1) || ' MB'
FROM pragma_page_count(), pragma_page_size();

-- Cleanup temp
DROP TABLE pluto;
DROP TABLE canonical_units;
DROP TABLE cu_mapped;
DROP TABLE bldg_disc;
DROP TABLE gaps;
DROP TABLE num_seq;
VACUUM;

SELECT 'Final size: ' || ROUND(page_count * page_size / 1048576.0, 1) || ' MB'
FROM pragma_page_count(), pragma_page_size();
SQLEOF

echo "=== DONE ==="
ls -lh "$NEW_DB"

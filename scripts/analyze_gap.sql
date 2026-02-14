-- Comprehensive Gap Analysis

SELECT '=== Total NYC Units ===' as report;
SELECT
    'Total NYC residential units (PLUTO):' as metric,
    SUM(unitsres) as value
FROM pluto
WHERE unitsres > 0;

SELECT
    'Discovered units (canonical):' as metric,
    COUNT(*) as value
FROM canonical_units;

SELECT
    'Coverage:' as metric,
    ROUND(100.0 * (SELECT COUNT(*) FROM canonical_units) /
          (SELECT SUM(unitsres) FROM pluto WHERE unitsres > 0), 1) || '%' as value;

SELECT '';
SELECT '=== Missing Units by Building Size ===' as report;
SELECT
    CASE
        WHEN p.unitsres = 1 THEN '1 unit (single family)'
        WHEN p.unitsres BETWEEN 2 AND 5 THEN '2-5 units (small)'
        WHEN p.unitsres BETWEEN 6 AND 20 THEN '6-20 units (medium)'
        WHEN p.unitsres BETWEEN 21 AND 50 THEN '21-50 units (large)'
        WHEN p.unitsres BETWEEN 51 AND 200 THEN '51-200 units (very large)'
        ELSE '200+ units (towers)'
    END as building_size,
    COUNT(*) as num_buildings,
    SUM(p.unitsres) as total_units_missing
FROM pluto p
WHERE p.unitsres > 0
AND p.bbl NOT IN (SELECT DISTINCT bbl FROM canonical_units WHERE bbl IS NOT NULL)
GROUP BY 1
ORDER BY 3 DESC;

SELECT '';
SELECT '=== Building Type Distribution of Gap ===' as report;
SELECT
    p.bldgclass,
    p.landuse,
    COUNT(*) as num_buildings,
    SUM(p.unitsres) as missing_units
FROM pluto p
WHERE p.unitsres > 0
AND p.bbl NOT IN (SELECT DISTINCT bbl FROM canonical_units WHERE bbl IS NOT NULL)
GROUP BY p.bldgclass, p.landuse
ORDER BY SUM(p.unitsres) DESC
LIMIT 15;

SELECT '';
SELECT '=== Borough Totals ===' as report;
SELECT
    borough,
    SUM(unitsres) as total_units
FROM pluto
WHERE unitsres > 0
GROUP BY borough
ORDER BY SUM(unitsres) DESC;

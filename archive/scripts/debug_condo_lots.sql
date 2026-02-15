-- Check if ACRIS BBLs are condo lot numbers (75xx range)
SELECT '--- ACRIS BBL lot ranges ---';
SELECT
    CASE
        WHEN CAST(SUBSTR(bbl, 7, 4) AS INTEGER) >= 7500 THEN 'Condo lot (>=7500)'
        WHEN CAST(SUBSTR(bbl, 7, 4) AS INTEGER) >= 1000 THEN 'Lot 1000-7499'
        ELSE 'Regular lot (<1000)'
    END as lot_type,
    COUNT(*) as units,
    COUNT(DISTINCT bbl) as distinct_bbls
FROM canonical_units
WHERE bbl IS NOT NULL AND LENGTH(bbl) = 10
  AND source_systems LIKE '%ACRIS%'
GROUP BY 1;

-- Check if truncating to block level helps match
SELECT '--- If we match on boro+block only ---';
SELECT COUNT(DISTINCT SUBSTR(c.bbl, 1, 6))
FROM canonical_units c
WHERE c.bbl IS NOT NULL AND LENGTH(c.bbl) = 10;

-- Check PLUTO for condo lots
SELECT '--- PLUTO condo lots ---';
SELECT
    CASE
        WHEN CAST(SUBSTR(CAST(CAST(bbl AS INTEGER) AS TEXT), 7) AS INTEGER) >= 7500 THEN 'Condo lot (>=7500)'
        WHEN CAST(SUBSTR(CAST(CAST(bbl AS INTEGER) AS TEXT), 7) AS INTEGER) >= 1000 THEN 'Lot 1000-7499'
        ELSE 'Regular lot (<1000)'
    END as lot_type,
    COUNT(*) as buildings,
    SUM(unitsres) as units
FROM pluto
WHERE unitsres > 0
GROUP BY 1;

-- Do we have a condo lot -> billing BBL mapping anywhere?
SELECT '--- Check acris_real_property for billing BBL ---';
SELECT * FROM acris_real_property LIMIT 3;

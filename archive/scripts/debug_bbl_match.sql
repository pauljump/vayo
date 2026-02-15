-- Check BBL matching between canonical_units and PLUTO
SELECT '--- canonical_units borough values ---';
SELECT borough, COUNT(*) as cnt FROM canonical_units WHERE bbl IS NOT NULL GROUP BY borough ORDER BY cnt DESC LIMIT 10;

SELECT '--- BBL leading digit ---';
SELECT SUBSTR(bbl, 1, 1) as boro_digit, COUNT(*) as cnt
FROM canonical_units
WHERE bbl IS NOT NULL AND LENGTH(bbl) = 10
GROUP BY 1 ORDER BY 2 DESC;

SELECT '--- sample non-matching BBLs ---';
SELECT c.bbl, c.unit_number, c.source_systems
FROM canonical_units c
WHERE c.bbl IS NOT NULL AND LENGTH(c.bbl) = 10
AND CAST(c.bbl AS INTEGER) NOT IN (SELECT CAST(bbl AS INTEGER) FROM pluto WHERE unitsres > 0)
LIMIT 10;

SELECT '--- how many canonical BBLs match PLUTO at all ---';
SELECT COUNT(DISTINCT CAST(c.bbl AS INTEGER))
FROM canonical_units c
WHERE c.bbl IS NOT NULL AND LENGTH(c.bbl) = 10
AND CAST(c.bbl AS INTEGER) IN (SELECT CAST(bbl AS INTEGER) FROM pluto);

SELECT '--- how many canonical BBLs match PLUTO with units ---';
SELECT COUNT(DISTINCT CAST(c.bbl AS INTEGER))
FROM canonical_units c
WHERE c.bbl IS NOT NULL AND LENGTH(c.bbl) = 10
AND CAST(c.bbl AS INTEGER) IN (SELECT CAST(bbl AS INTEGER) FROM pluto WHERE unitsres > 0);

SELECT '--- total distinct BBLs in canonical_units (10-digit) ---';
SELECT COUNT(DISTINCT CAST(bbl AS INTEGER))
FROM canonical_units
WHERE bbl IS NOT NULL AND LENGTH(bbl) = 10;

-- Where did 2.3M units go?
SELECT '=== canonical_units breakdown ===';
SELECT 'Total: ' || COUNT(*) FROM canonical_units;
SELECT 'Has BBL (10-digit, non-zero): ' || COUNT(*)
FROM canonical_units WHERE bbl IS NOT NULL AND LENGTH(bbl) = 10 AND bbl != '0000000000';
SELECT 'Has BIN only (no usable BBL): ' || COUNT(*)
FROM canonical_units WHERE (bbl IS NULL OR LENGTH(bbl) != 10 OR bbl = '0000000000') AND bin IS NOT NULL;
SELECT 'Neither BBL nor BIN: ' || COUNT(*)
FROM canonical_units WHERE (bbl IS NULL OR LENGTH(bbl) != 10 OR bbl = '0000000000') AND (bin IS NULL OR bin = '');

SELECT '';
SELECT '=== By source ===';
SELECT
    CASE
        WHEN source_systems LIKE '%ACRIS%' THEN 'ACRIS'
        WHEN source_systems LIKE '%HPD%' THEN 'HPD'
        WHEN source_systems LIKE '%TEXT_MINED%' THEN 'Text Mining'
        ELSE 'Other'
    END as source,
    COUNT(*) as total,
    SUM(CASE WHEN bbl IS NOT NULL AND LENGTH(bbl) = 10 AND bbl != '0000000000' THEN 1 ELSE 0 END) as has_bbl,
    SUM(CASE WHEN (bbl IS NULL OR LENGTH(bbl) != 10 OR bbl = '0000000000') AND bin IS NOT NULL THEN 1 ELSE 0 END) as bin_only
FROM canonical_units
GROUP BY 1;

SELECT '';
SELECT '=== HPD units sample ===';
SELECT unit_id, bbl, bin, unit_number FROM canonical_units
WHERE source_systems LIKE '%HPD%' LIMIT 10;

SELECT '';
SELECT '=== Text mined sample (no BBL) ===';
SELECT unit_id, bbl, bin, unit_number FROM canonical_units
WHERE source_systems LIKE '%TEXT_MINED%'
AND (bbl IS NULL OR LENGTH(bbl) != 10 OR bbl = '0000000000')
LIMIT 10;

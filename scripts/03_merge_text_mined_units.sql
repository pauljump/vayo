-- Optimized merge of text-mined units into canonical_units
-- Splits the merge into two efficient queries instead of using OR condition

-- First: Add units matched by BBL
INSERT OR IGNORE INTO canonical_units (
    unit_id, bbl, bin, unit_number, full_address,
    source_systems, confidence_score, verified
)
SELECT
    COALESCE(bbl, bin) || '-' || unit_number as unit_id,
    bbl,
    bin,
    unit_number,
    address,
    '["TEXT_MINED"]' as source_systems,
    0.6 as confidence_score,
    0 as verified
FROM text_mined_units
WHERE bbl IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM canonical_units c
    WHERE c.bbl = text_mined_units.bbl
    AND c.unit_number = text_mined_units.unit_number
);

-- Second: Add units matched by BIN (that weren't already matched by BBL)
INSERT OR IGNORE INTO canonical_units (
    unit_id, bbl, bin, unit_number, full_address,
    source_systems, confidence_score, verified
)
SELECT
    COALESCE(bbl, bin) || '-' || unit_number as unit_id,
    bbl,
    bin,
    unit_number,
    address,
    '["TEXT_MINED"]' as source_systems,
    0.6 as confidence_score,
    0 as verified
FROM text_mined_units
WHERE bin IS NOT NULL
AND bbl IS NULL  -- Only process records without BBL
AND NOT EXISTS (
    SELECT 1 FROM canonical_units c
    WHERE c.bin = text_mined_units.bin
    AND c.unit_number = text_mined_units.unit_number
);

-- Report results
SELECT 'Text-mined units:' as metric, COUNT(*) as count FROM text_mined_units
UNION ALL
SELECT 'Canonical units:', COUNT(*) FROM canonical_units;

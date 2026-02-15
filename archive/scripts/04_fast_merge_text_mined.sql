-- Super-fast merge using temporary table and LEFT JOIN approach
-- This is much faster than NOT EXISTS for large datasets

-- Create index on text_mined_units for faster joins
CREATE INDEX IF NOT EXISTS idx_text_mined_bbl_unit ON text_mined_units(bbl, unit_number);
CREATE INDEX IF NOT EXISTS idx_text_mined_bin_unit ON text_mined_units(bin, unit_number);
CREATE INDEX IF NOT EXISTS idx_canonical_bbl_unit ON canonical_units(bbl, unit_number);
CREATE INDEX IF NOT EXISTS idx_canonical_bin_unit ON canonical_units(bin, unit_number);

-- Count text-mined units
SELECT 'Text-mined units found:' as status, COUNT(*) as count FROM text_mined_units;

-- Insert new units (matched by BBL) - only those that don't exist
INSERT OR IGNORE INTO canonical_units (
    unit_id, bbl, bin, unit_number, full_address,
    source_systems, confidence_score, verified
)
SELECT
    COALESCE(t.bbl, t.bin) || '-' || t.unit_number as unit_id,
    t.bbl,
    t.bin,
    t.unit_number,
    t.address,
    '["TEXT_MINED"]' as source_systems,
    0.6 as confidence_score,
    0 as verified
FROM text_mined_units t
LEFT JOIN canonical_units c ON c.bbl = t.bbl AND c.unit_number = t.unit_number
WHERE t.bbl IS NOT NULL
AND c.unit_id IS NULL  -- Only where no match exists
LIMIT 500000;  -- Process in chunks

SELECT '... added units matched by BBL (batch 1)' as status, changes() as count;

-- Second batch (BIN-based, where BBL is null)
INSERT OR IGNORE INTO canonical_units (
    unit_id, bbl, bin, unit_number, full_address,
    source_systems, confidence_score, verified
)
SELECT
    COALESCE(t.bbl, t.bin) || '-' || t.unit_number as unit_id,
    t.bbl,
    t.bin,
    t.unit_number,
    t.address,
    '["TEXT_MINED"]' as source_systems,
    0.6 as confidence_score,
    0 as verified
FROM text_mined_units t
LEFT JOIN canonical_units c ON c.bin = t.bin AND c.unit_number = t.unit_number
WHERE t.bin IS NOT NULL
AND t.bbl IS NULL  -- Only process records without BBL
AND c.unit_id IS NULL
LIMIT 500000;

SELECT '... added units matched by BIN (batch 1)' as status, changes() as count;

-- Final count
SELECT 'Total canonical units now:' as status, COUNT(*) as count FROM canonical_units;

-- ============================================================================
-- MERGE ALL UNIT SOURCES INTO CANONICAL TABLE
-- ============================================================================
-- Strategy:
-- 1. Insert ACRIS units first (highest quality - has transactions)
-- 2. Add HPD units that don't exist yet
-- 3. Link everything to buildings via BBL/BIN
-- 4. Calculate confidence scores

-- ============================================================================
-- STEP 1: Insert ACRIS units (highest confidence)
-- ============================================================================

INSERT OR IGNORE INTO canonical_units (
    unit_id,
    bbl,
    bin,
    borough,
    block,
    lot,
    unit_number,
    full_address,
    street_number,
    street_name,
    property_type,
    ownership_type,
    source_systems,
    confidence_score,
    verified,
    first_discovered_date
)
SELECT
    bbl || '-' || unit_number as unit_id,
    bbl,
    NULL as bin,  -- Will fill in next step
    borough,
    block,
    lot,
    unit_number,
    full_address,
    street_number,
    street_name,
    property_type,
    CASE
        WHEN property_type LIKE '%CONDO%' THEN 'condo'
        WHEN property_type LIKE '%COOP%' THEN 'coop'
        ELSE 'unknown'
    END as ownership_type,
    '["ACRIS"]' as source_systems,
    0.9 as confidence_score,  -- High confidence - from official transactions
    1 as verified,
    (SELECT MIN(document_date)
     FROM acris_master m
     JOIN acris_real_property rp ON m.document_id = rp.document_id
     WHERE rp.borough = a.borough
       AND rp.block = a.block
       AND rp.lot = a.lot
       AND rp.unit = a.unit_number
     LIMIT 1
    ) as first_discovered_date
FROM units_from_acris a;

SELECT 'Step 1: Inserted ' || changes() || ' units from ACRIS';

-- ============================================================================
-- STEP 2: Insert HPD units that don't already exist
-- ============================================================================

INSERT OR IGNORE INTO canonical_units (
    unit_id,
    bbl,
    bin,
    borough,
    block,
    lot,
    unit_number,
    source_systems,
    confidence_score,
    verified
)
SELECT
    COALESCE(bbl, bin) || '-' || unit_number as unit_id,
    bbl,
    bin,
    borough,
    block,
    lot,
    unit_number,
    '["HPD_COMPLAINTS"]' as source_systems,
    0.7 as confidence_score,  -- Medium confidence
    0 as verified
FROM units_from_hpd
WHERE NOT EXISTS (
    SELECT 1 FROM canonical_units c
    WHERE c.bbl = units_from_hpd.bbl
      AND c.unit_number = units_from_hpd.unit_number
);

SELECT 'Step 2: Inserted ' || changes() || ' new units from HPD';

-- ============================================================================
-- STEP 3: Update units with multiple sources (merge source_systems)
-- ============================================================================

-- If a unit appears in both ACRIS and HPD, merge the sources
UPDATE canonical_units
SET
    source_systems = (
        SELECT '["ACRIS","HPD_COMPLAINTS"]'
        FROM units_from_hpd h
        WHERE h.bbl = canonical_units.bbl
          AND h.unit_number = canonical_units.unit_number
        LIMIT 1
    ),
    confidence_score = 0.95,  -- Higher confidence when multiple sources agree
    bin = COALESCE(
        canonical_units.bin,
        (SELECT h.bin FROM units_from_hpd h
         WHERE h.bbl = canonical_units.bbl
           AND h.unit_number = canonical_units.unit_number
         LIMIT 1)
    )
WHERE EXISTS (
    SELECT 1 FROM units_from_hpd h
    WHERE h.bbl = canonical_units.bbl
      AND h.unit_number = canonical_units.unit_number
);

SELECT 'Step 3: Updated ' || changes() || ' units with multiple sources';

-- ============================================================================
-- STEP 4: Link units to buildings (fill in missing BINs and addresses)
-- ============================================================================

UPDATE canonical_units
SET
    bin = COALESCE(
        canonical_units.bin,
        (SELECT b.bin FROM buildings b
         WHERE b.bbl = canonical_units.bbl
         LIMIT 1)
    ),
    full_address = COALESCE(
        canonical_units.full_address,
        (SELECT b.address FROM buildings b
         WHERE b.bbl = canonical_units.bbl
         LIMIT 1)
    )
WHERE canonical_units.bbl IS NOT NULL;

SELECT 'Step 4: Linked ' || changes() || ' units to buildings';

-- ============================================================================
-- STEP 5: Calculate transaction counts
-- ============================================================================

UPDATE canonical_units
SET transaction_count = (
    SELECT COUNT(DISTINCT m.document_id)
    FROM acris_master m
    JOIN acris_real_property rp ON m.document_id = rp.document_id
    WHERE rp.borough = canonical_units.borough
      AND rp.block = canonical_units.block
      AND rp.lot = canonical_units.lot
      AND rp.unit = canonical_units.unit_number
)
WHERE source_systems LIKE '%ACRIS%';

SELECT 'Step 5: Calculated transaction counts';

-- ============================================================================
-- STEP 6: Calculate complaint counts
-- ============================================================================

UPDATE canonical_units
SET complaint_count = (
    SELECT COUNT(*)
    FROM complaints c
    WHERE c.bin = canonical_units.bin
      AND c.apartment = canonical_units.unit_number
)
WHERE source_systems LIKE '%HPD%' AND canonical_units.bin IS NOT NULL;

SELECT 'Step 6: Calculated complaint counts';

-- ============================================================================
-- STEP 7: Set last_seen_date to today for all units
-- ============================================================================

UPDATE canonical_units
SET last_seen_date = DATE('now')
WHERE last_seen_date IS NULL;

-- ============================================================================
-- FINAL STATS
-- ============================================================================

SELECT 'MERGE COMPLETE - FINAL STATS:';
SELECT '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━';
SELECT 'Total units in canonical table: ' || COUNT(*) FROM canonical_units;
SELECT 'Units with BIN: ' || COUNT(*) FROM canonical_units WHERE bin IS NOT NULL;
SELECT 'Units with full address: ' || COUNT(*) FROM canonical_units WHERE full_address IS NOT NULL;
SELECT 'Units with transactions: ' || COUNT(*) FROM canonical_units WHERE transaction_count > 0;
SELECT 'Units with complaints: ' || COUNT(*) FROM canonical_units WHERE complaint_count > 0;
SELECT '';
SELECT 'By source:';
SELECT 'ACRIS only: ' || COUNT(*) FROM canonical_units WHERE source_systems = '["ACRIS"]';
SELECT 'HPD only: ' || COUNT(*) FROM canonical_units WHERE source_systems = '["HPD_COMPLAINTS"]';
SELECT 'Both ACRIS+HPD: ' || COUNT(*) FROM canonical_units WHERE source_systems LIKE '%ACRIS%' AND source_systems LIKE '%HPD%';
SELECT '';
SELECT 'By ownership type:';
SELECT 'Condos: ' || COUNT(*) FROM canonical_units WHERE ownership_type = 'condo';
SELECT 'Co-ops: ' || COUNT(*) FROM canonical_units WHERE ownership_type = 'coop';
SELECT 'Unknown: ' || COUNT(*) FROM canonical_units WHERE ownership_type = 'unknown';

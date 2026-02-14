-- ============================================================================
-- CANONICAL UNITS - COMPREHENSIVE STATS
-- ============================================================================

.mode column
.headers on
.width 40 15

SELECT '════════════════════════════════════════════════════════════════';
SELECT 'CANONICAL UNITS - STATISTICS REPORT';
SELECT '════════════════════════════════════════════════════════════════';
SELECT '';

-- ============================================================================
-- OVERALL COUNTS
-- ============================================================================

SELECT '━━━ OVERALL COUNTS ━━━';
SELECT '';

SELECT 'Metric' as metric, 'Count' as count
UNION ALL
SELECT '─────────────────────────────', '─────────────'
UNION ALL
SELECT 'Total canonical units', COUNT(*) FROM canonical_units
UNION ALL
SELECT 'Units with BIN', COUNT(*) FROM canonical_units WHERE bin IS NOT NULL
UNION ALL
SELECT 'Units with BBL', COUNT(*) FROM canonical_units WHERE bbl IS NOT NULL
UNION ALL
SELECT 'Units with full address', COUNT(*) FROM canonical_units WHERE full_address IS NOT NULL
UNION ALL
SELECT 'Verified units', COUNT(*) FROM canonical_units WHERE verified = 1
UNION ALL
SELECT 'High confidence (>0.8)', COUNT(*) FROM canonical_units WHERE confidence_score > 0.8;

SELECT '';

-- ============================================================================
-- BY SOURCE
-- ============================================================================

SELECT '━━━ BY SOURCE ━━━';
SELECT '';

SELECT 'Source' as source, 'Count' as count
UNION ALL
SELECT '─────────────────────────────', '─────────────'
UNION ALL
SELECT 'ACRIS only', COUNT(*) FROM canonical_units WHERE source_systems = '["ACRIS"]'
UNION ALL
SELECT 'HPD only', COUNT(*) FROM canonical_units WHERE source_systems = '["HPD_COMPLAINTS"]'
UNION ALL
SELECT 'Both ACRIS + HPD', COUNT(*) FROM canonical_units
    WHERE source_systems LIKE '%ACRIS%' AND source_systems LIKE '%HPD%'
UNION ALL
SELECT 'Other sources', COUNT(*) FROM canonical_units
    WHERE source_systems NOT LIKE '%ACRIS%' AND source_systems NOT LIKE '%HPD%';

SELECT '';

-- ============================================================================
-- BY BOROUGH
-- ============================================================================

SELECT '━━━ BY BOROUGH ━━━';
SELECT '';

SELECT 'Borough' as borough, 'Count' as count
UNION ALL
SELECT '─────────────────────────────', '─────────────'
UNION ALL
SELECT CASE borough
    WHEN '1' THEN 'Manhattan'
    WHEN '2' THEN 'Bronx'
    WHEN '3' THEN 'Brooklyn'
    WHEN '4' THEN 'Queens'
    WHEN '5' THEN 'Staten Island'
    ELSE 'Unknown'
END, COUNT(*)
FROM canonical_units
GROUP BY borough
ORDER BY borough;

SELECT '';

-- ============================================================================
-- BY OWNERSHIP TYPE
-- ============================================================================

SELECT '━━━ BY OWNERSHIP TYPE ━━━';
SELECT '';

SELECT 'Type' as type, 'Count' as count
UNION ALL
SELECT '─────────────────────────────', '─────────────'
UNION ALL
SELECT 'Condo', COUNT(*) FROM canonical_units WHERE ownership_type = 'condo'
UNION ALL
SELECT 'Co-op', COUNT(*) FROM canonical_units WHERE ownership_type = 'coop'
UNION ALL
SELECT 'Rental', COUNT(*) FROM canonical_units WHERE ownership_type = 'rental'
UNION ALL
SELECT 'Unknown', COUNT(*) FROM canonical_units WHERE ownership_type = 'unknown' OR ownership_type IS NULL;

SELECT '';

-- ============================================================================
-- ACTIVITY METRICS
-- ============================================================================

SELECT '━━━ ACTIVITY METRICS ━━━';
SELECT '';

SELECT 'Metric' as metric, 'Count' as count
UNION ALL
SELECT '─────────────────────────────', '─────────────'
UNION ALL
SELECT 'Units with transactions (>0)', COUNT(*) FROM canonical_units WHERE transaction_count > 0
UNION ALL
SELECT 'Units with complaints (>0)', COUNT(*) FROM canonical_units WHERE complaint_count > 0
UNION ALL
SELECT 'Units with violations (>0)', COUNT(*) FROM canonical_units WHERE violation_count > 0
UNION ALL
SELECT 'Total transactions tracked', SUM(transaction_count) FROM canonical_units
UNION ALL
SELECT 'Total complaints tracked', SUM(complaint_count) FROM canonical_units
UNION ALL
SELECT 'Total violations tracked', SUM(violation_count) FROM canonical_units;

SELECT '';

-- ============================================================================
-- TOP BUILDINGS BY UNIT COUNT
-- ============================================================================

SELECT '━━━ TOP BUILDINGS BY DISCOVERED UNITS ━━━';
SELECT '';

.mode column
.width 15 50 10

SELECT
    bin,
    full_address,
    COUNT(*) as units
FROM canonical_units
WHERE bin IS NOT NULL
GROUP BY bin, full_address
ORDER BY units DESC
LIMIT 20;

SELECT '';

-- ============================================================================
-- PLACEHOLDER UNITS
-- ============================================================================

SELECT '━━━ PLACEHOLDER UNITS ━━━';
SELECT '';

.mode column
.width 40 15

SELECT 'Metric' as metric, 'Count' as count
UNION ALL
SELECT '─────────────────────────────', '─────────────'
UNION ALL
SELECT 'Total placeholder units', COUNT(*) FROM placeholder_units
UNION ALL
SELECT 'Buildings with placeholders', COUNT(DISTINCT bin) FROM placeholder_units;

SELECT '';
SELECT '════════════════════════════════════════════════════════════════';
SELECT 'END OF REPORT';
SELECT '════════════════════════════════════════════════════════════════';

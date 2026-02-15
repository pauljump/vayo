-- ============================================================================
-- TURNOVER SIGNALS FOR GRAMERCY GEMS
-- ============================================================================

-- 1. ACRIS: recent deed transfers, estates, lis pendens on our target buildings
SELECT '=== ACRIS FILINGS ON TARGET BUILDINGS ===';
SELECT
    r.street_number || ' ' || r.street_name as address,
    r.unit,
    m.doc_type,
    m.recorded_datetime,
    m.document_amt,
    p.name as party_name,
    p.party_type
FROM acris_real_property r
JOIN acris_master m ON m.document_id = r.document_id
LEFT JOIN acris_parties p ON p.document_id = r.document_id AND p.party_type IN ('1','2')
WHERE r.borough = 1
AND (
    (r.street_number = '18' AND r.street_name LIKE '%GRAMERCY%')
    OR (r.street_number = '5' AND r.street_name LIKE '%17%ST%')
    OR (r.street_number = '24' AND r.street_name LIKE '%21%ST%')
    OR (r.street_number = '43' AND r.street_name LIKE '%19%ST%')
    OR (r.street_number = '31' AND r.street_name LIKE '%21%ST%' AND r.street_name LIKE '%W%')
    OR (r.street_number = '12' AND r.street_name LIKE '%13%ST%')
    OR (r.street_number = '20' AND r.street_name LIKE '%20%ST%')
    OR (r.street_number = '26' AND r.street_name LIKE '%22%ST%')
    OR (r.street_number = '64' AND r.street_name LIKE '%11%ST%')
    OR (r.street_number = '35' AND r.street_name LIKE '%20%ST%')
    OR (r.street_number = '57' AND r.street_name LIKE '%IRVING%')
    OR (r.street_number = '8' AND r.street_name LIKE '%19%ST%' AND r.street_name LIKE '%W%')
    OR (r.street_number = '40' AND r.street_name LIKE '%19%ST%')
)
AND m.doc_type IN ('DEED','MTGE','AALR','AL&R','LPEN','RPTT&RET')
AND m.recorded_datetime >= '2023-01-01'
ORDER BY m.recorded_datetime DESC;

-- 2. DOB permits on target buildings (renovation = turnover)
SELECT '';
SELECT '=== DOB PERMITS ON TARGET BUILDINGS ===';
SELECT
    dp.house_number || ' ' || dp.street_name as address,
    dp.job_type,
    dp.job_description,
    dp.latest_action_date,
    dp.job_status_description,
    dp.initial_cost,
    dp.existing_dwelling_units,
    dp.proposed_dwelling_units
FROM dob_permits dp
WHERE dp.borough = 'MANHATTAN'
AND (
    (dp.house_number = '18' AND dp.street_name LIKE '%GRAMERCY%')
    OR (dp.house_number = '5' AND dp.street_name LIKE '%17%ST%')
    OR (dp.house_number = '24' AND dp.street_name LIKE '%21%ST%' AND dp.street_name LIKE '%E%')
    OR (dp.house_number = '43' AND dp.street_name LIKE '%19%ST%' AND dp.street_name LIKE '%E%')
    OR (dp.house_number = '31' AND dp.street_name LIKE '%21%ST%' AND dp.street_name LIKE '%W%')
    OR (dp.house_number = '12' AND dp.street_name LIKE '%13%ST%')
    OR (dp.house_number = '20' AND dp.street_name LIKE '%20%ST%' AND dp.street_name LIKE '%E%')
    OR (dp.house_number = '26' AND dp.street_name LIKE '%22%ST%')
    OR (dp.house_number = '64' AND dp.street_name LIKE '%11%ST%')
    OR (dp.house_number = '35' AND dp.street_name LIKE '%20%ST%')
    OR (dp.house_number = '57' AND dp.street_name LIKE '%IRVING%')
    OR (dp.house_number = '8' AND dp.street_name LIKE '%19%ST%' AND dp.street_name LIKE '%W%')
    OR (dp.house_number = '40' AND dp.street_name LIKE '%19%ST%')
)
AND dp.latest_action_date >= '2023-01-01'
ORDER BY dp.latest_action_date DESC;

-- 3. Marshal evictions (recent)
SELECT '';
SELECT '=== RECENT EVICTIONS NEAR TARGET AREA ===';
SELECT eviction_address, executed_date, marshal_first_name, marshal_last_name
FROM marshal_evictions
WHERE residential_commercial = 'Residential'
AND borough = 'MANHATTAN'
AND executed_date >= '2024-01-01'
AND (eviction_address LIKE '%GRAMERCY%'
     OR eviction_address LIKE '%E 17%' OR eviction_address LIKE '%E 19%'
     OR eviction_address LIKE '%E 20%' OR eviction_address LIKE '%E 21%'
     OR eviction_address LIKE '%E 22%' OR eviction_address LIKE '%IRVING%')
ORDER BY executed_date DESC
LIMIT 20;

-- 4. Who are the parties (buyers/sellers) on recent deeds?
SELECT '';
SELECT '=== RECENT DEED PARTIES (WHO IS BUYING/SELLING) ===';
SELECT
    r.street_number || ' ' || r.street_name as address,
    r.unit,
    m.recorded_datetime,
    m.document_amt,
    GROUP_CONCAT(
        CASE p.party_type WHEN '1' THEN 'SELLER: ' WHEN '2' THEN 'BUYER: ' END
        || p.name, ' | '
    ) as parties
FROM acris_real_property r
JOIN acris_master m ON m.document_id = r.document_id
JOIN acris_parties p ON p.document_id = r.document_id
WHERE r.borough = 1
AND m.doc_type = 'DEED'
AND m.recorded_datetime >= '2022-01-01'
AND (
    (r.street_number = '18' AND r.street_name LIKE '%GRAMERCY%')
    OR (r.street_number = '5' AND r.street_name LIKE '%17%ST%')
    OR (r.street_number = '24' AND r.street_name LIKE '%21%ST%')
    OR (r.street_number = '43' AND r.street_name LIKE '%19%ST%')
    OR (r.street_number = '31' AND r.street_name LIKE '%21%ST%' AND r.street_name LIKE '%W%')
    OR (r.street_number = '12' AND r.street_name LIKE '%13%ST%')
    OR (r.street_number = '20' AND r.street_name LIKE '%20%ST%')
    OR (r.street_number = '26' AND r.street_name LIKE '%22%ST%')
    OR (r.street_number = '64' AND r.street_name LIKE '%11%ST%')
    OR (r.street_number = '35' AND r.street_name LIKE '%20%ST%')
    OR (r.street_number = '57' AND r.street_name LIKE '%IRVING%')
    OR (r.street_number = '8' AND r.street_name LIKE '%19%ST%' AND r.street_name LIKE '%W%')
    OR (r.street_number = '40' AND r.street_name LIKE '%19%ST%')
)
GROUP BY m.document_id
ORDER BY m.recorded_datetime DESC;

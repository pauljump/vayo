-- ============================================================================
-- DIAGNOSE ALL BBL FORMAT ISSUES
-- ============================================================================

-- 1. 311 BBL format
SELECT '=== 311 BBL samples ===';
SELECT bbl, COUNT(*) as cnt FROM nyc_311_complete WHERE bbl IS NOT NULL GROUP BY bbl ORDER BY cnt DESC LIMIT 10;
SELECT 'Total 311 rows: ' || COUNT(*) FROM nyc_311_complete;
SELECT '311 with BBL: ' || COUNT(*) FROM nyc_311_complete WHERE bbl IS NOT NULL AND bbl <> '';

-- 2. Rent stabilization BBL format
SELECT '';
SELECT '=== Rent stab BBL samples ===';
SELECT bbl, uc_2007, uc_2017 FROM rent_stabilization WHERE bbl IS NOT NULL LIMIT 10;
SELECT 'Total rent stab rows: ' || COUNT(*) FROM rent_stabilization;

-- 3. ECB violations
SELECT '';
SELECT '=== ECB violations structure ===';
SELECT sql FROM sqlite_master WHERE name = 'ecb_violations';

-- 4. HPD registrations - how does buildingid map?
SELECT '';
SELECT '=== HPD registration -> contact join ===';
SELECT r.registrationid, r.bin, r.housenumber, r.streetname, r.zip,
       c.contacttype, c.corporationname, c.firstname, c.lastname
FROM hpd_registrations r
JOIN hpd_contacts c ON c.registrationid = r.registrationid
WHERE r.zip = '10003'
LIMIT 15;

-- 5. PLUTO BBL format reminder
SELECT '';
SELECT '=== PLUTO BBL format ===';
SELECT bbl, CAST(bbl AS INTEGER), address FROM pluto WHERE zipcode = '10003' LIMIT 5;

-- 6. ACRIS real_property - how BBLs work
SELECT '';
SELECT '=== ACRIS BBL construction ===';
SELECT borough, block, lot, street_number, street_name, unit
FROM acris_real_property
WHERE borough = 1 AND street_name LIKE '%13%ST%' AND street_number = '12'
LIMIT 10;

-- 7. Eviction filings
SELECT '';
SELECT '=== eviction_filings structure ===';
SELECT sql FROM sqlite_master WHERE name = 'eviction_filings';
SELECT COUNT(*) FROM eviction_filings;

-- 8. HPD litigation
SELECT '';
SELECT '=== HPD litigation structure ===';
SELECT sql FROM sqlite_master WHERE name = 'hpd_litigation';
SELECT * FROM hpd_litigation LIMIT 3;

-- 9. DOB complaints
SELECT '';
SELECT '=== DOB complaints structure ===';
SELECT sql FROM sqlite_master WHERE name = 'dob_complaints';
SELECT COUNT(*) FROM dob_complaints;

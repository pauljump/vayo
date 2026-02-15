-- ============================================================================
-- FIND LARGE HIDDEN GEMS IN GRAMERCY AREA
-- ============================================================================
-- Criteria: big units, well-maintained, quiet, possibly rent-stabilized

-- Gramercy core + nearby: 10010, 10003, 10009, 10011, 10016, 10001
-- Complaints use BIN, so we need buildings table to bridge

WITH gramercy AS (
    SELECT
        CAST(p.bbl AS INTEGER) as bbl,
        p.address,
        p.zipcode,
        p.yearbuilt,
        CAST(p.numfloors AS INTEGER) as floors,
        p.unitsres as units,
        p.bldgclass,
        p.ownername,
        p.resarea,
        CASE WHEN p.resarea > 0 AND p.unitsres > 0
             THEN CAST(ROUND(p.resarea * 1.0 / p.unitsres) AS INTEGER)
             ELSE NULL END as avg_sqft
    FROM pluto p
    WHERE p.zipcode IN ('10010','10003','10009','10011','10016','10001','10002')
      AND p.unitsres >= 6
      AND p.numfloors >= 4
      AND p.resarea > 0
),
-- HPD complaints by BIN -> join via buildings
complaint_counts AS (
    SELECT b.bbl, COUNT(*) as cnt
    FROM complaints c
    JOIN buildings b ON b.bin = c.bin
    WHERE b.bbl IS NOT NULL
    GROUP BY b.bbl
),
-- 311 by BBL
noise_311 AS (
    SELECT CAST(bbl AS INTEGER) as bbl, COUNT(*) as cnt
    FROM nyc_311_complete
    WHERE complaint_type LIKE '%Noise%'
    AND bbl IS NOT NULL
    GROUP BY CAST(bbl AS INTEGER)
),
-- Rent stabilized
rent_stab AS (
    SELECT CAST(bbl AS INTEGER) as bbl,
           MAX(uc_2017) as stab_units_2017
    FROM rent_stabilization
    WHERE bbl IS NOT NULL
    GROUP BY CAST(bbl AS INTEGER)
)
SELECT
    g.address,
    g.zipcode as zip,
    g.yearbuilt as built,
    g.floors,
    g.units,
    g.avg_sqft,
    COALESCE(cc.cnt, 0) as hpd_complaints,
    ROUND(COALESCE(cc.cnt, 0) * 1.0 / g.units, 1) as compl_per_unit,
    COALESCE(n.cnt, 0) as noise_311,
    COALESCE(rs.stab_units_2017, 0) as stab_units,
    g.bldgclass as class,
    SUBSTR(g.ownername, 1, 30) as owner
FROM gramercy g
LEFT JOIN complaint_counts cc ON cc.bbl = CAST(g.bbl AS TEXT)
LEFT JOIN noise_311 n ON n.bbl = g.bbl
LEFT JOIN rent_stab rs ON rs.bbl = g.bbl
WHERE g.avg_sqft >= 900
  AND COALESCE(cc.cnt, 0) * 1.0 / g.units < 5
ORDER BY g.avg_sqft DESC, compl_per_unit ASC
LIMIT 40;

.mode csv
.headers on
.output /Users/pjump/Desktop/projects/vayo/nycdb_data/hpd_units.csv
SELECT unit_id, bbl, bin, borough, unit_number, full_address, ownership_type, source_systems, confidence_score FROM canonical_units WHERE source_systems LIKE '%HPD%' AND (bbl IS NULL OR LENGTH(bbl) <> 10 OR bbl = '0000000000') AND bin IS NOT NULL;
.output /Users/pjump/Desktop/projects/vayo/nycdb_data/text_units_nomatch.csv
SELECT unit_id, bbl, bin, borough, unit_number, full_address, ownership_type, source_systems, confidence_score FROM canonical_units WHERE source_systems LIKE '%TEXT_MINED%' AND (bbl IS NULL OR LENGTH(bbl) <> 10 OR bbl = '0000000000');
.output /Users/pjump/Desktop/projects/vayo/nycdb_data/bin_to_bbl.csv
SELECT bin, bbl FROM buildings WHERE bin IS NOT NULL AND bbl IS NOT NULL;
.output stdout
SELECT 'done';

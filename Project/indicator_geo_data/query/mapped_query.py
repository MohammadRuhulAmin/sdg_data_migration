mapped_query= """
SELECT tempx.serial_no,tempx.indicator_id,
tempx.geo_division_id,gdiv.bbs_code,tempx.geo_district_id,gdis.bbs_code,tempx.geo_upazila_id,gupa.bbs_code
FROM(SELECT temp1.serial_no,temp1.indicator_id,sidc.geo_division_id,
sidc.geo_district_id,sidc.geo_upazila_id ,sidc.value 
FROM (SELECT sil.serial_no,sid.indicator_id,sid.id indicator_data_id FROM sdg_indicator_langs sil
LEFT JOIN sdg_indicator_data sid ON sid.indicator_id = sil.indicator_id
WHERE sil.language_id = 1 AND sil.serial_no = "1.3.1")temp1
LEFT JOIN sdg_indicator_data_children sidc ON sidc.indicator_data_id = temp1.indicator_data_id
WHERE sidc.is_location = 1)tempx
LEFT JOIN geo_divisions gdiv ON tempx.geo_division_id = gdiv.id
LEFT JOIN geo_districts gdis ON tempx.geo_district_id = gdis.id
LEFT JOIN geo_upazilas gupa  ON tempx.geo_upazila_id  = gupa.id
ORDER BY tempx.serial_no;
"""


qdb = """
SELECT tempx.serial_no,tempx.indicator_id,
tempx.geo_division_id,gdiv.bbs_code,
tempx.geo_district_id,gdis.bbs_code,
tempx.geo_upazila_id,gupa.bbs_code
FROM(SELECT temp1.serial_no,temp1.indicator_id,
sidc.geo_division_id,
sidc.geo_district_id,sidc.geo_upazila_id ,sidc.value 
FROM (SELECT sil.serial_no,sid.indicator_id,sid.id indicator_data_id FROM 
sdg_v1_v2_live.sdg_indicator_langs sil
LEFT JOIN sdg_v1_v2_live.sdg_indicator_data sid ON sid.indicator_id = sil.indicator_id
WHERE sil.language_id = 1 AND sil.serial_no = "1.3.1")temp1
LEFT JOIN sdg_v1_v2_live.sdg_indicator_data_children sidc ON sidc.indicator_data_id = temp1.indicator_data_id
WHERE sidc.is_location = 1)tempx
LEFT JOIN sdg_v1_v2_live.geo_divisions gdiv ON tempx.geo_division_id = gdiv.id
LEFT JOIN sdg_v1_v2_live.geo_districts gdis ON tempx.geo_district_id = gdis.id
LEFT JOIN sdg_v1_v2_live.geo_upazilas gupa  ON tempx.geo_upazila_id  = gupa.id
ORDER BY tempx.serial_no;
"""
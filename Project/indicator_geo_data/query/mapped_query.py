mapped_query = """
SELECT temp1.*,sidc.is_location,sidc.geo_division_id,sidc.geo_district_id,sidc.geo_upazila_id ,sidc.value FROM (SELECT sil.serial_no,sid.indicator_id,sid.id indicator_data_id FROM sdg_indicator_langs sil
LEFT JOIN sdg_indicator_data sid ON sid.indicator_id = sil.indicator_id
WHERE sil.language_id = 1 AND sil.serial_no = "1.3.1")temp1
LEFT JOIN sdg_indicator_data_children sidc ON sidc.indicator_data_id = temp1.indicator_data_id
WHERE sidc.is_location = 1
"""
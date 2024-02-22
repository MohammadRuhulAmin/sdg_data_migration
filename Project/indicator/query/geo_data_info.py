get_division_bbs_code = """
select bbs_code from geo_divisions 
where division_name_eng like %s
"""

get_upazila_bbs_code = """
select gdiv.division_name_eng,gdiv.id,gdiv.bbs_code,
gdist.district_name_eng,gdist.id,gdist.bbs_code,
gupazila.upazila_name_eng,gupazila.id,gupazila.bbs_code
from geo_upazilas gupazila
left join geo_districts gdist on gdist.id = gupazila.geo_district_id 
left join geo_divisions gdiv on gdiv.id = gupazila.geo_division_id 
WHERE gdiv.division_name_eng LIKE %s
AND gdist.district_name_eng LIKE %s
AND gupazila.upazila_name_eng LIKE %s;
"""

old_get_upazila_bbs_code = """
SELECT gdiv.id,gdiv.bbs_code,gdist.id,gdist.bbs_code,gupazila.id,gupazila.bbs_code FROM geo_divisions gdiv 
LEFT JOIN geo_districts gdist ON gdist.geo_division_id = gdiv.id
LEFT JOIN geo_upazilas gupazila ON gupazila.geo_division_id = gdiv.id AND gupazila.geo_district_id = gdist.id
WHERE gdiv.division_name_eng LIKE %s
AND gdist.district_name_eng LIKE %s
AND gupazila.upazila_name_eng LIKE %s
"""

get_district_bbs_code = """
SELECT gdiv.id,gdiv.bbs_code,gdist.id,gdist.bbs_code FROM geo_divisions gdiv 
LEFT JOIN geo_districts gdist ON gdist.geo_division_id = gdiv.id
WHERE gdiv.division_name_eng LIKE %s
AND gdist.district_name_eng LIKE %s
"""


insert_in_indicator_geo_data = """
insert into indicator_geo_data(ind_data_id,type,bbscode,data_value)values(%s,%s,%s,%s)
"""


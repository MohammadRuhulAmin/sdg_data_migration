division_bbs_code = """
select bbs_code from geo_divisions 
where division_name_eng like %s
"""

district_bbs_code = """
SELECT bbs_code FROM geo_districts
WHERE district_name_eng LIKE %s
"""

upazila_bbs_code = """
SELECT bbs_code FROM geo_upazilas
WHERE upazila_name_eng LIKE %s
"""

insert_in_indicator_geo_data = """"""


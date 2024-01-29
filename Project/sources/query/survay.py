query_get_survy_info = """SELECT id,name FROM ind_data_source_survey;"""

query_getting_ids = """SELECT GROUP_CONCAT(id) AS id_list FROM ind_sources 
WHERE `name` LIKE  %s;"""

query_update_survay_id = """UPDATE ind_sources SET survey_id = %s
WHERE FIND_IN_SET(id,%s)"""
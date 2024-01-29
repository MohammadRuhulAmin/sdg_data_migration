query_getting_ids = """SELECT GROUP_CONCAT(id) AS id_list FROM ind_sources 
WHERE `name` LIKE  %s;"""


query_update_ministry_id = """UPDATE ind_sources SET ministry_id = %s
WHERE FIND_IN_SET(id,%s)"""


query_get_ministry_info = """SELECT id,name FROM ministries"""
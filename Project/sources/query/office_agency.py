query_get_office_agencies_info = """SELECT id,ministry_id,division_id,name FROM office_agencies WHERE 
division_id != 0 """

query_getting_ids = """
SELECT GROUP_CONCAT(id) as id_list FROM ind_sources
WHERE ministry_id = %s and ministry_division_id = %s
AND name LIKE %s
"""


query_update_office_agency_id = """
UPDATE ind_sources SET office_agency_id = %s
WHERE FIND_IN_SET(id,%s)
"""
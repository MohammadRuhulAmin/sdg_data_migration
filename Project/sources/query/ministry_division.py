query_get_ministry_division_info = """
SELECT id,ministry_id,name FROM ministry_divisions;
"""

query_getting_ids = """
SELECT GROUP_CONCAT(id) AS id_list FROM ind_sources
WHERE ministry_id = %s AND name LIKE %s;
"""

query_update_ministry_id = """UPDATE ind_sources SET ministry_division_id = %s
WHERE FIND_IN_SET(id,%s)"""
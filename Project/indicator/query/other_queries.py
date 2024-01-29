query_source = """SELECT * FROM mapped_sources WHERE old_source_id =%s """

query_ind_def = """SELECT id FROM ind_definitions WHERE ind_id = %s LIMIT 1"""

query_get_disagg_id = """
SELECT id,name FROM disaggregation_name
WHERE `name` like %s;
"""
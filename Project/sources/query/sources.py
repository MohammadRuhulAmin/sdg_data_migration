query_src = """SELECT * FROM sdg_v1_v2_live.mapped_sources;"""

query_dest = """SELECT * FROM uat_sdg_tracker_clone.ind_sources WHERE name = %s"""

insert_query_name = """INSERT INTO uat_sdg_tracker_clone.ind_sources 
(ministry_id,ministry_division_id,office_agency_id,survey_id,name)
VALUES(0,0,0,0,%s)"""
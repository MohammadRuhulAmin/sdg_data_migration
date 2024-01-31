unique_user_type = """
SELECT temp1.user_category,ut.type_name 
FROM(SELECT DISTINCT spl.user_category FROM sdg_v1_v2_live.sdg_provider_logs spl)temp1
LEFT JOIN uat_sdg_tracker_clone.user_types ut ON temp1.user_category = ut.type_name
WHERE ut.type_name IS NULL;
"""

insert_new_user_type = """
INSERT INTO user_types(type_name)VALUES(%s);
"""
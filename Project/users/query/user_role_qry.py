query = """
        SELECT sur.name
        FROM sdg_v1_v2_live.sdg_user_roles sur
        LEFT JOIN uat_sdg_tracker_clone.user_role ur ON 
        ur.role_name COLLATE utf8_unicode_ci = sur.name COLLATE utf8_unicode_ci
        WHERE ur.role_name IS NULL;
"""

is_role_exist = """SELECT * FROM user_role WHERE role_name = %s;"""

query_insert = """INSERT INTO user_role(role_name) VALUES(%s)"""
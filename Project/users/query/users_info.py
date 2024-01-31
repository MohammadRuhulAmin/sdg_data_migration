
get_user_type_id = """
SELECT id FROM user_types WHERE type_name LIKE %s;
"""

get_user_role_id = """
SELECT id FROM user_role where role_name like %s;
"""

is_user_exist = """
SELECT * FROM users WHERE username = %s;
"""

insert_users = """
INSERT INTO users(username,user_type,user_role_id,additional_role_ids,name_eng,user_email,user_mobile)
VALUES(%s,%s,%s,%s,%s,%s,%s);
"""
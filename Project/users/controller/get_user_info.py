import Project.connection as mysql_connection
import Project.users.query.users_info as uiq


def get_necessary_information_for_users_table(username, user_category, user_type, name_en, email, contact_no):
    cursor_dest = mysql_connection.mydb_connection_destinationdb.cursor()
    try:
        user_type_f = user_category
        user_role = user_type
        #getting user_type_id
        cursor_dest.execute(uiq.get_user_type_id,(f"%{user_type_f}%",))
        user_type_f = cursor_dest.fetchall()
        user_type_id = user_type_f[0][0] if user_type_f and user_type_f[0] and user_type_f[0][0] else None
        #getting user_role_id
        cursor_dest.execute(uiq.get_user_role_id,(f"%{user_role}%",))
        user_role = cursor_dest.fetchall()
        user_role_id = user_role[0][0] if user_role and user_role[0] and user_role[0][0] else None
        temp_json = {
            "username":username,
            "user_type":user_type_id,
            "user_role_id":user_role_id,
            "additional_role_ids":user_role_id,
            "name_eng":name_en,
            "user_email":email,
            "user_mobile":contact_no
        }
        print(temp_json)
        #check username already exist:

    except Exception as E:
        print(str(E))
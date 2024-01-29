import mysql.connector
mydb_connection_sourcedb = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="root",
    password="ruhulamin",
    database="sdg_v1_v2_live"
)

mydb_connection_destinationdb = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="root",
    password="ruhulamin",
    database="uat_sdg_tracker_clone"
)

def get_user_id():
    try:
        cursor_exist = mydb_connection_sourcedb.cursor()
        query = """SELECT DISTINCT u.id FROM sdg_users u ORDER BY u.id ;"""
        cursor_exist.execute(query)
        id_list = cursor_exist.fetchall()
        return id_list

    except Exception as E:
        print(str(E))

def get_users_from_existing_database():
    try:
        id_list = get_user_id()
        cursor_existing = mydb_connection_sourcedb.cursor()
        cursor_dest = mydb_connection_destinationdb.cursor()
        query = """
        SELECT u.username,u.user_alias,u.email,u.contact_no,ur.name FROM sdg_users u 
        LEFT JOIN sdg_user_roles ur ON ur.id = u.user_role_id
        WHERE u.id = %s;
        """

        for id in id_list:
            try:
                id = id[0]
                cursor_existing.execute(query,(id,))
                user_info = cursor_existing.fetchall()
                if user_info and user_info[0]:
                    userdetail = user_info[0] if user_info[0] else None
                    username = userdetail[0] if userdetail[0] else None
                    name_eng = userdetail[1] if userdetail[1] else None
                    user_email = userdetail[2] if userdetail[2] else None
                    user_mobile = userdetail[3] if userdetail[3] else None
                    user_role_name = userdetail[4] if userdetail[4] else None
                    user_role_id = None
                    query_user_type = """SELECT id FROM user_role WHERE role_name LIKE %s;"""
                    cursor_dest.execute(query_user_type,(f"%{user_role_name}%",))
                    user_role_id_inf = cursor_dest.fetchall()
                    if user_role_id_inf and user_role_id_inf[0] and user_role_id_inf[0][0]:
                        user_role_id =user_role_id_inf[0][0]
                    user_tpl = (username, user_role_id, name_eng, user_email, user_mobile,)

                    query_insert_user = """
                    INSERT INTO users(username,user_role_id,name_eng,user_email,user_mobile)VALUES(%s,%s,%s,%s,%s);
                    """
                    cursor_dest.execute(query_insert_user,user_tpl)
                    mydb_connection_destinationdb.commit()
                    print(username, "Inserted!")

            except Exception as E:
                print(str(E))
                continue

    except Exception as E:
        print(str(E))

if __name__ == "__main__":
    get_users_from_existing_database()
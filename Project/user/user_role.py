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


def get_all_user_role():
    cursor_exist = mydb_connection_sourcedb.cursor()
    cursor_dest = mydb_connection_destinationdb.cursor()
    try:
        query = """
        SELECT sur.name
        FROM sdg_v1_v2_live.sdg_user_roles sur
        LEFT JOIN uat_sdg_tracker_clone.user_role ur ON 
        ur.role_name COLLATE utf8_unicode_ci = sur.name COLLATE utf8_unicode_ci
        WHERE ur.role_name IS NULL;
        """
        cursor_exist.execute(query)
        rows = cursor_exist.fetchall()
        for row in rows:
            query_insert = """INSERT INTO user_role(role_name) VALUES(%s)"""
            cursor_dest.execute(query_insert,(row[0],))
            mydb_connection_destinationdb.commit()
            print(row[0], " Has been inserted")

    except Exception as E:
        print(str(E))


if __name__ == "__main__":
    get_all_user_role()
import Project.connection as mysql_connection
import Project.users.query.user_role_qry as urq
def get_all_user_role():
    cursor_exist = mysql_connection.mydb_connection_sourcedb.cursor()
    cursor_dest = mysql_connection.mydb_connection_destinationdb.cursor()
    try:
        cursor_exist.execute(urq.query)
        rows = cursor_exist.fetchall()
        for row in rows:
            print(row)
            cursor_dest.execute(urq.query_insert,(row[0],))
            mysql_connection.mydb_connection_destinationdb.commit()
            print(row[0], " Has been inserted")
    except Exception as E:
        print(str(E))


if __name__ == "__main__":
    get_all_user_role()
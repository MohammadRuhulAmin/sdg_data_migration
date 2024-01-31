import Project.connection as mysql_connection
import Project.users.query.user_type as utq

def new_user_type():
    try:
        cursor_dest = mysql_connection.mydb_connection_destinationdb.cursor()
        cursor = mysql_connection.mydb_connection.cursor()
        cursor.execute(utq.unique_user_type)
        unique_type_list = cursor.fetchall()
        for unique_type in unique_type_list:
            cursor_dest.execute(utq.insert_new_user_type,(unique_type[0],))
            mysql_connection.mydb_connection_destinationdb.commit()
            print(unique_type[0], " new type has been inserted!")
    except Exception as E:
        print(str(E))

if __name__ == "__main__":
    new_user_type()
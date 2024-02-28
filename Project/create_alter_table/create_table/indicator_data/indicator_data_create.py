import Project.connection as mysql_connection
import Project.create_alter_table.create_table.indicator_data.ddl_cmd as ddl


def create_indicator_data():
    try:
        cursor_dest = mysql_connection.mydb_connection_destinationdb.cursor()
        cursor_dest.execute(ddl.create_indicator_data)
        mysql_connection.mydb_connection_destinationdb.commit()
        print("Indicator_data table created successfully!")
    except Exception as E:
        print(str(E))


if __name__ == "__main__":
    create_indicator_data()

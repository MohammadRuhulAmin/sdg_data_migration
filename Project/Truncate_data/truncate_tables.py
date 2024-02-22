import Project.connection as mysql_connection
import Project.Truncate_data.query as qry


def Truncate_tables():
    try:
        cursor_dest = mysql_connection.mydb_connection_destinationdb.cursor()
        cursor_dest.execute(qry.truncate_indicator_data)
        mysql_connection.mydb_connection_destinationdb.commit()
        print("Indicator_data table truncated successfully!")
        cursor_dest.execute(qry.truncate_indicator_disagg_data)
        mysql_connection.mydb_connection_destinationdb.commit()
        print("Indicator_disagg_data table truncated successfully!")
        cursor_dest.execute(qry.truncate_indicator_geo_data)
        mysql_connection.mydb_connection_destinationdb.commit()
        print("Indicator_geo_data table truncated successfully!")

    except Exception as E:
        print(str(E))


if __name__ == "__main__":
    Truncate_tables()
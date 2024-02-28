import Project.connection as mysql_connection
import Project.truncate_data.query as qry

def truncate_indicator():
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


def truncate_user():
    try:
        cursor_dest = mysql_connection.mydb_connection_destinationdb.cursor()
        cursor_dest.execute(qry.truncate_users)
        mysql_connection.mydb_connection_destinationdb.commit()
        print("users table truncated successfully!")
        cursor_dest.execute(qry.truncate_user_role)
        mysql_connection.mydb_connection_destinationdb.commit()
        print("user_role table truncated successfully!")
        cursor_dest.execute(qry.truncate_user_type)
        mysql_connection.mydb_connection_destinationdb.commit()
        print("user_type table truncated successfully!")

    except Exception as E:
        print(str(E))

def truncate_disaggregation():
    try:
        cursor_dest = mysql_connection.mydb_connection_destinationdb.cursor()
        cursor_dest.execute(qry.truncate_disaggregation_name)
        mysql_connection.mydb_connection_destinationdb.commit()
        print("disaggregation_name table truncated successfully!")
        cursor_dest.execute(qry.truncate_disaggregation_type)
        mysql_connection.mydb_connection_destinationdb.commit()
        print("disaggregation_type table truncated successfully!")
    except Exception as E:
        print(str(E))


def drop_indicator_data():
    try:
        cursor_dest = mysql_connection.mydb_connection_destinationdb.cursor()
        cursor_dest.execute(qry.drop_indicator_data)
        print("indicator_data table dropped")
    except Exception as E:
        print(str(E))


def truncate_rapper():
    truncate_indicator()
    truncate_user()
    truncate_disaggregation()
    drop_indicator_data()

if __name__ == "__main__":
    truncate_rapper()
import Project.connection as mysql_connection

def indicator_disagg_data_alter():
    try:
        alter_disagg_data = """
        alter table indicator_disagg_data modify ind_data_id int(11) null,
        modify disagg_id int(11) null,
        modify data_value double null,
        modify ordering int(11) null;
        """
        cursor_dest = mysql_connection.mydb_connection_destinationdb.cursor()
        cursor_dest.execute(alter_disagg_data)
        mysql_connection.mydb_connection_destinationdb.commit()
        print("indicator_disagg_data table has been altered for data migration")

    except Exception as E:
        print(str(E))

def indicator_geo_data_alter():
    try:
        alter_disagg_data = """
        alter table indicator_geo_data modify ind_data_id int(11) null,
        modify type int(11) null;
        """
        cursor_dest = mysql_connection.mydb_connection_destinationdb.cursor()
        cursor_dest.execute(alter_disagg_data)
        mysql_connection.mydb_connection_destinationdb.commit()
        print("indicator_geo_data table has been altered for data migration")

    except Exception as E:
        print(str(E))


def combine_indicator_family():
    indicator_disagg_data_alter()
    indicator_geo_data_alter()

if __name__ == "__main__":
    combine_indicator_family()
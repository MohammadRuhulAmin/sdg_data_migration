import Project.connection as mysql_connection

def disaggregation_name():
    try:
        alter_disaggregation_name_ddl = """
        alter table disaggregation_name modify type_id int(11) null;
        """
        cursor_dest = mysql_connection.mydb_connection_destinationdb.cursor()
        cursor_dest.execute(alter_disaggregation_name_ddl)
        mysql_connection.mydb_connection_destinationdb.commit()
        print("disaggregation_name table has been altered for data migration")

    except Exception as E:
        print(str(E))

def disaggregation_type():
    try:
        alter_disaggregation_type = """
        alter table disaggregation_type modify name varchar(256) null,
        modify updated_by int(11) null;
        """
        cursor_dest = mysql_connection.mydb_connection_destinationdb.cursor()
        cursor_dest.execute(alter_disaggregation_type)
        mysql_connection.mydb_connection_destinationdb.commit()
        print("disaggregation_type table has been altered for data migration")
    except Exception as E:
        print(str(E))


def combine_disaggregation():
    disaggregation_name()
    disaggregation_type()

if __name__ == "__main__":
    combine_disaggregation()
import mysql.connector
import Project.disaggregation.query.get_serial_list as sl
import Project.disaggregation.query.mapped_query as mq
import Project.disaggregation.query.other_queries as oq
import Project.connection as mysql_connection


def get_serial_no_from_exist_db():
    try:
        serial_no_list = []
        cursor_exist = mysql_connection.mydb_connection_sourcedb.cursor()
        cursor_exist.execute(sl.query)
        serial_rows = cursor_exist.fetchall()
        for serial_no in serial_rows:
            serial_no_list.append(serial_no[0])
        return serial_no_list
    except Exception as E:
        print(str(E))
def import_type_name_disagg_name():
    serial_list = get_serial_no_from_exist_db()
    cursor_source = mysql_connection.mydb_connection_sourcedb.cursor()
    cursor_dest = mysql_connection.mydb_connection_destinationdb.cursor()

    nested_array =[]
    for serial_no in serial_list:
        cursor_source.execute(mq.query, (serial_no,))
        rows = cursor_source.fetchall()
        row_count = cursor_source.rowcount
        for row in rows:
            try:
                disagg_name = row[4] if row[4] else None
                type_name = row[6] if row[6] else None
                if disagg_name is None and type_name is None:continue
                if disagg_name is not None and type_name is None:type_name = disagg_name
                if disagg_name is  None and type_name is not None: disagg_name = type_name
                nested_array.append([disagg_name,type_name])

            except Exception as E:
                print(str(E))

    disagg_info = [item for index, item in enumerate(nested_array) if item not in nested_array[:index]]
    for item in disagg_info:
        disagg_name = item[0]
        type_name = item[1]
        cursor_dest.execute(oq.query, (f"%{disagg_name}%",))
        dis_name = cursor_dest.fetchall()
        if dis_name and dis_name[0] and dis_name[0][0]:continue
        else:
            cursor_dest.execute(oq.query_search_type, (f"%{type_name}%",))
            tp_nm = cursor_dest.fetchall()
            if tp_nm and tp_nm[0] and tp_nm[0][1]:
                type_id = tp_nm[0][0]
                cursor_dest.execute(oq.qy_dnm, (type_id, disagg_name,))
                mysql_connection.mydb_connection_destinationdb.commit()
                print("Disaggregation_name has been inserted")
            else:

                cursor_dest.execute(oq.insert_type_name, (type_name,))
                mysql_connection.mydb_connection_destinationdb.commit()
                print("New Type Has been inserted", type_name)
                new_type_id = cursor_dest.lastrowid

                cursor_dest.execute(oq.qy_dnmx, (new_type_id, disagg_name,))
                mysql_connection.mydb_connection_destinationdb.commit()
                print("Disaggregation_name has been inserted", disagg_name)

    print(len(disagg_info))
if __name__ == "__main__":
    import_type_name_disagg_name()
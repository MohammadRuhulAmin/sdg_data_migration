import mysql.connector
import main


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

def import_type_name_disagg_name():
    serial_list = main.get_serial_no_from_exist_db()
    cursor_source = mydb_connection_sourcedb.cursor()
    cursor_dest = mydb_connection_destinationdb.cursor()
    query = """
           SELECT tmp.*,tmp2.type_name
        -- tmp2.disaggregation_name,
            FROM (SELECT sil.serial_no,
            sidc.sdg_disaggregation_id,
            sidc.value,stp.name data_period,
            sdl.name,sid.source_id
            FROM sdg_indicator_langs sil
            LEFT JOIN sdg_indicator_data sid ON sid.indicator_id = sil.indicator_id
            LEFT JOIN sdg_indicator_data_children sidc ON sidc.indicator_data_id = sid.id
            LEFT JOIN sdg_time_periods stp ON stp.id = sid.time_period_id
            LEFT JOIN sdg_disaggregation_langs sdl ON sdl.disaggregation_id = sidc.sdg_disaggregation_id
            WHERE sil.serial_no = %s AND sil.language_id = 1)tmp
            LEFT JOIN(SELECT child.disaggregation_id disaggregation_id,child.language_id,child.parent_id,
            child.name disaggregation_name,parent.name type_name,parent.disaggregation_id type_id FROM (SELECT id,disaggregation_id,language_id,parent_id,NAME
            FROM sdg_disaggregation_langs WHERE parent_id = 0)parent
            LEFT JOIN (SELECT id,disaggregation_id,language_id,parent_id,NAME
            FROM sdg_disaggregation_langs WHERE parent_id > 0)child
            ON parent.disaggregation_id = child.parent_id)tmp2 ON tmp.sdg_disaggregation_id=tmp2.disaggregation_id;
            """
    nested_array =[]
    for serial_no in serial_list:
        cursor_source.execute(query, (serial_no,))
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
        query = """SELECT name FROM disaggregation_name WHERE name LIKE %s;"""
        cursor_dest.execute(query, (f"%{disagg_name}%",))
        dis_name = cursor_dest.fetchall()
        if dis_name and dis_name[0] and dis_name[0][0]:continue
        else:
            query_search_type = """
            SELECT id,name FROM disaggregation_type WHERE name LIKE %s
            """
            cursor_dest.execute(query_search_type, (f"%{type_name}%",))
            tp_nm = cursor_dest.fetchall()
            if tp_nm and tp_nm[0] and tp_nm[0][1]:
                type_id = tp_nm[0][0]
                qy_dnm = """
                INSERT INTO disaggregation_name(type_id,name) VALUES(%s,%s)
                """
                cursor_dest.execute(qy_dnm, (type_id, disagg_name,))
                mydb_connection_destinationdb.commit()
                print("Disaggregation_name has been inserted")
            else:
                insert_type_name = """
                INSERT INTO disaggregation_type(name)VALUES(%s)
                """
                cursor_dest.execute(insert_type_name, (type_name,))
                mydb_connection_destinationdb.commit()
                print("New Type Has been inserted", type_name)
                new_type_id = cursor_dest.lastrowid
                qy_dnm = """
                INSERT INTO disaggregation_name(type_id,name) VALUES(%s,%s)
                """
                cursor_dest.execute(qy_dnm, (new_type_id, disagg_name,))
                mydb_connection_destinationdb.commit()
                print("Disaggregation_name has been inserted", disagg_name)

    print(len(disagg_info))
if __name__ == "__main__":
    import_type_name_disagg_name()
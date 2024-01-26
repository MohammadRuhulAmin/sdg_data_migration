


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
def indicator_disagg_data(temp_json):
    cursor_dest = mydb_connection_destinationdb.cursor()
    try:
        indicator_id_list = temp_json['indicator_id_list']
        ind_def_id_list = temp_json['ind_def_id_list']
        for index in range(0, len(indicator_id_list)):
            try:
                ind_id = indicator_id_list[index]
                ind_def_id = ind_def_id_list[index]
                source_id = temp_json['source_id']
                data_period = temp_json['data_period']
                data_value = temp_json['data_value']
                disagg_name = temp_json['disagg_name']
                insert_indicator_dis_multiple = """
                                        INSERT INTO indicator_data(ind_id,ind_def_id,source_id,data_period) VALUES (%s,%s,%s,%s);
                                        """
                cursor_dest.execute(insert_indicator_dis_multiple, (ind_id, ind_def_id, source_id, data_period,))
                ind_data_id = cursor_dest.lastrowid
                disagg_name = disagg_name
                data_value = temp_json['data_value']
                disagg_id = None
                query_get_disagg_id = """
                                        SELECT id,name FROM disaggregation_name
                                        WHERE `name` like %s;
                                        """
                cursor_dest.execute(query_get_disagg_id, (f"%{disagg_name}%",))
                row = cursor_dest.fetchall()
                if row and row[0] and row[0][0]:
                    disagg_id = row[0][0]
                else:
                    disagg_id = None
                if row and row[0] and row[0][1]:
                    disagg_name = row[0][1]
                else:
                    disagg_name = None
                insert_in_disagg_data = """
                INSERT INTO indicator_disagg_data(ind_data_id,disagg_id,disagg_name,data_value)
                VALUES(%s,%s,%s,%s)
                """
                cursor_dest.execute(insert_in_disagg_data, (ind_data_id, disagg_id, disagg_name, data_value))
                mydb_connection_destinationdb.commit()
                print("Data inserted in indicator_disagg_data ", ind_data_id, disagg_id, disagg_name, data_value)
            except Exception as E:
                print(str(E))
    except Exception as E:
        print(str(E))
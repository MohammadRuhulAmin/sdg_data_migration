from indicator_data import ind_data_values
import Project.connection as mysql_connection

def indicator_disagg_data(temp_json):
    cursor_dest = mysql_connection.mydb_connection_destinationdb.cursor()
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
                ind_data_id = ind_data_values['ind_data_id']
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
                mysql_connection.mydb_connection_destinationdb.commit()
                #print("Data inserted in indicator_disagg_data ", ind_data_id, disagg_id, disagg_name, data_value)
            except Exception as E:
                print(str(E))
    except Exception as E:
        print(str(E))
from Project.indicator.controller.indicator_data import ind_data_values
import Project.connection as mysql_connection
import Project.indicator.query.get_indicator_id as indidc
import Project.indicator.query.other_queries as oq

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
                created_at = temp_json['created_at']
                updated_at = temp_json['updated_at']
                disagg_id = None
                cursor_dest.execute(oq.query_get_disagg_id, (f"%{disagg_name}%",))
                row = cursor_dest.fetchall()
                if row and row[0] and row[0][0]:disagg_id = row[0][0]
                else:disagg_id = None
                if row and row[0] and row[0][1]:disagg_name = row[0][1]
                else:disagg_name = None
                cursor_dest.execute(indidc.insert_in_disagg_data, (ind_data_id, disagg_id, disagg_name, data_value,created_at,updated_at,))
                mysql_connection.mydb_connection_destinationdb.commit()
            except Exception as E:
                print(str(E))
                with open("log.txt", 'a') as f:
                    f.write("error_table: indicator_disagg_data"+ "\n")
                    f.write(str(temp_json) + "\n")
                    f.write(str(E) + "\n")
    except Exception as E:
        print(str(E))
import Project.connection as mysql_connection
import Project.indicator.query.get_indicator_id as indid
ind_data_values = {
    "ind_data_id" :None
}
def indicator_data(temp_json):
    cursor_dest = mysql_connection.mydb_connection_destinationdb.cursor()
    try:
        indicator_id_list = temp_json['indicator_id_list']
        ind_def_id_list = temp_json['ind_def_id_list']
        for index in range(0, len(indicator_id_list)):
            ind_id = indicator_id_list[index]
            ind_def_id = ind_def_id_list[index]
            source_id = temp_json['source_id']
            data_period = temp_json['data_period']
            data_value = temp_json['data_value']
            status = temp_json['status']
            created_at = temp_json['created_at']
            updated_at = temp_json['updated_at']
            cursor_dest.execute(indid.insert_indicator_dis_1, (ind_id, ind_def_id, source_id, data_period, data_value,status,created_at,updated_at,))
            mysql_connection.mydb_connection_destinationdb.commit()
            ind_data_values['ind_data_id'] = cursor_dest.lastrowid
            #print("data inserted in indicator_data when disaggregation_id = 1", ind_id, ind_def_id, source_id,
            #data_period, data_value)
    except Exception as E:
        print(str(E))


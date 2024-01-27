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

ind_data_values = {
    "ind_data_id" :None
}
def indicator_data(temp_json):
    cursor_dest = mydb_connection_destinationdb.cursor()
    try:
        indicator_id_list = temp_json['indicator_id_list']
        ind_def_id_list = temp_json['ind_def_id_list']
        for index in range(0, len(indicator_id_list)):
            ind_id = indicator_id_list[index]
            ind_def_id = ind_def_id_list[index]
            source_id = temp_json['source_id']
            data_period = temp_json['data_period']
            data_value = temp_json['data_value']
            insert_indicator_dis_1 = """
            INSERT INTO indicator_data (ind_id,ind_def_id,source_id, data_period, data_value) VALUES ( %s,%s, %s, %s,%s);
            """
            cursor_dest.execute(insert_indicator_dis_1, (ind_id, ind_def_id, source_id, data_period, data_value,))
            mydb_connection_destinationdb.commit()
            ind_data_values['ind_data_id'] = cursor_dest.lastrowid
            #print("data inserted in indicator_data when disaggregation_id = 1", ind_id, ind_def_id, source_id,
            #data_period, data_value)
    except Exception as E:
        print(str(E))

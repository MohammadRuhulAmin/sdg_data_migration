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

def get_serial_no_from_exist_db():
    try:
        serial_no_list = []
        cursor_exist = mydb_connection_sourcedb.cursor()
        query = """
        select sil.serial_no from sdg_indicator_langs sil
        where sil.language_id = 1
        group by sil.serial_no
        order by sil.serial_no;
        """
        cursor_exist.execute(query)
        serial_rows = cursor_exist.fetchall()
        for serial_no in serial_rows:
            serial_no_list.append(serial_no[0])
        return serial_no_list
    except Exception as E:
        print(str(E))



def operation_mapped_data(serial_no_list):
    try:
        cursor_source = mydb_connection_sourcedb.cursor()
        cursor_dest = mydb_connection_destinationdb.cursor()
        query = """
        SELECT ind.serial_no IndicatorID,
        ind_data_c.sdg_disaggregation_id dis_id,
         ind_data_c.value `Value`, 
        -- ind_data.status `Status`,
        -- ind_data.publish Publish,
        t.name TimePeriod,
        dis.name DisaggregationName,
         -- s.name SourceName, 
        s.source_id 
        FROM sdg_indicator_langs ind
        LEFT JOIN`sdg_indicator_data` ind_data ON ind.id = ind_data.indicator_id
        LEFT JOIN sdg_time_periods t ON ind_data.time_period_id = t.id
        LEFT JOIN sdg_arrange_source_langs s ON ind_data.source_id = s.source_id
        LEFT JOIN sdg_indicator_data_children ind_data_c ON ind_data.id = ind_data_c.indicator_data_id 
        LEFT JOIN sdg_disaggregation_langs dis ON ind_data_c.sdg_disaggregation_id = dis.disaggregation_id
        WHERE ind.serial_no = %s AND ind.language_id=1 AND s.language_id=1 
        GROUP BY  
        t.name , s.name,ind.serial_no,t.name,s.name,dis.name,
        ind_data_c.value,ind_data.status,
        ind_data.publish,ind_data_c.sdg_disaggregation_id,s.source_id;
        """
        for serial_no in serial_no_list:
            cursor_source.execute(query,(serial_no,))
            rows = cursor_source.fetchall()
            for row in rows:
                try:

                    serial_no = row[0]
                    disaggregation_id = row[1]
                    data_value = row[2]
                    data_period = row[3]
                    disagg_name = row[4]
                    source_id = row[5]
                    get_indicator_id = """SELECT id FROM sdg_indicator_details WHERE indicator_number = %s;"""
                    cursor_dest.execute(get_indicator_id, (serial_no,))
                    indicator_id_list = cursor_dest.fetchall()
                    # getting source_id from ind_sources table
                    query_source = """SELECT * FROM mapped_sources WHERE old_source_id =%s """
                    cursor_dest.execute(query_source, (source_id,))
                    new_source_id = cursor_dest.fetchall()[0][1]
                    # getting ind_def_id
                    # getting ind_def_id from ind_definitions table

                    temp_ind_def_id_list = []
                    temp_indicator_id_list = []
                    for indicator_id in indicator_id_list:
                        indicator_id = indicator_id[0]
                        query_ind_def = """SELECT id FROM ind_definitions WHERE ind_id = %s LIMIT 1"""
                        cursor_dest.execute(query_ind_def,(indicator_id,))
                        temp_indicator_id_list.append(indicator_id)
                        ind_def_id = cursor_dest.fetchone()
                        temp_ind_def_id_list.append(ind_def_id[0] if ind_def_id else None)


                    temp_json = {
                        'serial_no':serial_no if serial_no else None,
                        'disaggregation_id':disaggregation_id if disaggregation_id else None,
                        'data_period':data_period if data_period else None,
                        'data_value':data_value if data_value else None,
                        'disagg_name':disagg_name if disagg_name else None,
                        'source_id':new_source_id if new_source_id else None,
                        'indicator_id_list':temp_indicator_id_list if temp_indicator_id_list else None,
                        'ind_def_id_list':temp_ind_def_id_list if temp_ind_def_id_list else None
                    }
                    print("--------------------------")
                    print(row)
                    print(temp_indicator_id_list, temp_ind_def_id_list)
                    print(temp_json)
                    print("--------------------------")

                except Exception as E:continue



    except Exception as E:
        print(str(E))



### NOTE: indicator_data er data_period need to be in varchar
## all column will be null


if __name__ == "__main__":
    serial_no_list = get_serial_no_from_exist_db()
    operation_mapped_data(serial_no_list)
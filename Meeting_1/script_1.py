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
        SELECT 
                #sdg_indicator_langs sil
                sil.serial_no serial_no,
                #sdg_indicator_data ind_data
                ind_data.indicator_id indicator_id,
                ind_data.time_period_id time_period_id,
                #sdg_indicator_data_children sidc
                sidc.sdg_disaggregation_id disaggregation_id,
                sidc.value `value`,
                #sdg_time_periods tp
                tp.name,
                #sdg_disaggregation_langs sdl
                sdl.name dis_name,
                #sdg_arrange_source_langs sasl
                sasl.name source_name,
                sasl.source_id
        FROM sdg_indicator_data ind_data
        LEFT JOIN sdg_indicator_data_children sidc ON sidc.id = ind_data.indicator_id
        LEFT JOIN sdg_time_periods tp ON tp.id = ind_data.time_period_id
        LEFT JOIN sdg_indicator_langs sil ON sil.indicator_id = ind_data.indicator_id
        LEFT JOIN sdg_disaggregation_langs sdl ON sdl.disaggregation_id = sidc.sdg_disaggregation_id
        LEFT JOIN sdg_arrange_source_langs sasl ON ind_data.source_id = sasl.source_id
        WHERE sil.serial_no = %s  AND sil.language_id=1 
        AND sasl.language_id = 1
        GROUP BY ind_data.indicator_id,ind_data.time_period_id,
            sidc.sdg_disaggregation_id,
            sidc.value,tp.name,sdl.name,
            sasl.name,sasl.source_id
            ORDER BY sil.serial_no,ind_data.indicator_id,ind_data.time_period_id,
            sidc.sdg_disaggregation_id,
            sidc.value,tp.name,sdl.name;
        """

        for serial_no in serial_no_list:
            cursor_source.execute(query,(serial_no,))
            results = cursor_source.fetchall()
            for row in results:
                serial_no = row[0]
                indicator_id = row[1]
                time_period_id = row[2]
                disaggregation_id = row[3]
                value = row[4]
                time_name = row[5]
                disagg_name = row[6]
                source_name = row[7]
                source_id = row[8]
                try:
                    # getting source_id from ind_sources table
                    query_source = """SELECT * FROM mapped_sources WHERE old_source_id =%s """
                    cursor_dest.execute(query_source,(source_id,))
                    new_source_id = cursor_dest.fetchall()[0][1]
                    # getting ind_def_id from ind_definitions table
                    query_ind_def = """SELECT id FROM ind_definitions WHERE ind_id = %s"""
                    cursor_dest.execute(query_ind_def,(indicator_id,))
                    new_ind_def_id = cursor_dest.fetchall()[0][0]
                    if disaggregation_id == 1:
                        insert_indicator_dis_1 = """
                        INSERT INTO indicator_data (ind_def_id,source_id, data_period, data_value) VALUES (%s, %s, %s, %s);
                        """
                        cursor_dest.execute(insert_indicator_dis_1,(new_ind_def_id,source_id,time_name,value,))
                        mydb_connection_destinationdb.commit()
                        print("data inserted in indicator_data when disaggregation_id = 1")
                    else:
                        print(new_ind_def_id,source_id,time_name,value)
                        insert_indicator_dis_multiple = """
                        INSERT INTO indicator_data(ind_def_id,source_id,data_period) VALUES (%s,%s,%s);
                        """
                        cursor_dest.execute(insert_indicator_dis_multiple,(new_ind_def_id,source_id,time_name,))
                        mydb_connection_destinationdb.commit()
                        last_inserted_id = cursor_dest.lastrowid
                        ind_data_id = last_inserted_id
                        disagg_name = disagg_name
                        data_value = value
                        query_get_disagg_id = """
                        SELECT id,name FROM disaggregation_name 
                        WHERE `name` like %s;
                        """
                        cursor_dest.execute(query_get_disagg_id, (f"%{disagg_name}%",))
                        disagg_id = cursor_dest.fetchall()[0][0]
                        insert_in_disagg_data = """
                         INSERT INTO indicator_disagg_data(ind_data_id,disagg_id,disagg_name,data_value)
                         VALUES(%s,%s,%s,%s)
                        """
                        cursor_dest.execute(insert_in_disagg_data,(ind_data_id,disagg_id,disagg_name,data_value))
                        mydb_connection_destinationdb.commit()
                        print("Data inserted in indicator_disagg_data ",ind_data_id,disagg_id,disagg_name,data_value)





                except Exception as E:
                    continue
    except Exception as E:
        print(str(E))



### NOTE: indicator_data er data_period need to be in varchar
## all column will be null


if __name__ == "__main__":
    serial_no_list = get_serial_no_from_exist_db()
    operation_mapped_data(serial_no_list)
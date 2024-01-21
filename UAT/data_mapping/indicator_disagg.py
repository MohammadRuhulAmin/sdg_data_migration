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

def insert_data_destination_indicator_data(serial_no, time_name, value):
    try:
        cursor_destination = mydb_connection_destinationdb.cursor()
        # Getting indicator_id using indicator_no from sdg_indicator_details
        query_get_serial_no = """
        SELECT indicator_id, indicator_number FROM sdg_indicator_details 
        WHERE indicator_number = %s;
        """
        cursor_destination.execute(query_get_serial_no, (serial_no,))
        indicator_info = cursor_destination.fetchall()
        print(time_name,value)
        for info in indicator_info:
            indicator_id = info[0]
            insert_query = """
            INSERT INTO indicator_data (ind_id, data_period, data_value) VALUES (%s, %s, %s);
            """
            insert_qury = (indicator_id, time_name, value)

            cursor_destination.execute(insert_query, insert_qury)
            mydb_connection_destinationdb.commit()
            print("Updated on indicator_data", indicator_id, time_name, value)

    except Exception as e:
        print(f"MySQL error: {e}")



def insert_date_destination_indicator_disagg_data(serial_no,disagg_name,value):
    try:
        if value is None:value = 0

        cursor_destination = mydb_connection_destinationdb.cursor()
        # getting indicator_id using indicator_no from sdg_indicator_details
        query_get_serial_no = """
                SELECT indicator_id,indicator_number from sdg_indicator_details 
                WHERE indicator_number = %s;
                """
        cursor_destination.execute(query_get_serial_no, (serial_no,))
        indicator_info = cursor_destination.fetchall()
        for row in indicator_info:
            indicator_id = row[0]
            serial_no = row[1]
            #getting id of disagg_name
            query_disagg_name = """
            SELECT id,name FROM disaggregation_name 
            WHERE `name` like %s;
            """

            cursor_destination.execute(query_disagg_name,(f"%{disagg_name}%",))
            disagg_details =  cursor_destination.fetchall()
            for disagg_delt in disagg_details:
                disagg_id = disagg_delt[0] #disagg_id
                disagg_name = disagg_delt[1] #disagg_name
                ind_data_id = indicator_id #ind_data_id (indicator data er id porbe)
                query_insert_indicator_disagg_data = """
                INSERT INTO indicator_disagg_data(ind_data_id,disagg_id,disagg_name,data_value)
                VALUES(%s,%s,%s,%s)
                """
                cursor_destination.execute(query_insert_indicator_disagg_data,(ind_data_id,disagg_id,disagg_name,value))
                mydb_connection_destinationdb.commit()
                print("data inserted on indicator_disagg_data ",ind_data_id,disagg_id,disagg_name)

    except Exception as E:
        print(str(E))


def unique_nested_array(nested_array):
    try:
        unique_tuples_set = set(tuple(subarray) for subarray in nested_array)
        unique_information = [list(t) for t in unique_tuples_set]
        return unique_information
    except Exception as E:
        return str(E)


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

def insert_disagg_info_ind_def_disagg(unique_defination_data):
    try:
        cursor_dest = mydb_connection_destinationdb.cursor()
        for defination_data in unique_defination_data:
            indicator_id = defination_data[0]
            ind_id = indicator_id
            disagg_id = defination_data[1]
            disagg_name = defination_data[2]
            # print(indicator_id,disagg_id,disagg_name)
            # now getting ind_def_id
            try:
                query = """
                SELECT id FROM uat_sdg_tracker_clone.ind_definitions
                WHERE ind_id = %s;
                """
                cursor_dest.execute(query,(indicator_id,))
                ind_def_id = cursor_dest.fetchall()[0][0]
                find_same_row = """
                SELECT *
                FROM uat_sdg_tracker_clone.ind_def_disagg WHERE ind_id = %s AND 
                ind_def_id = %s AND disagg_id = %s AND disagg_name = %s;
                """
                cursor_dest.execute(find_same_row,(ind_id,ind_def_id,disagg_id,disagg_name,))
                row = cursor_dest.fetchone()
                if row:
                    continue
                else:
                    insert_ind_def_disagg = """
                    INSERT INTO uat_sdg_tracker_clone.ind_def_disagg(ind_id,ind_def_id,disagg_id,disagg_name)
                    VALUES(%s,%s,%s,%s);
                    """
                    cursor_dest.execute(insert_ind_def_disagg,(ind_id,ind_def_id,disagg_id,disagg_name,))
                    mydb_connection_destinationdb.commit()
                    print("Inserted in ind_def_disagg:  ",ind_id,ind_def_id,disagg_id,disagg_name)
            except Exception as E:
                continue


    except Exception as E:
        print(str(E))


def operation_mapped_data(serial_no_list):
    try:
        cursor_source = mydb_connection_sourcedb.cursor()
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
                sdl.name dis_name
        FROM sdg_indicator_data ind_data
        LEFT JOIN sdg_indicator_data_children sidc ON sidc.id = ind_data.indicator_id
        LEFT JOIN sdg_time_periods tp ON tp.id = ind_data.time_period_id
        LEFT JOIN sdg_indicator_langs sil ON sil.indicator_id = ind_data.indicator_id
        LEFT JOIN sdg_disaggregation_langs sdl ON sdl.disaggregation_id = sidc.sdg_disaggregation_id
        WHERE sil.serial_no = %s  AND sil.language_id=1  
        GROUP BY ind_data.indicator_id,ind_data.time_period_id,
            sidc.sdg_disaggregation_id,
            sidc.value,tp.name,sdl.name
            ORDER BY sil.serial_no,ind_data.indicator_id,ind_data.time_period_id,
            sidc.sdg_disaggregation_id,
            sidc.value,tp.name,sdl.name;
        """
        nested_array = []
        for serial_no in serial_no_list:
            cursor_source.execute(query,(serial_no,))
            results = cursor_source.fetchall()
            for row in results:
                indicator_id = row[1]
                disaggregation_id = row[3]
                disagg_name = row[6]
                nested_array.append([indicator_id,disaggregation_id,disagg_name])

        unique_defination_data = unique_nested_array(nested_array)
        # insert disagg_name,disagg_id and ind_def_id in uat.ind_def_disagg table. Ind_def_id = ind_definations.id where ind_id = indicator_id
        insert_disagg_info_ind_def_disagg(unique_defination_data)

        for serial_no in serial_no_list:
            cursor_source.execute(query, (serial_no,))
            results = cursor_source.fetchall()
            for row in results:
                serial_no = row[0]
                indicator_id = row[1]
                time_period_id = row[2]
                disaggregation_id = row[3]
                value = row[4]
                time_name = row[5]
                disagg_name = row[6]
                if disaggregation_id == 1:
                    insert_data_destination_indicator_data(serial_no,time_name,value)
                else:
                    continue
                    insert_date_destination_indicator_disagg_data(serial_no,disagg_name,value)




    except Exception as E:
        print(str(E))



#step 1: get unique serial_no from sdg_indicator_langs
#step 2: get mapped data from different table using query by using serial_no from step1
#step 3: insert disagg_name,disagg_id and ind_def_id in uat.ind_def_disagg table. Ind_def_id = ind_definations.id where ind_id = indicator_id

#step 4:
#        if disaggregation_id == 1 then data_period and data_value will be inserted in uat.indicator_data table
#       else data will be inserted in uat.indicator_disagg_data table

if __name__ == "__main__":
    serial_no_list = get_serial_no_from_exist_db()
    operation_mapped_data(serial_no_list)
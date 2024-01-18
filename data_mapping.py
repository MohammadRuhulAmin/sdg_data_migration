from datetime import datetime
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


def get_mapped_data_from_source_db():
    try:
        cursor_mapped_data = mydb_connection_sourcedb.cursor()
        mapped_query = """
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
        from sdg_indicator_data ind_data
        left join sdg_indicator_data_children sidc on sidc.id = ind_data.indicator_id
        left join sdg_time_periods tp on tp.id = ind_data.time_period_id
        left join sdg_indicator_langs sil on sil.indicator_id = ind_data.indicator_id
        left join sdg_disaggregation_langs sdl on sdl.disaggregation_id = sidc.sdg_disaggregation_id;
        """
        cursor_mapped_data.execute(mapped_query)
        rows = cursor_mapped_data.fetchall()
        return rows

    except Exception as E:
        print(str(E))

def insert_data_destination_indicator_data(serial_no,time_name,value):
    try:
        cursor_destination = mydb_connection_destinationdb.cursor()
        #getting indicator_id using indicator_no from sdg_indicator_details
        query_get_serial_no = """
        SELECT indicator_id,indicator_number from sdg_indicator_details 
        WHERE indicator_number = %s;
        """
        cursor_destination.execute(query_get_serial_no,(serial_no,))
        indicator_info = cursor_destination.fetchall()
        for info in indicator_info:
            indicator_id = info[0]
            update_query = """
            UPDATE indicator_data SET data_period = %s, data_value = %s
            WHERE id = %s
            """
            if_only_update = (time_name,value,indicator_id,)
            cursor_destination.execute(update_query,(if_only_update))
            mydb_connection_destinationdb.commit()
            print("updated on indicator_data", indicator_id,time_name,value)
    except Exception as E:
        print(str(E))

def insert_date_destination_indicator_disagg_data(serial_no,disagg_name):
    try:
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
                ind_data_id = indicator_id #ind_data_id
                query_insert_indicator_disagg_data = """
                INSERT INTO indicator_disagg_data(ind_data_id,disagg_id,disagg_name)
                VALUES(%s,%s,%s)
                """
                cursor_destination.execute(query_insert_indicator_disagg_data,(ind_data_id,disagg_id,disagg_name))
                mydb_connection_destinationdb.commit()
                print("data inserted on indicator_disagg_data ",ind_data_id,disagg_id,disagg_name)

    except Exception as E:
        print(str(E))

def insert_mapped_data_to_destination_db():
    try:
        mapped_data = get_mapped_data_from_source_db()
        for row in mapped_data:
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
                insert_date_destination_indicator_disagg_data(serial_no,disagg_name)
    except Exception as E:
        print(str(E))



if __name__ == "__main__":
    insert_mapped_data_to_destination_db()
    mydb_connection_sourcedb.close()
    mydb_connection_destinationdb.close()

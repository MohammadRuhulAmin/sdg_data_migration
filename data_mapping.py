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
        current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
            #22
            upsert_query = """
            INSERT INTO indicator_data (ind_id,ind_def_id,source_id,provider_id,approver_id,publisher_id,
            sent_for_approval_date,approved_date,published_date,data_period,raw_data_json,data_value,
            remarks,status,is_archived,created_at,created_by,updated_at,updated_by)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
            id = %s;
            """
            update_query = """
            
            UPDATE indicator_data SET data_period = %s, data_value = %s
            WHERE id = %s
            """
            if_only_update = (time_name,value,indicator_id,)
            if_upsert = (0,0,0,0,0,0,0,None,None,time_name,None,value,
                         None,0,0,current_date,0,current_date,0,
                        indicator_id,)
            cursor_destination.execute(update_query,(if_upsert))


            mydb_connection_destinationdb.commit()
            print("updated on table indicator_data", indicator_id,time_name,value)
    except Exception as E:
        print(str(E))


def insert_mapped_data_to_destination_db():
    try:
        mapped_data = get_mapped_data_from_source_db()
        for row in mapped_data:
            disaggregation_id = row[3]
            indicator_id = row[1]
            time_name = row[5]
            serial_no = row[0]
            value = row[4]
            if disaggregation_id == 1:
                insert_data_destination_indicator_data(serial_no,time_name,value)
            # serial_no = row[0]
            #
            # time_period_id = row[2]
            # disaggregation_id = row[3]
            #disagg_name = row[6]

    except Exception as E:
        print(str(E))





if __name__ == "__main__":
    insert_mapped_data_to_destination_db()
    mydb_connection_sourcedb.close()
    mydb_connection_destinationdb.close()

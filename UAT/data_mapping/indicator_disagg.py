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

def get_mapped_data(serial_no_list):
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
        for serial_no in serial_no_list:
            cursor_source.execute(query,(serial_no,))
            results = cursor_source.fetchall()
            for row in results:
                print(row)



    except Exception as E:
        print(str(E))



#step 1: get unique serial_no from sdg_indicator_langs
#step 2: get mapped data from different table using query by using serial_no from step1

if __name__ == "__main__":
    serial_no_list = get_serial_no_from_exist_db()
    get_mapped_data(serial_no_list)
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









if __name__ == "__main__":
    print(get_mapped_data_from_source_db())
    mydb_connection_sourcedb.close()
    mydb_connection_destinationdb.close()

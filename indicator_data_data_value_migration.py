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


def get_serial_no_from_sdg_indicator_langs():
    try:
        cursor_get_serial_no = mydb_connection_sourcedb.cursor()
        query = "select serial_no from sdg_indicator_langs group by serial_no;"
        cursor_get_serial_no.execute(query)
        serial_no = cursor_get_serial_no.fetchall()
        return serial_no
    except Exception as E:
        print(str(E))


def uat_data_operation(serial,value):
    try:
        cursor_uat = mydb_connection_destinationdb.cursor()
        query_find_indicator_data = """
        SELECT indicator_id FROM sdg_indicator_details 
        WHERE indicator_number = %s;
        """
        cursor_uat.execute(query_find_indicator_data,(serial,))
        indicator_id = cursor_uat.fetchall()[0][0]
        #print(indicator_id[0][0],serial,value)

        update_value_query = """
        UPDATE indicator_data SET data_value = %s
        WHERE id = %s
        """
        cursor_uat.execute(update_value_query,(value,indicator_id,))
        print("id->",indicator_id, "data_value->",value,"serial_no->",serial," has been updated!")
        mydb_connection_destinationdb.commit()

    except Exception as E:
        print(str(E))
def get_mapped_data_from_source():
    try:
        serial_no_list = get_serial_no_from_sdg_indicator_langs()
        cursor_mapped_data = mydb_connection_sourcedb.cursor()
        query_mapped = """
        SELECT
            #sdg_indicator_data
            ind_data.publish,
            ind_data.status,
            ind_data.time_period_id,
            #sdg_indocator_data_children
            ind_data_c.value `Value`,
            #sdg_time_periods
            t.name TimePeriod,
            #sdg_indicator_langs
            ind.serial_no serial_no,
            #sdg_arrange_source_langs
            s.name SourceName,
            #sdg_disaggrigation_langs
            dis.name DisaggregationName
        FROM sdg_indicator_data ind_data
        LEFT JOIN sdg_indicator_data_children ind_data_c ON ind_data_c.indicator_data_id = ind_data.id
        LEFT JOIN sdg_time_periods t ON t.id = ind_data.time_period_id
        LEFT JOIN sdg_indicator_langs ind ON ind.id = ind_data.indicator_id
        LEFT JOIN sdg_arrange_source_langs s ON s.source_id = ind_data.source_id
        LEFT JOIN sdg_disaggregation_langs dis ON dis.disaggregation_id = ind_data_c.sdg_disaggregation_id
        WHERE ind.serial_no = %s
        AND ind.language_id = 1
        AND s.language_id = 1
        GROUP BY
            ind_data.publish,
            ind_data.status,
            ind_data.time_period_id,
            ind_data_c.value,
            t.name,
            ind.serial_no,
            s.name,
            dis.name;
        """
        for serial_no in serial_no_list:
            cursor_mapped_data.execute(query_mapped,(serial_no[0],))
            rows = cursor_mapped_data.fetchall()
            for row in rows:
                serial = row[5]
                value = row[3]
                uat_data_operation(serial,value)



    except Exception as E:
        print(str(E))



# work flow:

# step 1: get serial_no from sdg_indicator_langs
# step 2: get value data from given query using serial_no
# step 3: get indicator_id from sdg_indicator_details using serial_no
# step 4: update data_value where id = indicator_id in indicator_data table

if __name__ == "__main__":
    get_mapped_data_from_source()
    mydb_connection_sourcedb.close()
    mydb_connection_destinationdb.close()

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
            data = cursor_mapped_data.fetchall()
            print(data)


    except Exception as E:
        print(str(E))


if __name__ == "__main__":
    get_mapped_data_from_source()
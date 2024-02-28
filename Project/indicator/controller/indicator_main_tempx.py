import indicator_data as id
import indicator_disagg_data as idd
import Project.indicator.query.mapped_query_tempx as qmap
import Project.indicator.query.get_serial_list as sl
import Project.indicator.query.get_indicator_id as indi_id
import Project.connection as mysql_connection
import Project.indicator.query.other_queries as oq
import Project.indicator.controller.indicator_geo_data as igd

status_mapping = {(3, 4): 1, (4, 4): 2, (5, 4): 3, (5, 5): 4}
def get_serial_no_from_exist_db():
    try:
        serial_no_list = []
        cursor_exist = mysql_connection.mydb_connection_sourcedb.cursor()
        cursor_exist.execute(sl.query)
        serial_rows = cursor_exist.fetchall()
        for serial_no in serial_rows:
            serial_no_list.append(serial_no[0])
        return serial_no_list
    except Exception as E:
        print(str(E))

def operation_mapped_data(serial_no_list):
    try:
        cursor_source = mysql_connection.mydb_connection_sourcedb.cursor()
        cursor_dest = mysql_connection.mydb_connection_destinationdb.cursor()
        for serial_no in serial_no_list:
            cursor_source.execute(qmap.query,(serial_no,))
            rows = cursor_source.fetchall()
            for row in rows:
                try:
                    serial_no, disaggregation_id, data_value, data_period, disagg_name, source_id, type_name, status, publish,created_at,\
                    updated_at,is_location,geo_division_id,geo_division_name,geo_district_id,geo_district_name,geo_upazila_id,\
                    geo_upazila_name= row[:19]
                    if (status, publish) in status_mapping:status = status_mapping[(status, publish)]
                    cursor_dest.execute(indi_id.get_indicator_id, (serial_no,))
                    indicator_id_list = cursor_dest.fetchall()
                    # getting source_id from ind_sources table
                    cursor_dest.execute(oq.query_source, (source_id,))
                    new_source_id = cursor_dest.fetchall()[0][3]
                    # getting ind_def_id
                    # getting ind_def_id from ind_definitions table
                    temp_ind_def_id_list = []
                    temp_indicator_id_list = []
                    for indicator_id in indicator_id_list:
                        indicator_id = indicator_id[0]
                        cursor_dest.execute(oq.query_ind_def,(indicator_id,))
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
                        'ind_def_id_list':temp_ind_def_id_list if temp_ind_def_id_list else None,
                        'type_name':type_name if type_name else None,
                        'status':status,
                        'created_at':created_at,
                        'updated_at':updated_at,
                        'is_location':is_location,
                        'geo_division_id':geo_division_id,
                        'geo_division_name':geo_division_name,
                        'geo_district_id':geo_district_id,
                        'geo_district_name':geo_district_name,
                        'geo_upazila_id':geo_upazila_id,
                        'geo_upazila_name':geo_upazila_name
                    }
                    print(temp_json)
                    if temp_json.get('is_location') == 0:
                        if temp_json.get('disaggregation_id') == 1:id.indicator_data(temp_json)
                        else:idd.indicator_disagg_data(temp_json)
                    if temp_json.get('is_location') == 1:
                        igd.indicator_geo_data(temp_json)

                except Exception as E:continue
    except Exception as E:
        print(str(E))

def indicator_rapper_main():
    serial_no_list = get_serial_no_from_exist_db()
    operation_mapped_data(serial_no_list)

if __name__ == "__main__":
    indicator_rapper_main()
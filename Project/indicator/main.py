import indicator_data as id
import indicator_disagg_data as idd
import query.mapped_query as qmap
import query.get_serial_list as sl
import query.get_indicator_id as indi_id
import Project.connection as mysql_connection

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
                    serial_no = row[0]
                    disaggregation_id = row[1]
                    data_value = row[2]
                    data_period = row[3]
                    disagg_name = row[4]
                    source_id = row[5]
                    type_name = row[6]
                    status = row[7]
                    publish = row[8]
                    if status == 5 and publish == 5:status = 5
                    else:status = 1

                    cursor_dest.execute(indi_id.get_indicator_id, (serial_no,))
                    indicator_id_list = cursor_dest.fetchall()
                    # getting source_id from ind_sources table
                    query_source = """SELECT * FROM mapped_sources WHERE old_source_id =%s """
                    cursor_dest.execute(query_source, (source_id,))
                    new_source_id = cursor_dest.fetchall()[0][3]
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
                        'ind_def_id_list':temp_ind_def_id_list if temp_ind_def_id_list else None,
                        'type_name':type_name if type_name else None,
                        'status':status
                    }

                    print("Indicator: ",serial_no)
                    if disaggregation_id == 1:id.indicator_data(temp_json)
                    else:idd.indicator_disagg_data(temp_json)


                except Exception as E:continue
    except Exception as E:
        print(str(E))

### NOTE: indicator_data er data_period need to be in varchar
## all column will be null


if __name__ == "__main__":
    serial_no_list = get_serial_no_from_exist_db()
    operation_mapped_data(serial_no_list)
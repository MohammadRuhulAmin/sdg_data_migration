import Project.connection as mysql_connection
import Project.ind_def_disagg.query.mapping_query as mq


def get_indicator_number_from_uat():
    try:
        cursor_uat = mysql_connection.mydb_connection_destinationdb.cursor()
        cursor_uat.execute(mq.indicator_number_list)
        indicator_number_list = cursor_uat.fetchall()
        return indicator_number_list
    except Exception as E:
        print(str(E))

def ind_def_disagg():
    try:
        cursor_dest = mysql_connection.mydb_connection_destinationdb.cursor()
        indicator_number_list = get_indicator_number_from_uat()
        for indicator_number in indicator_number_list:
            print(indicator_number[0])
            cursor_dest.execute(mq.mapped_query,(indicator_number[0],))
            rows = cursor_dest.fetchall()
            for row in rows:
                ind_id,ind_def_id,disagg_type_id,disagg_id,disagg_name = row[:5]
                temp_tuple = (ind_id,ind_def_id,disagg_type_id,disagg_id,disagg_name,)
                cursor_dest.execute(mq.insert_into_ind_def_disagg,temp_tuple)
                mysql_connection.mydb_connection_destinationdb.commit()
                print("Data inserted for indicator", indicator_number)

    except Exception as E:
        print(str(E))


if __name__ == "__main__":
    ind_def_disagg()

import Project.connection as mysql_connection
import Project.indicator.additional_column.provider_id.query.time_period as tpq

def get_indicator_number_list_and_time_period__listfrom_uat():
    try:
        cursor = mysql_connection.mydb_connection.cursor()
        cursor_uat = mysql_connection.mydb_connection_destinationdb.cursor()
        cursor_exist = mysql_connection.mydb_connection_sourcedb.cursor()
        cursor_uat.execute(tpq.indicator_wise_time_period_list)
        rows = cursor_uat.fetchall()
        for row in rows:
            try:
                indicator_number = row[0]
                cursor.execute(tpq.get_specific_user_info_by_time_period,(indicator_number,))
                temp_data = cursor.fetchall()
                for temp in temp_data:
                    data_period = temp[3]
                    provider_id = temp[5]
                    indicator_id = temp[7]
                    cursor_uat.execute(tpq.update_provider_id,(provider_id,indicator_id,data_period,))
                    mysql_connection.mydb_connection_destinationdb.commit()
                    print(provider_id, " has been updated to " , indicator_id, data_period)

            except Exception as E:
                continue
    except Exception as E:
        print(str(E))



if __name__ == "__main__":
    get_indicator_number_list_and_time_period__listfrom_uat()
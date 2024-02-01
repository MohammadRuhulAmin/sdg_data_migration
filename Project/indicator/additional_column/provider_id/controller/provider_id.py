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
                    print(temp)
            except Exception as E:
                continue
    except Exception as E:
        print(str(E))



if __name__ == "__main__":
    get_indicator_number_list_and_time_period__listfrom_uat()
import Project.connection as mysql_connection
import Project.indicator.additional_column.provider_id.query as tpq

def get_indicator_number_list_and_time_period__listfrom_uat():
    try:
        cursor_uat = mysql_connection.mydb_connection_destinationdb.cursor()
        cursor_uat.execute()
    except Exception as E:
        print(str(E))
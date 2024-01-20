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
mydb_connection_exist_uat = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="root",
    password="ruhulamin"
)



def existing_sdg_time_periods_to_uat_data_period():
    try:
        cursor_sdg_time_periods = mydb_connection_exist_uat.cursor()
        query_mapped_data = """
        SELECT temp.old_name
        # ,temp.new_name
        FROM (
            SELECT 
                o.name COLLATE utf8_unicode_ci old_name,
                n.name COLLATE utf8_unicode_ci new_name
            FROM 
                sdg_v1_v2_live.sdg_time_periods o
            LEFT JOIN 
                uat_sdg_tracker_clone.data_period n ON n.name COLLATE utf8_unicode_ci = o.name COLLATE utf8_unicode_ci
            ORDER BY 
                o.name
        )temp
        WHERE temp.new_name IS NULL ;
        """
        cursor_sdg_time_periods.execute(query_mapped_data)
        time_periods = cursor_sdg_time_periods.fetchall()
        for name in time_periods:
            print(name[0])

    except Exception as E:
        print(str(E))


if __name__ == "__main__":
    existing_sdg_time_periods_to_uat_data_period()
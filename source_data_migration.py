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

def connection_data():
    cursor_src = mydb_connection_sourcedb.cursor()
    cursor_dest = mydb_connection_destinationdb.cursor()
    try:
        query_src = """SELECT * FROM sdg_v1_v2_live.mapped_sources;"""
        cursor_src.execute(query_src)
        data = cursor_src.fetchall()
        for row in data:
            new_source_name = row[4]
            query_dest = """SELECT * FROM uat_sdg_tracker_clone.ind_sources WHERE name = %s"""
            cursor_dest.execute(query_dest,(new_source_name,))
            result = cursor_dest.fetchone()
            if result is None:
                insert_query_name = """INSERT INTO uat_sdg_tracker_clone.ind_sources 
                (ministry_id,ministry_division_id,office_agency_id,survey_id,name)
                VALUES(0,0,0,0,%s)"""
                cursor_dest.execute(insert_query_name,(new_source_name,))
                mydb_connection_destinationdb.commit()
                print(new_source_name)
            else:
                continue

    except Exception as E:
        print(str(E))
    finally:
        cursor_src.close()
        cursor_dest.close()



# Call the function to fetch data
connection_data()

# Close the database connection when done
mydb_connection_sourcedb.close()

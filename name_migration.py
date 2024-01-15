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


def insert_source_name():
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
                print(new_source_name,"-> already Exist")
                continue

    except Exception as E:
        print(str(E))
    finally:
        cursor_src.close()
        cursor_dest.close()

def string_modification(position_index):
    cursor_dest = mydb_connection_destinationdb.cursor()
    modified_string_list = []
    try:
        get_name_query = """SELECT name as process_name FROM uat_sdg_tracker_clone.ind_sources ;"""
        cursor_dest.execute(get_name_query)
        data = cursor_dest.fetchall()
        for row in data:
            try:
                temp_row = row[0].split(',')
                ministry_name = temp_row[position_index]
                modify_ministry_name = ministry_name.replace(" ", "").lower()
                modified_string_list.append(modify_ministry_name)
            except Exception as E:
                continue
        return modified_string_list
    except Exception as E:
        print(str(E))
def insert_ministry_id():
    print(string_modification(3))




if __name__ == "__main__":
    #insert_source_name()
    insert_ministry_id()
    mydb_connection_sourcedb.close()
    mydb_connection_destinationdb.close()
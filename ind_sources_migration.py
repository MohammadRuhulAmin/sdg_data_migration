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
                print("Data inserted")
            else:
                print("Data already exist")
                continue

    except Exception as E:
        print(str(E))
    finally:
        cursor_src.close()
        cursor_dest.close()

def string_modification(name_string,index_position):
    temp_data = name_string.split(',')
    name = temp_data[index_position].replace(" ", "").lower()
    return name

def row_modification(position_index):
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
                modified_string_list.append([ministry_name,modify_ministry_name])
            except Exception as E:
                continue
        return modified_string_list
    except Exception as E:
        print(str(E))

def update_survey_id():
    cursor_survey_id = mydb_connection_destinationdb.cursor()
    try:
        query_get_survy_info = """SELECT id,name FROM ind_data_source_survey;"""
        cursor_survey_id.execute(query_get_survy_info)
        survey_data =cursor_survey_id.fetchall()
        for data in survey_data:
            id = data[0]
            name = data[1]
            query_getting_ids = """SELECT GROUP_CONCAT(id) AS id_list FROM ind_sources 
            WHERE `name` LIKE  %s;"""
            cursor_survey_id.execute(query_getting_ids,(f"{name}%",))
            list_id = cursor_survey_id.fetchall()[0][0]
            query_update_survay_id = """UPDATE ind_sources SET survey_id = %s
            WHERE FIND_IN_SET(id,%s)"""
            cursor_survey_id.execute(query_update_survay_id, ((id, list_id,)))
            mydb_connection_destinationdb.commit()
        print("Survey Data updated Successfully!")
    except Exception as E:
        print(str(E))


def update_office_agencies_id():
    cursor_office_agencies = mydb_connection_destinationdb.cursor()
    try:
        query_get_office_agencies_info = """SELECT id,ministry_id,division_id,name FROM office_agencies WHERE 
        division_id != 0 """
        cursor_office_agencies.execute(query_get_office_agencies_info)
        office_agencies_data = cursor_office_agencies.fetchall()
        for agency_data in office_agencies_data:
            id = agency_data[0]
            ministry_id = agency_data[1]
            division_id = agency_data[2]
            name = agency_data[3]
            query_getting_ids = """
            SELECT GROUP_CONCAT(id) as id_list FROM ind_sources
            WHERE ministry_id = %s and ministry_division_id = %s
            AND name LIKE %s
            """
            cursor_office_agencies.execute(query_getting_ids,(ministry_id,division_id,f"%{name}%",))
            list_id = cursor_office_agencies.fetchall()[0][0]
            query_update_office_agency_id = """
            UPDATE ind_sources SET office_agency_id = %s
            WHERE FIND_IN_SET(id,%s)
            """
            cursor_office_agencies.execute(query_update_office_agency_id,((id,list_id,)))
            mydb_connection_destinationdb.commit()
        print("office_agency_id data updated successfully!")
    except Exception as E:
        print(str(E))
def update_ministry_division_id():

    try:
        cursor_ministry_division_id = mydb_connection_destinationdb.cursor()
        query_get_ministry_division_info = """SELECT id,ministry_id,name FROM ministry_divisions;"""
        cursor_ministry_division_id.execute(query_get_ministry_division_info)
        ministry_division_id_data =  cursor_ministry_division_id.fetchall()

        for min_div_data in ministry_division_id_data:
            id = min_div_data[0]
            minstriy_id = min_div_data[1]
            name = min_div_data[2]
            print(id,minstriy_id,name)
            query_getting_ids = """
            SELECT GROUP_CONCAT(id) AS id_list FROM ind_sources
            WHERE ministry_id = %s AND name LIKE %s;
            """
            cursor_ministry_division_id.execute(query_getting_ids,(minstriy_id,f"%{name}%",))
            list_id = cursor_ministry_division_id.fetchall()[0][0]
            print(list_id)
            query_update_ministry_id = """UPDATE ind_sources SET ministry_division_id = %s
            WHERE FIND_IN_SET(id,%s)"""
            cursor_ministry_division_id.execute(query_update_ministry_id, ((id, list_id,)))
            mydb_connection_destinationdb.commit()
        print("ministry_division_id data updated successfully!")
    except Exception as E:
        print(str(E))

def update_ministry_id():
    cursor_ministry = mydb_connection_destinationdb.cursor()
    try:
        query_get_ministry_info = """SELECT id,name FROM ministries"""
        cursor_ministry.execute(query_get_ministry_info)
        ministry_data  = cursor_ministry.fetchall()
        for mindata in ministry_data:
            id = mindata[0]
            name = mindata[1]
            query_getting_ids = """SELECT GROUP_CONCAT(id) AS id_list FROM ind_sources 
            WHERE `name` LIKE  %s;"""
            cursor_ministry.execute(query_getting_ids, (f"%{name}%",))
            list_id = cursor_ministry.fetchall()[0][0]
            print(list_id)
            query_update_ministry_id = """UPDATE ind_sources SET ministry_id = %s
            WHERE FIND_IN_SET(id,%s)"""
            cursor_ministry.execute(query_update_ministry_id, ((id, list_id,)))
            mydb_connection_destinationdb.commit()

    except Exception as E:
        print(str(E))






if __name__ == "__main__":
    insert_source_name()
    update_ministry_id()
    update_ministry_division_id()
    update_office_agencies_id()
    update_survey_id()
    mydb_connection_sourcedb.close()
    mydb_connection_destinationdb.close()
import Project.connection as mysql_connection
import Project.sources.query.ministry as mq
import Project.sources.query.ministry_division as mdq
import Project.sources.query.office_agency as oaq
import Project.sources.query.survay as sq
import Project.sources.query.sources as sourcesqry
import Project.sources.query.ind_sources as inds
import Project.sources.query.mapped_sources as ms

def insert_source_name():
    cursor_src = mysql_connection.mydb_connection_sourcedb.cursor()
    cursor_dest = mysql_connection.mydb_connection_destinationdb.cursor()
    try:

        cursor_src.execute(sourcesqry.query_src)
        data = cursor_src.fetchall()
        for row in data:
            new_source_name = row[4]

            cursor_dest.execute(sourcesqry.query_dest,(new_source_name,))
            result = cursor_dest.fetchone()
            if result is None:
                cursor_dest.execute(sourcesqry.insert_query_name,(new_source_name,))
                mysql_connection.mydb_connection_destinationdb.commit()
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
    cursor_dest = mysql_connection.mydb_connection_destinationdb.cursor()
    modified_string_list = []
    try:
        cursor_dest.execute(inds.get_name_query)
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
    cursor_survey_id = mysql_connection.mydb_connection_destinationdb.cursor()
    try:

        cursor_survey_id.execute(sq.query_get_survy_info)
        survey_data =cursor_survey_id.fetchall()
        for data in survey_data:
            id = data[0]
            name = data[1]
            cursor_survey_id.execute(sq.query_getting_ids,(f"{name}%",))
            list_id = cursor_survey_id.fetchall()[0][0]
            cursor_survey_id.execute(sq.query_update_survay_id, ((id, list_id,)))
            mysql_connection.mydb_connection_destinationdb.commit()
        print("Survey Data updated Successfully!")
    except Exception as E:
        print(str(E))


def update_office_agencies_id():
    cursor_office_agencies = mysql_connection.mydb_connection_destinationdb.cursor()
    try:

        cursor_office_agencies.execute(oaq.query_get_office_agencies_info)
        office_agencies_data = cursor_office_agencies.fetchall()
        for agency_data in office_agencies_data:
            id = agency_data[0]
            ministry_id = agency_data[1]
            division_id = agency_data[2]
            name = agency_data[3]

            cursor_office_agencies.execute(oaq.query_getting_ids,(ministry_id,division_id,f"%{name}%",))
            list_id = cursor_office_agencies.fetchall()[0][0]

            cursor_office_agencies.execute(oaq.query_update_office_agency_id,((id,list_id,)))
            mysql_connection.mydb_connection_destinationdb.commit()
        print("office_agency_id data updated successfully!")
    except Exception as E:
        print(str(E))
def update_ministry_division_id():

    try:
        cursor_ministry_division_id = mysql_connection.mydb_connection_destinationdb.cursor()
        cursor_ministry_division_id.execute(mdq.query_get_ministry_division_info)
        ministry_division_id_data =  cursor_ministry_division_id.fetchall()
        for min_div_data in ministry_division_id_data:
            id = min_div_data[0]
            minstriy_id = min_div_data[1]
            name = min_div_data[2]
            print(id,minstriy_id,name)
            cursor_ministry_division_id.execute(mdq.query_getting_ids,(minstriy_id,f"%{name}%",))
            list_id = cursor_ministry_division_id.fetchall()[0][0]
            print(list_id)
            cursor_ministry_division_id.execute(mdq.query_update_ministry_id, ((id, list_id,)))
            mysql_connection.mydb_connection_destinationdb.commit()
        print("ministry_division_id data updated successfully!")
    except Exception as E:
        print(str(E))

def update_ministry_id():
    cursor_ministry = mysql_connection.mydb_connection_destinationdb.cursor()
    try:

        cursor_ministry.execute(mq.query_get_ministry_info)
        ministry_data  = cursor_ministry.fetchall()
        for mindata in ministry_data:
            id = mindata[0]
            name = mindata[1]
            cursor_ministry.execute(mq.query_getting_ids, (f"%{name}%",))
            list_id = cursor_ministry.fetchall()[0][0]
            print(list_id)
            cursor_ministry.execute(mq.query_update_ministry_id, ((id, list_id,)))
            mysql_connection.mydb_connection_destinationdb.commit()

    except Exception as E:
        print(str(E))

def mapped_sources():
    try:
        cursor_dest = mysql_connection.mydb_connection_destinationdb.cursor()
        cursor_dest.execute(ms.drop_mapped_sources)
        mysql_connection.mydb_connection_destinationdb.commit()
        print("mapped_table dropped if exist")
        cursor_dest.execute(ms.table_create_mapped_sources)
        mysql_connection.mydb_connection_destinationdb.commit()
        print("mapped_sources table created successfully!")

        cursor_dest.execute(ms.insert_mapped_sources)
        mysql_connection.mydb_connection_destinationdb.commit()
        print("mapped_sources table inserted successfully!")

    except Exception as E:
        print(str(E))


def source_rapper():
    mapped_sources()
    insert_source_name()
    update_ministry_id()
    update_ministry_division_id()
    update_office_agencies_id()
    update_survey_id()


if __name__ == "__main__":
    source_rapper()
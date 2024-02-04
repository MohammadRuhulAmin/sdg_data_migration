import Project.svg_map.resources.districts as dmp
import Project.svg_map.resources.svg_map as smp
import Project.connection as mysql_connection
import Project.svg_map.query.insert_query as iq

def country():
    try:
        cursor = mysql_connection.mydb_connection_sdg_map.cursor()
        cursor.execute(iq.country,(smp.svg_map['bangladesh'],))
        mysql_connection.mydb_connection_sdg_map.commit()
        print("Bangladesh Map Has been inserted!")
    except Exception as E:
        print(str(E))
def divisions():
    try:
        cursor = mysql_connection.mydb_connection_sdg_map.cursor()
        mapping_district = dmp.mapped_district
        bangladesh = mapping_district['bangladesh']
        for division in bangladesh:
            temp_div = division['region_id']
            cursor.execute(iq.divisions,(1,temp_div,smp.svg_map[temp_div]))
            mysql_connection.mydb_connection_sdg_map.commit()
            print(temp_div, "has been inserted Successfully!")
    except Exception as E:
        print(str(E))


if __name__ == "__main__":
    #country()
    divisions()
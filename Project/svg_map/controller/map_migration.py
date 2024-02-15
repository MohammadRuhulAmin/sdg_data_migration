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

def districts():
    try:
        cursor = mysql_connection.mydb_connection_sdg_map.cursor()
        bangladesh = dmp.mapped_district['bangladesh']
        divisions_list = dmp.mapped_district['dhakaDivision']
        country_id = 1
        for division in bangladesh:
            cursor.execute(iq.get_division_id,(division['region_id'],))
            division_id = cursor.fetchall()[0][0]
            region_id_div = division['region_id']
            for district_info in dmp.mapped_district[region_id_div]:
                print("-->",district_info['region_id'])
                district = district_info['region_id']
                temp_json = {
                    "country_id":country_id,
                    "division_id":division_id,
                    "district_name":district_info['region_id'],
                    "path":smp.svg_map[district]
                }
                temp_tpl = (temp_json['country_id'],temp_json['division_id'],temp_json['district_name'],temp_json['path'],)
                cursor.execute(iq.districts,temp_tpl)
                mysql_connection.mydb_connection_sdg_map.commit()
                print(temp_json ,"Inserted !")
    except Exception as E:
        print(str(E))

if __name__ == "__main__":
    country()
    divisions()
    districts()
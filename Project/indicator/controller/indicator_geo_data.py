from indicator_data import ind_data_values
import Project.connection as mysql_connection
import Project.indicator.query.geo_data_info as gdi
def indicator_geo_data(temp_json):
    try:
        cursor_dest = mysql_connection.mydb_connection_destinationdb.cursor()
        ind_data_id = ind_data_values['ind_data_id']
        type = None
        bbs_code = None
        data_value = temp_json.get('data_value')
        geo_division_id_existing, geo_division_name, geo_district_id_existing, geo_district_name, geo_upazila_id_existing, geo_upazila_name = (
            temp_json.get(key) for key in ['geo_division_id', 'geo_division_name', 'geo_district_id', 'geo_district_name', 'geo_upazila_id','geo_upazila_name']
        )

        if geo_upazila_id_existing !=0 and geo_district_id_existing!=0 and geo_division_id_existing  !=0 :
            type     = 3
            upazila  = geo_upazila_name
            district = geo_district_name
            division = geo_division_name
            cursor_dest.execute(gdi.get_upazila_bbs_code,(f"%{division}%",f"%{district}%",f"%{upazila}%",))
            bbs_code = cursor_dest.fetchall()[0][5]

        if geo_upazila_id_existing == 0 and geo_district_id_existing !=0 and geo_division_id_existing !=0:
            type  = 2
            district = geo_district_name
            division = geo_division_name
            cursor_dest.execute(gdi.get_district_bbs_code,(f"%{division}%",f"%{district}%"))
            district_bbs_code = cursor_dest.fetchall()
            if district_bbs_code[0][0] and district_bbs_code[0][1] and district_bbs_code[0][2] and district_bbs_code[0][3]:bbs_code = district_bbs_code[0][3]
        if geo_upazila_id_existing == 0 and geo_district_id_existing == 0 and geo_division_id_existing !=0:
            type = 1
            cursor_dest.execute(gdi.get_division_bbs_code,(f"%{geo_division_name}%",))
            division_bbs_code = cursor_dest.fetchall()
            if division_bbs_code[0] and division_bbs_code[0][0]:bbs_code = division_bbs_code[0][0]

        cursor_dest.execute(gdi.insert_in_indicator_geo_data,(ind_data_id,type,bbs_code,data_value,))
        mysql_connection.mydb_connection_destinationdb.commit()
        temp = {
            "ind_data_id":ind_data_id,"type":type,"bbs_code":bbs_code,"data_value":data_value
        }



    except Exception as E:
        print(temp_json)
        print(str(E))
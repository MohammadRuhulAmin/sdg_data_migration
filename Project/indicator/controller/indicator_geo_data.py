from indicator_data import ind_data_values
import Project.connection as mysql_connection
import Project.indicator.query.geo_data_info as gdi
def indicator_geo_data(temp_json):
    try:
        ind_data_id = ind_data_values['ind_data_id']
        type = None
        bbs_code = None
        data_value = temp_json.get('data_value')
        geo_division_id, geo_division_name, geo_district_id, geo_district_name, geo_upazila_id, geo_upazila_name = (
            temp_json.get(key) for key in ['geo_division_id', 'geo_division_name', 'geo_district_id', 'geo_district_name', 'geo_upazila_id','geo_upazila_name']
        )

        if geo_upazila_id !=0 and geo_district_id!=0 and geo_division_id!=0 :
            type   = 3

        if geo_upazila_id == 0 and geo_district_id !=0 and geo_division_id!=0:
            type  = 2
        if geo_upazila_id == 0 and geo_district_id == 0 and geo_division_id!=0:
            type = 1



    except Exception as E:
        print(str(E))
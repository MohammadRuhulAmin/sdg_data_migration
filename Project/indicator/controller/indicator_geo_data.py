from indicator_data import ind_data_values

def indicator_geo_data(temp_json):
    try:
        print(temp_json)
        type = None
        ind_data_id = ind_data_values['ind_data_id']
        geo_division_id, geo_division_name, geo_district_id, geo_district_name, geo_upazila_id, geo_upazila_name = (
            temp_json.get(key) for key in ['geo_division_id', 'geo_division_name', 'geo_district_id', 'geo_district_name', 'geo_upazila_id','geo_upazila_name']
        )

        if geo_upazila_id !=0 and geo_district_id!=0 and geo_division_id!=0 :type   = 3
        if geo_upazila_id == 0 and geo_district_id !=0 and geo_division_id!=0:type  = 2
        if geo_upazila_id == 0 and geo_district_id == 0 and geo_division_id!=0:type = 1
        print(type,geo_division_name,geo_district_name,geo_upazila_name)

    except Exception as E:
        print(str(E))
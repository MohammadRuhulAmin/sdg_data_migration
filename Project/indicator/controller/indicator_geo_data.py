from indicator_data import ind_data_values

def indicator_geo_data(temp_json):
    try:
        type = None
        ind_data_id = ind_data_values['ind_data_id']
        geo_division_id = temp_json.get('geo_division_id')
        geo_district_id = temp_json.get('geo_district_id')
        geo_upazila_id  = temp_json.get('geo_upazila_id')
        if geo_upazila_id !=0 and geo_district_id!=0 and geo_division_id!=0 :type   = 3
        if geo_upazila_id == 0 and geo_district_id !=0 and geo_division_id!=0:type  = 2
        if geo_upazila_id == 0 and geo_district_id == 0 and geo_division_id!=0:type = 1


    except Exception as E:
        print(str(E))
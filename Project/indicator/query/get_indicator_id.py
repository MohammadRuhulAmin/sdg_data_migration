get_indicator_id = """
SELECT sid.indicator_id FROM sdg_indicator_details sid 
LEFT JOIN sdg_indicators si ON si.id = sid.indicator_id
WHERE sid.indicator_number  = %s AND sid.language_id = 1
AND si.is_npt_thirty_nine = 0 AND si.is_plus_one = 0;
"""

insert_indicator_dis_1 = """
INSERT INTO indicator_data (ind_id,ind_def_id,source_id, data_period, data_value,status,created_at,updated_at) 
VALUES ( %s,%s, %s, %s,%s,%s,%s,%s);
"""


insert_in_disagg_data = """
INSERT INTO indicator_disagg_data(ind_data_id,disagg_id,disagg_name,data_value,created_at,updated_at)
VALUES(%s,%s,%s,%s,%s,%s)
"""
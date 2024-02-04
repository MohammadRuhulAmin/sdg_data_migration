
mapped_query = """
SELECT DISTINCT temp1.disagg_name, temp1.ind_id,temp1.ind_def_id,temp2.type_id disagg_type_id,temp1.disagg_id,temp1.disagg_name
FROM (SELECT ind.ind_id,ind.ind_def_id,idd.disagg_id,idd.disagg_name
FROM sdg_indicator_details sid
LEFT JOIN indicator_data ind ON ind.ind_id = sid.indicator_id
LEFT JOIN indicator_disagg_data idd ON idd.ind_data_id = ind.id
WHERE sid.indicator_number = %s AND sid.language_id = 1
AND disagg_name IS NOT NULL) temp1
LEFT JOIN (SELECT dn.type_id,dn.name FROM disaggregation_name dn)temp2 
ON temp1.disagg_name = temp2.name;
"""



indicator_number_list = """
SELECT DISTINCT indicator_number FROM sdg_indicator_details 
WHERE 
#indicator_number = "1.1.1" and 
language_id = 1 and indicator_number <> "";
"""


insert_into_ind_def_disagg = """
INSERT INTO ind_def_disagg(ind_id,ind_def_id,disagg_type_id,disagg_id,disagg_name) VALUES(%s,%s,%s,%s,%s);
"""
get_specific_user_info_by_time_period = """
    SELECT DISTINCT stp.name time_period, sil.serial_no,spl.username
    #sid.indicator_id,sidc.indicator_data_id,sidc.provider_id
    FROM `sdg_indicator_langs` sil
    LEFT JOIN sdg_indicator_data sid ON sid.indicator_id = sil.indicator_id 
    LEFT JOIN sdg_indicator_data_children sidc ON sidc.indicator_data_id = sid.id AND sidc.is_location = 0
    LEFT JOIN sdg_time_periods stp ON stp.id = sid.time_period_id
    LEFT JOIN sdg_provider_logs spl ON spl.sdg_provider_id = sidc.provider_id
    WHERE sil.serial_no = %s AND sid.status = 3 AND stp.name = %s
    AND spl.status = 3;
"""


indicator_wise_time_period_list = """
    SELECT DISTINCT * FROM (SELECT sid.indicator_number,sid.indicator_id,ind.data_period 
    FROM sdg_indicator_details sid
    LEFT JOIN indicator_data ind ON sid.indicator_id = ind.ind_id
    WHERE sid.language_id = 1 ORDER BY sid.indicator_number)temp
"""
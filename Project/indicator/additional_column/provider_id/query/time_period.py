get_specific_user_info_by_time_period = """
    select temp1.serial_no,temp1.data_period,temp1.provider_id,temp2.username existing_user,
    u.username uat_user,u.id uat_provider_id,
    sid.indicator_number,sid.indicator_id
    from(SELECT sil.serial_no,sidc.sdg_disaggregation_id,sidc.provider_id,stp.name data_period
    #,spl.username
    FROM sdg_v1_v2_live.sdg_indicator_langs sil
    LEFT JOIN sdg_v1_v2_live.sdg_indicator_data sid ON sid.indicator_id = sil.indicator_id
    LEFT JOIN sdg_v1_v2_live.sdg_indicator_data_children sidc ON sidc.indicator_data_id = sid.id AND sidc.is_location = 0
    LEFT JOIN sdg_v1_v2_live.sdg_time_periods stp ON stp.id = sid.time_period_id
    #LEFT JOIN sdg_provider_logs spl ON spl.sdg_provider_id = sidc.provider_id
    LEFT JOIN sdg_v1_v2_live.sdg_disaggregation_langs sdl ON sdl.disaggregation_id = sidc.sdg_disaggregation_id
    WHERE sil.serial_no = "1.3.1" AND sil.language_id = 1 AND sid.status = 3 AND sid.publish IN(4,5)
    and sidc.sdg_disaggregation_id = 1)temp1
    left join(select distinct sdg_provider_id,username from sdg_v1_v2_live.sdg_provider_logs where status=3)temp2
    on temp2.sdg_provider_id = temp1.provider_id
    left join uat_sdg_tracker_clone.users u on u.username = temp2.username 
    left join uat_sdg_tracker_clone.sdg_indicator_details sid 
    on sid.indicator_number  COLLATE utf8_general_ci = temp1.serial_no  COLLATE utf8_general_ci and sid.language_id = 1;
"""

indicator_wise_time_period_list = """
    select distinct sil.indicator_number from sdg_indicator_details sil
    where sil.language_id = 1 and sil.indicator_number  != ""
    order by sil.indicator_number;
"""
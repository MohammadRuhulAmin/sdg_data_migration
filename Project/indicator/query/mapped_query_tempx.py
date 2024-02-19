query="""
        SELECT tmp.serial_no,tmp.sdg_disaggregation_id,tmp.value,tmp.data_period,tmp.name,tmp.source_id,
        tmp2.type_name,tmp.status,tmp.publish,tmp.created,tmp.modified,tmp.is_location,
        tmp.geo_division_id,tmp.division_name_eng,
        tmp.geo_district_id,tmp.district_name_eng,tmp.geo_upazila_id,tmp.upazila_name_eng
        FROM (SELECT sil.serial_no,sidc.sdg_disaggregation_id,sidc.value,stp.name data_period,sdl.name,sid.source_id,sidc.is_location,
        sidc.status,sidc.publish,sid.created,sid.modified,sidc.geo_division_id,gdiv.division_name_eng,sidc.geo_district_id,
        gdist.district_name_eng,
        sidc.geo_upazila_id,gupazila.upazila_name_eng
        FROM sdg_indicator_langs sil
        LEFT JOIN sdg_indicator_data sid ON sid.indicator_id = sil.indicator_id
        LEFT JOIN sdg_indicator_data_children sidc ON sidc.indicator_data_id = sid.id 
        left join geo_divisions gdiv on gdiv.id = sidc.geo_division_id
        left join geo_districts gdist on gdist.id = sidc.geo_district_id
        left join geo_upazilas gupazila on gupazila.id = sidc.geo_upazila_id
        LEFT JOIN sdg_time_periods stp ON stp.id = sid.time_period_id
        LEFT JOIN sdg_disaggregation_langs sdl ON sdl.disaggregation_id = sidc.sdg_disaggregation_id
        WHERE sil.serial_no = %s AND sil.language_id = 1 AND sid.status = 3 AND sid.publish IN(4,5))tmp
        LEFT JOIN(SELECT child.disaggregation_id disaggregation_id,child.language_id,child.parent_id,
        child.name disaggregation_name,parent.name type_name,parent.disaggregation_id type_id 
        FROM (SELECT id,disaggregation_id,language_id,parent_id,NAME
        FROM sdg_disaggregation_langs WHERE parent_id = 0)parent
        LEFT JOIN (SELECT id,disaggregation_id,language_id,parent_id,NAME
        FROM sdg_disaggregation_langs WHERE parent_id > 0)child
        ON parent.disaggregation_id = child.parent_id)tmp2 
        ON tmp.sdg_disaggregation_id=tmp2.disaggregation_id;
"""
query = """
           SELECT tmp.*,tmp2.type_name
        -- tmp2.disaggregation_name,
            FROM (SELECT sil.serial_no,
            sidc.sdg_disaggregation_id,
            sidc.value,stp.name data_period,
            sdl.name,sid.source_id
            FROM sdg_indicator_langs sil
            LEFT JOIN sdg_indicator_data sid ON sid.indicator_id = sil.indicator_id
            LEFT JOIN sdg_indicator_data_children sidc ON sidc.indicator_data_id = sid.id
            LEFT JOIN sdg_time_periods stp ON stp.id = sid.time_period_id
            LEFT JOIN sdg_disaggregation_langs sdl ON sdl.disaggregation_id = sidc.sdg_disaggregation_id
            WHERE sil.serial_no = %s AND sil.language_id = 1)tmp
            LEFT JOIN(SELECT child.disaggregation_id disaggregation_id,child.language_id,child.parent_id,
            child.name disaggregation_name,parent.name type_name,parent.disaggregation_id type_id FROM (SELECT id,disaggregation_id,language_id,parent_id,NAME
            FROM sdg_disaggregation_langs WHERE parent_id = 0)parent
            LEFT JOIN (SELECT id,disaggregation_id,language_id,parent_id,NAME
            FROM sdg_disaggregation_langs WHERE parent_id > 0)child
            ON parent.disaggregation_id = child.parent_id)tmp2 ON tmp.sdg_disaggregation_id=tmp2.disaggregation_id;
"""
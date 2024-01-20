        SELECT
            #sdg_indicator_langs sil
            sil.serial_no serial_no,
            #sdg_indicator_data ind_data
            ind_data.indicator_id indicator_id,
            ind_data.time_period_id time_period_id,
            #sdg_indicator_data_children sidc
            sidc.sdg_disaggregation_id disaggregation_id,
            sidc.value `value`,
            #sdg_time_periods tp
            tp.name,
            #sdg_disaggregation_langs sdl
            sdl.name dis_name
        FROM sdg_indicator_data ind_data
        LEFT JOIN sdg_indicator_data_children sidc ON sidc.id = ind_data.indicator_id
        LEFT JOIN sdg_time_periods tp ON tp.id = ind_data.time_period_id
        LEFT JOIN sdg_indicator_langs sil ON sil.indicator_id = ind_data.indicator_id
        LEFT JOIN sdg_disaggregation_langs sdl ON sdl.disaggregation_id = sidc.sdg_disaggregation_id
	WHERE sil.serial_no = "1.1.1" AND sil.language_id=1
	GROUP BY ind_data.indicator_id,ind_data.time_period_id,
        sidc.sdg_disaggregation_id,
        sidc.value,tp.name,sdl.name
        ORDER BY sil.serial_no,ind_data.indicator_id,ind_data.time_period_id,
        sidc.sdg_disaggregation_id,
        sidc.value,tp.name,sdl.name;
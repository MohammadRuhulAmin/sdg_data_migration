SELECT * FROM ministries WHERE LOWER(REPLACE(`name`, ' ', '')) = 'ministryofland';



SELECT
	#sdg_indicator_data
	ind_data.publish,
	ind_data.status,
	ind_data.time_period_id,
	#sdg_indocator_data_children
	ind_data_c.value `Value`,
	#sdg_time_periods
	t.name TimePeriod,
	#sdg_indicator_langs
	ind.serial_no serial_no,
	#sdg_arrange_source_langs
	s.name SourceName,
	#sdg_disaggrigation_langs
	dis.name DisaggregationName
FROM sdg_indicator_data ind_data
LEFT JOIN sdg_indicator_data_children ind_data_c ON ind_data_c.indicator_data_id = ind_data.id
LEFT JOIN sdg_time_periods t ON t.id = ind_data.time_period_id
LEFT JOIN sdg_indicator_langs ind ON ind.id = ind_data.indicator_id
LEFT JOIN sdg_arrange_source_langs s ON s.source_id = ind_data.source_id
LEFT JOIN sdg_disaggregation_langs dis ON dis.disaggregation_id = ind_data_c.sdg_disaggregation_id
WHERE ind.serial_no = "1.1.1"
AND ind.language_id = 1
AND s.language_id = 1
GROUP BY
    ind_data.publish,
    ind_data.status,
    ind_data.time_period_id,
    ind_data_c.value,
    t.name,
    ind.serial_no,
    s.name,
    dis.name;


# mapped query
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
	tp.name
FROM sdg_indicator_data ind_data
LEFT JOIN sdg_indicator_data_children sidc ON sidc.id = ind_data.indicator_id
LEFT JOIN sdg_time_periods tp ON tp.id = ind_data.time_period_id
LEFT JOIN sdg_indicator_langs sil ON sil.indicator_id = ind_data.indicator_id;


# data_period column migration from ext.sdg_time_periods to uat.data_period

SELECT temp.old_name
        # ,temp.new_name
        FROM (
            SELECT
                o.name COLLATE utf8_unicode_ci old_name,
                n.name COLLATE utf8_unicode_ci new_name
            FROM
                sdg_v1_v2_live.sdg_time_periods o
            LEFT JOIN
                uat_sdg_tracker_clone.data_period n ON n.name COLLATE utf8_unicode_ci = o.name COLLATE utf8_unicode_ci
            ORDER BY
                o.name
        )temp
        WHERE temp.new_name IS NULL ;


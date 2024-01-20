##STEP1:  
	Get Necessary Data From Existing Database. The necessary parameters are:
	sdg_inidcator_langs.serial_no, sdg_indicator_data.indicator_id,
	sdg_inidcator_data.time_period_id,sdg_indicator_data_children.sdg_disaggregation_id,
	sdg_indicator_data_children_id.value,sdg_time_periods.name,sdg_disaggregation_langs.name
------------------------------------------------------------------------------------------------
##STEP2:  
	getting indicator_id using sdg_indicator_details.indicator_no by matching sdg_inidcator_langs.serial_no
------------------------------------------------------------------------------------------------

##STEP3:  
	If exisiting.sdg_indicator_data_children_id.value == 1 Then data_period and data_value will be inserted in uat.indicator_data where id = sdg_indicator_details.indicator_id
	Else data will be inserted in  uat.indicator_disagg_data.(In uat.indicator_disagg_data table we will insert ind_data_id,disagg_id,disagg_name and data_value).
	Here 
	ind_data_id = sdg_indicator_details.indicator_id
	disagg_id = uat.disaggregation_name.id
	disagg_name = uat.disaggregation_name.name
--------------------------------------------------------------------------------------------------
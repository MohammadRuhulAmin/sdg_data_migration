
mapped_query = """
select temp1.ind_id,temp1.ind_def_id,temp2.type_id disagg_type_id,temp1.disagg_id,temp1.disagg_name
from (select 
	#sdg_indicator_details sid
	#sid.indicator_number,
	#indicator_data ind
	ind.ind_id,ind.ind_def_id,
	#ind.id row_id,
	#indicator_disagg_data idd
	idd.disagg_id,idd.disagg_name
from sdg_indicator_details sid
left join indicator_data ind on ind.ind_id = sid.indicator_id
left join indicator_disagg_data idd on idd.ind_data_id = ind.id
where sid.indicator_number = "1.1.1" and sid.language_id = 1
and disagg_name is not null) temp1
left join (select dn.type_id,dn.name from disaggregation_name dn)temp2 
on temp1.disagg_name = temp2.name;
"""
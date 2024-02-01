user_mapping_query = """
SELECT result.username,result.serial_no,result.indicator_id,result.provider_id,
result.user_category,result.user_type,result.name,result.email,result.contact_no
FROM (SELECT  temp2.username,temp1.*,temp2.sdg_provider_id,temp2.user_category,
SUBSTRING(temp2.user_type, 1, LENGTH(temp2.user_type)-1)user_type,
temp2.name,temp2.email,temp2.contact_no
FROM (SELECT sil.serial_no,sidp.indicator_id,sidp.provider_id FROM sdg_indic_def_providers sidp
LEFT JOIN sdg_indicator_langs sil ON sil.indicator_id = sidp.indicator_id AND sil.language_id = 1
WHERE sil.serial_no = %s)temp1
LEFT JOIN(SELECT DISTINCT spl.sdg_provider_id,spl.user_category,spl.user_type,
spl.username,spl.name,spl.email,spl.contact_no FROM sdg_provider_logs spl 
)temp2 ON temp1.provider_id = temp2.sdg_provider_id)result
"""

indicator_list_query = """
        select sil.serial_no from sdg_indicator_langs sil
        where sil.language_id = 1
        -- and sil.serial_no = "15.6.1"
        group by sil.serial_no
        order by sil.serial_no;
"""
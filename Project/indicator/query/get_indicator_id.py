get_indicator_id = """
SELECT sid.id FROM sdg_indicator_details sid 
LEFT JOIN sdg_indicators si ON si.id = sid.indicator_id
WHERE sid.indicator_number  = %s AND sid.language_id = 1
AND si.is_npt_thirty_nine = 0 AND si.is_plus_one = 0;
"""
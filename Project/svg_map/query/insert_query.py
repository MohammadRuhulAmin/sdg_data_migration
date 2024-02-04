
country = """
INSERT INTO country(path)VALUES(%s);
"""

divisions = """
INSERT INTO divisions(country_id,division_name,path)VALUES(%s,%s,%s);
"""

districts = """
INSERT INTO districts(country_id,division_id,district_name,path)VALUES(%s,%s,%s,%s);
"""



get_division_id = """
SELECT id FROM divisions WHERE division_name = %s;
"""


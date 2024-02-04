
country = """
INSERT INTO country(path)VALUES(%s);
"""

divisions = """
INSERT INTO divisions(country_id,division_name,path)VALUES(%s,%s,%s);
"""
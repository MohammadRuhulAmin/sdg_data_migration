query = """SELECT name FROM disaggregation_name WHERE name LIKE %s;"""

query_search_type = """
SELECT id,name FROM disaggregation_type WHERE name LIKE %s
"""


qy_dnm = """
INSERT INTO disaggregation_name(type_id,name) VALUES(%s,%s)
"""

insert_type_name = """
INSERT INTO disaggregation_type(name)VALUES(%s)
"""

qy_dnmx = """
INSERT INTO disaggregation_name(type_id,name) VALUES(%s,%s)
"""
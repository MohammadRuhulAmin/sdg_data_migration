import mysql.connector
mydb_connection_sourcedb = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="root",
    password="ruhulamin",
    database="sdg_v1_v2_live"
)

mydb_connection_destinationdb = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="root",
    password="ruhulamin",
    database="uat_sdg_tracker_clone"
)

mydb_connection = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="root",
    password="ruhulamin"
)

# mydb_connection_sdg_map= mysql.connector.connect(
#     host="localhost",
#     port=3306,
#     user="root",
#     password="ruhulamin",
#     database="sdg_svg_map"
# )
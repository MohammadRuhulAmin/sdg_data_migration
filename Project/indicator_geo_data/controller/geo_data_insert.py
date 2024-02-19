import Project.connection as mysql_connection
import Project.indicator_geo_data.query.mapped_query as mq
def get_geo_mapped_data():
    try:
        cursor_exesting = mysql_connection.mydb_connection_sourcedb.cursor()
        cursor_exesting.execute(mq.mapped_query)
        rows = cursor_exesting.fetchall()
        for row in rows:
            print(row)
    except Exception as E:
        print(str(E))



if __name__ == "__main__":
    get_geo_mapped_data()
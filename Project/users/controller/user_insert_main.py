import Project.connection as mysql_connection
import Project.users.controller.get_user_info as gui
import Project.users.query.user_mapped as umq

def get_indicator():
    cursor_srs = mysql_connection.mydb_connection_sourcedb.cursor()
    cursor_dest = mysql_connection.mydb_connection_destinationdb.cursor()
    try:
        cursor_srs.execute(umq.indicator_list_query)
        indicator_list = cursor_srs.fetchall()
        for indicator in indicator_list:
            indicator = indicator[0]
            cursor_srs.execute(umq.user_mapping_query,(indicator,))
            provider_info_list = cursor_srs.fetchall()
            for provider in provider_info_list:
                username,_,_,_,user_category,user_type,name_en,email,contact_no = provider
                gui.get_necessary_information_for_users_table(username, user_category, user_type, name_en, email, contact_no)
    except Exception as E:
        print(str(E))

if __name__ == "__main__":
    get_indicator()
import Project.connection as mysql_connection

def alter_users():
    try:
        alter_user_ddl = """
        ALTER TABLE  users Modify username varchar(255) NULL,
        Modify user_type varchar(12) NULL,
        Modify user_role_id int(11) NULL,
        Modify additional_role_ids varchar(26) NULL;
        """
        cursor_dest = mysql_connection.mydb_connection_destinationdb.cursor()
        cursor_dest.execute(alter_user_ddl)
        mysql_connection.mydb_connection_destinationdb.commit()
        print("users table has been altered for data migration")


    except Exception as E:
        print(str(E))


def alter_user_role():
    try:
        alter_user_role_ddl = """
        ALTER TABLE  user_role Modify id int(11) auto_increment,
        Modify role_name varchar(150)  NULL,
        Modify status tinyint(4)  NULL,
        Modify created_by datetime  NULL;
        """
        cursor_dest = mysql_connection.mydb_connection_destinationdb.cursor()
        cursor_dest.execute(alter_user_role_ddl)
        mysql_connection.mydb_connection_destinationdb.commit()
        print("user_role table has been altered for data migration")
    except Exception as E:
        print(str(E))

def alter_user_type():
    try:
        alter_user_type_ddl = """
        alter table user_types Modify id int(10) auto_increment,
        Modify type_name varchar(255)  NULL,
        Modify is_registrable int(11) NULL,
        Modify security_profile_id int(11) NULL,
        Modify auth_token_type enum('optional','mandatory') NULL,
        Modify is_default tinyint(1) NULL,
        Modify status enum('active','inactive') NULL,
        Modify created_by int(11) NULL,
        Modify updated_by int(11) NULL
        """
        cursor_dest = mysql_connection.mydb_connection_destinationdb.cursor()
        cursor_dest.execute(alter_user_type_ddl)
        mysql_connection.mydb_connection_destinationdb.commit()
        print("user_type table has been altered for data migration")
    except Exception as E:
        print(str(E))

def combine_user_alfter():
    alter_users()
    alter_user_role()
    alter_user_type()

if __name__ == "__main__":
    combine_user_alfter()
import Project.connection as mysql_connection

def alter_users():
    try:
        alter_user_ddl = """
        ALTER TABLE  users Modify username varchar(255) not NULL,
        Modify user_type varchar(12) not NULL,
        Modify user_role_id int(11) not NULL,
        Modify additional_role_ids varchar(26) not NULL;
        """
        cursor_dest = mysql_connection.mydb_connection_destinationdb.cursor()
        cursor_dest.execute(alter_user_ddl)
        mysql_connection.mydb_connection_destinationdb.commit()


    except Exception as E:
        print(str(E))

def alter_user_role():
    try:
        alter_user_role_ddl = """
        ALTER TABLE  user_role Modify id int(11) ,
        Modify role_name varchar(150) not NULL,
        Modify status tinyint(4) not NULL,
        Modify created_by datetime not NULL;
        """
        cursor_dest = mysql_connection.mydb_connection_destinationdb.cursor()
        cursor_dest.execute(alter_user_role_ddl)
        mysql_connection.mydb_connection_destinationdb.commit()
    except Exception as E:
        print(str(E))


def alter_user_type():
    try:
        alter_user_type_ddl = """
        alter table user_types Modify id varchar(255),
        Modify type_name varchar(255) not NULL,
        Modify is_registrable int(11) not NULL,
        Modify security_profile_id int(11) not NULL,
        Modify auth_token_type enum('optional','mandatory') not NULL,
        Modify is_default tinyint(1) not NULL,
        Modify status enum('active','inactive') not NULL,
        Modify created_by int(11) not NULL,
        Modify updated_by int(11) not NULL
        """
        cursor_dest = mysql_connection.mydb_connection_destinationdb.cursor()
        cursor_dest.execute(alter_user_type_ddl)
        mysql_connection.mydb_connection_destinationdb.commit()
        print("user_type table has been altered for data migration")
    except Exception as E:
        print(str(E))


if __name__ == "__main__":
    alter_users()
    alter_user_role()
    alter_user_type()
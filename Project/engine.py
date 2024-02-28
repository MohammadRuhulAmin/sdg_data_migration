
import Project.truncate_data.truncate_tables as tt
import Project.indicator.controller.indicator_main_tempx as imt
import Project.sources.controller.source_main as sm
import Project.users.controller.user_insert_main as uim
import Project.users.controller.user_role as ur
import Project.users.controller.user_types as ut

def engine():
    try:

        # step1: alter all the necessary tables

        # step2: truncate all the tables
        tt.truncate_rapper()

        # step3: map and insert ind_sources table
        sm.source_rapper()

        # step4: users, user_role, user_type table migrate
        uim.get_indicator()
        ur.get_all_user_role()
        ut.new_user_type()

        imt.indicator_rapper_main()
    except Exception as E:
        print(str(E))
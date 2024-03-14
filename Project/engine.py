import Project.truncate_data.truncate_tables as tt
import Project.sources.controller.source_main as sm
import Project.users.controller.user_insert_main as uim
import Project.users.controller.user_role as ur
import Project.users.controller.user_types as ut
import Project.create_alter_table.create_table.indicator_data.indicator_data_create as idc
import Project.create_alter_table.alter_operation.before_migration.disaggregation as d
import Project.create_alter_table.alter_operation.before_migration.users as bu
import Project.create_alter_table.alter_operation.before_migration.indicator_family as indf

def engine():
    try:
        # step1: alter all the necessary tables
        bu.combine_user_alfter()
        d.combine_disaggregation()
        indf.combine_indicator_family()
        # step2: truncate all the tables
        tt.truncate_rapper()
        # step3 : create indicator_data table
        idc.create_indicator_data()
        # step4: map and insert ind_sources table
        sm.source_rapper()
        # step5: users, user_role, user_type table migrate
        uim.get_indicator()
        ur.get_all_user_role()
        ut.new_user_type()
        # step6: indicator entry
        # finally run indicator from indicator main!

    except Exception as E:
        print(str(E))


if __name__ == "__main__":
    engine()
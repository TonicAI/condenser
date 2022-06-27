import uuid, sys
import config_reader, result_tabulator
import time
from subset import Subset
from psql_database_creator import PsqlDatabaseCreator
from mysql_database_creator import MySqlDatabaseCreator
from db_connect import DbConnect
from subset_utils import print_progress
import database_helper

def db_creator(db_type, source, dest):
    if db_type == 'postgres':
        return PsqlDatabaseCreator(source, dest, False)
    elif db_type == 'mysql':
        return MySqlDatabaseCreator(source, dest)
    else:
        raise ValueError('unknown db_type ' + db_type)


if __name__ == '__main__':
    if "--stdin" in sys.argv:
        config_reader.initialize(sys.stdin)
    else:
        config_reader.initialize()

    db_type = config_reader.get_db_type()
    source_dbc = DbConnect(db_type, config_reader.get_source_db_connection_info())
    destination_dbc = DbConnect(db_type, config_reader.get_destination_db_connection_info())

    database = db_creator(db_type, source_dbc, destination_dbc)
    database.teardown()
    database.create()

    # Get list of tables to operate on
    db_helper = database_helper.get_specific_helper()
    all_tables = db_helper.list_all_tables(source_dbc)
    all_tables = [x for x in all_tables if x not in config_reader.get_excluded_tables()]

    subsetter = Subset(source_dbc, destination_dbc, all_tables)

    try:
        subsetter.prep_temp_dbs()
        subsetter.run_middle_out()

        print("Beginning pre constraint SQL calls")
        start_time = time.time()
        for idx, sql in enumerate(config_reader.get_pre_constraint_sql()):
            print_progress(sql, idx+1, len(config_reader.get_pre_constraint_sql()))
            db_helper.run_query(sql, destination_dbc.get_db_connection())
        print("Completed pre constraint SQL calls in {}s".format(time.time()-start_time))
        

        print("Adding database constraints")
        if "--no-constraints" not in sys.argv:
            database.add_constraints()

        print("Beginning post subset SQL calls")
        start_time = time.time()
        for idx, sql in enumerate(config_reader.get_post_subset_sql()):
            print_progress(sql, idx+1, len(config_reader.get_post_subset_sql()))
            db_helper.run_query(sql, destination_dbc.get_db_connection())
        print("Completed post subset SQL calls in {}s".format(time.time()-start_time))

        result_tabulator.tabulate(source_dbc, destination_dbc, all_tables)
    finally:
        subsetter.unprep_temp_dbs()



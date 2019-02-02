import uuid, sys
import config_reader, result_tabulator
from subset import Subset
from database_creator import DatabaseCreator
from db_connect import DbConnect
from database_helper import list_all_tables

if __name__ == '__main__':
    if "--stdin" in sys.argv:
        config_reader.initialize(sys.stdin)
    else:
        config_reader.initialize()

    source_dbc = DbConnect(config_reader.get_source_db_connection_info())
    destination_dbc = DbConnect(config_reader.get_destination_db_connection_info())


    temp_schema = 'subset_' + str(uuid.uuid4()).replace('-','')

    database = DatabaseCreator(source_dbc, destination_dbc, temp_schema, False)
    database.teardown()
    database.create()
    database.validate_database_create()


    # Get list of tables to operate on
    all_tables = list_all_tables(source_dbc.get_db_connection())
    all_tables = [x for x in all_tables if x not in config_reader.get_excluded_tables()]

    s = Subset(source_dbc, destination_dbc, temp_schema, all_tables)
    s.run_middle_out()

    if "--no-constraints" not in sys.argv:
        database.add_constraints()
        database.validate_constraints()

    result_tabulator.tabulate(source_dbc, destination_dbc, all_tables)



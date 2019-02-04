import uuid
import result_tabulator, config_reader
from subset import Subset
from database_creator import DatabaseCreator
from db_connect import DbConnect
from subset_result_norm import SubsetResultNorm
from scipy.optimize import bisect
from database_helper import list_all_tables

def func(percent, lower_limit, lower_limit_norm, upper_limit, upper_limit_norm):

    if percent == lower_limit:
        return lower_limit_norm

    if percent == upper_limit:
        return upper_limit_norm

    return func_base(percent)

def func_base(percent):
    database = DatabaseCreator(source_dbc, destination_dbc, temp_schema, False)

    database.teardown()
    database.create()

    s = Subset(source_dbc, destination_dbc, temp_schema, all_tables)

    s.run_downward(percent)

    database.add_constraints()

    norm = SubsetResultNorm(source_dbc, destination_dbc).norm()

    print(percent, norm)
    return norm

def compute_fast_limits():
    desired_result = config_reader.get_target_percent()
    upper_limit_guess = desired_result

    last_result = desired_result
    lower_limit_guess = 0
    result = func_base(upper_limit_guess)
    while result > 0 and upper_limit_guess < 100:
        lower_limit_guess = upper_limit_guess
        upper_limit_guess *= 2
        last_result = result
        result = func_base(upper_limit_guess)

    return (lower_limit_guess, last_result, min(upper_limit_guess, 100), result)

if __name__ == '__main__':
    config_reader.initialize()

    source_dbc = DbConnect(config_reader.get_source_db_connection_info())
    destination_dbc = DbConnect(config_reader.get_destination_db_connection_info())
    temp_schema = 'subset_' + str(uuid.uuid4()).replace('-','')

    # Get list of tables to operate on
    all_tables = list_all_tables(source_dbc.get_db_connection())
    all_tables = [x for x in all_tables if x not in config_reader.get_excluded_tables()]


    lower_limit, lower_limit_norm, upper_limit, upper_limit_norm = compute_fast_limits()
    max_tries = config_reader.get_max_tries()

    try:
        bisect(func, lower_limit, upper_limit, maxiter=max_tries, args=(lower_limit, lower_limit_norm, upper_limit, upper_limit_norm))
    except:
        pass

    result_tabulator.tabulate(source_dbc, destination_dbc, all_tables)


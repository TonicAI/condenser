from subset import Subset
from database_creator import DatabaseCreator
from db_connect import DbConnect
from config_reader import ConfigReader
from subset_result_norm import SubsetResultNorm
from scipy.optimize import bisect
from result_tabulator import SubsetResultFunc

source_dbc = DbConnect('.source_db_connection_info')
destination_dbc = DbConnect('.destination_db_connection_info')
temp_schema = 'subset'
schema = 'public'

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
    database.validate_database_create()

    s = Subset(source_dbc, destination_dbc, percent, temp_schema)
    s.run_downward()

    database.add_constraints()
    database.validate_constraints()

    norm = SubsetResultNorm(source_dbc, destination_dbc, 'public').norm()

    print(percent, norm)
    return norm

def compute_fast_limits():
    desired_result = ConfigReader().get_desired_result()['percent']
    upper_limit_guess = desired_result

    last_result = desired_result
    lower_limit_guess = 0
    result = func_base(upper_limit_guess)
    while result > 0:
        lower_limit_guess = upper_limit_guess
        upper_limit_guess *= 2
        last_result = result
        result = func_base(upper_limit_guess)

    return (lower_limit_guess, last_result, upper_limit_guess, result)

if __name__ == '__main__':

    lower_limit, lower_limit_norm, upper_limit, upper_limit_norm = compute_fast_limits()
    max_tries = ConfigReader().get_max_tries()

    try:
        bisect(func, lower_limit, upper_limit, maxiter=max_tries, args=(lower_limit, lower_limit_norm, upper_limit, upper_limit_norm))
    except:
        pass

    SubsetResultFunc(source_dbc, destination_dbc, schema).tabulate()


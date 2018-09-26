import config_reader
import database_helper


def tabulate(source_dbc, destination_dbc, tables):
    #tabulate
    row_counts = list()
    for table in tables:
        with source_dbc.get_db_connection() as conn:
            o = database_helper.get_table_count(table_name(table), schema_name(table), conn)
        with destination_dbc.get_db_connection() as conn:
            n = database_helper.get_table_count(table_name(table), schema_name(table), conn)
        row_counts.append((table,o,n))

    print('\n'.join([f'{x[0]}, {x[1]}, {x[2]}, {x[2]/x[1] if x[1] > 0 else 0}' for x in row_counts]))


def schema_name(table):
    return table.split('.')[0]

def table_name(table):
    return table.split('.')[1]

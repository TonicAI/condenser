import config_reader
import database_helper
from db_connect import MySqlConnection


def tabulate(source_dbc, destination_dbc, tables):
    #tabulate
    row_counts = list()
    source_conn = source_dbc.get_db_connection()
    dest_conn = destination_dbc.get_db_connection()
    db_helper = database_helper.get_specific_helper()
    try:
        for table in tables:
            o = db_helper.get_table_count_estimate(table_name(table), schema_name(table), source_conn)
            dest_schema_name = dest_conn.db_name if isinstance(dest_conn, MySqlConnection) else schema_name(table)
            n = db_helper.get_table_count_estimate(table_name(table), dest_schema_name, dest_conn)
            row_counts.append((table,o,n))
    finally:
        source_conn.close()
        dest_conn.close()

    print('\n'.join(['{}, {}, {}, {}'.format(x[0], x[1], x[2], x[2]/x[1] if x[1] > 0 else 0) for x in row_counts]))


def schema_name(table):
    return table.split('.')[0]

def table_name(table):
    return table.split('.')[1]

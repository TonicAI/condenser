import config_reader
import database_helper

class SubsetResultNorm:
    def __init__(self, source_dbc, destination_dbc):
        self.source_db = source_dbc
        self.destination_db = destination_dbc

    def norm(self):

        table = config_reader.get_target_table()
        percent = config_reader.get_target_percent()

        with self.source_db.get_db_connection() as conn:
            original_count = database_helper.get_table_count(table_name(table), schema_name(table), conn)
        with self.destination_db.get_db_connection() as conn:
            new_count = database_helper.get_table_count(table_name(table), schema_name(table), conn)

        current_percent = 100 * (new_count / original_count)

        return percent - current_percent


def schema_name(t):
    return t.split('.')[0]

def table_name(t):
    return t.split('.')[1]


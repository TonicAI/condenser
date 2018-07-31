from config_reader import ConfigReader
import database_helper

class SubsetResultNorm:
    def __init__(self, source_dbc, destination_dbc, schema):
        self.source_db = source_dbc
        self.destination_db = destination_dbc
        self.schema = schema

    def norm(self):
        desired_result = ConfigReader().get_desired_result()

        table = desired_result['table']
        percent = desired_result['percent']

        with self.source_db.get_db_connection() as conn:
            original_count = database_helper.get_table_count(table, self.schema, conn)
        with self.destination_db.get_db_connection() as conn:
            new_count = database_helper.get_table_count(table, self.schema, conn)

        current_percent = 100 * (new_count / original_count)

        return percent - current_percent




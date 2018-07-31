from config_reader import ConfigReader
import database_helper

class SubsetResultFunc:
    def __init__(self, source_dbc, destination_dbc, schema):
        self.source_db = source_dbc
        self.destination_db = destination_dbc
        self.schema = schema

    def tabulate(self):
        #tabulate
        row_counts = list()
        for table in ConfigReader().get_all_tables():
            with self.source_db.get_db_connection() as conn:
                o = database_helper.get_table_count(table, self.schema, conn)
            with self.destination_db.get_db_connection() as conn:
                n = database_helper.get_table_count(table, self.schema, conn)
            row_counts.append((table,o,n))

        print('\n'.join([f'{x[0]}, {x[1]}, {x[2]}, {x[2]/x[1]}' for x in row_counts]))

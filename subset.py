from topo_orderer import TopoOrderer
import database_helper
from config_reader import ConfigReader
import shutil, os, uuid

class Subset:

    def __init__(self, source_dbc, destination_dbc, scalePercent, temp_schema, clean_previous = True):
        self.schema = 'public'
        self.temp_schema = temp_schema
        self.scalePercent = scalePercent

        self.__source_dbc = source_dbc
        self.__destination_dbc = destination_dbc

        self.__source_conn = source_dbc.get_db_connection()
        self.__destination_conn = destination_dbc.get_db_connection()

    def run_downward(self):
        relationships = database_helper.get_fk_relationships(self.__source_conn)
        order = TopoOrderer().get_topological_order_by_tables(relationships)
        order = list(reversed(order))

        database_helper.run_query(f'CREATE SCHEMA IF NOT EXISTS {self.temp_schema}', self.__destination_conn)

        if len(order)==0:
            return

        passthrough_tables = self.__get_passthrough_tables(order)
        sampled_tables = self.__get_sampled_tables(order, passthrough_tables)

        if len(sampled_tables) == 0:
            return

        for t in sampled_tables:
            columns_query = self.__columns_to_copy(t, relationships)
            q = f'SELECT {columns_query} FROM "{self.schema}"."{t}" WHERE random() < {self.scalePercent/100}'
            database_helper.copy_rows(self.__source_conn, self.__destination_conn, q, t, self.schema)

        for t in passthrough_tables:
            #copy passthrough tables directly to new database
            q = f'SELECT * FROM "{self.schema}"."{t}"'
            database_helper.copy_rows(self.__source_conn, self.__destination_conn, q, t, self.schema)

        for c in range(1, len(order)):

            for t in order[c]:
                if t in passthrough_tables:
                    continue

                self.subset_via_parents(t, relationships)

        database_helper.run_query(f'DROP SCHEMA IF EXISTS {self.temp_schema} CASCADE', self.__destination_conn)

    def __get_sampled_tables(self, order, passthrough_tables):

        if len(order)==0:
            return []

        sampled_tables = list()

        for t in order[0]:
            if t in passthrough_tables:
                continue

            sampled_tables.append(t)

        return sampled_tables

    def __get_passthrough_tables(self, order):
        passthrough_tables = ConfigReader().get_passthrough_tables()
        passthrough_threshold = ConfigReader().get_passthrough_threshold()

        for o in order:
            for t in o:
                c = database_helper.get_table_count(t, self.schema, self.__source_conn)
                if c<= passthrough_threshold:
                    passthrough_tables.append(t)
        #an explicitly marked passthrough table canhave under 100 rows in which case it'll appear in final list twice
        return list(set(passthrough_tables))

    #visit parents first, find which ids they reference from child tables then insert those rows into the child tables.
    #e.g.  Table A -> Table B and Table A has the column b_id.  So we SELECT b_id from table_a from our destination database.  And we take those b_ids and run
    # select * from table b where id in (those list of ids) then insert that result set into table b of the destination database
    def subset_via_parents(self, table_name, relationships):
        referencing_tables = database_helper.get_referencing_tables(table_name, self.__source_conn)

        temp_table_name = database_helper.create_id_temp_table(self.__destination_conn, self.temp_schema, 'varchar')

        if len(referencing_tables) > 0:
            pk_name = referencing_tables[0]['pk_column_name']

        for r in referencing_tables:
            parent_name = r['parent_table_name']
            fk_name = r['fk_column_name']

            q=f'SELECT "{fk_name}" FROM "{self.schema}"."{parent_name}"'
            database_helper.copy_rows(self.__destination_conn, self.__destination_conn, q, temp_table_name, self.temp_schema)

        cursor = self.__destination_conn.cursor()
        cursor_name='table_cursor_'+str(uuid.uuid4()).replace('-','')
        q =f'DECLARE {cursor_name} SCROLL CURSOR FOR SELECT distinct t FROM "{self.temp_schema}"."{temp_table_name}"'
        cursor.execute(q)
        fetch_row_count = 10000
        while True:
            cursor.execute(f'FETCH FORWARD {fetch_row_count} FROM {cursor_name}')
            if cursor.rowcount == 0:
                break

            ids = [str(row[0]) for row in cursor.fetchall() if row[0] is not None]

            if len(ids) == 0:
                break

            ids_to_query = ','.join(ids)
            columns_query = self.__columns_to_copy(table_name, relationships)
            q = f'SELECT {columns_query} FROM "{self.schema}"."{table_name}" WHERE {pk_name} IN ({ids_to_query})'
            temp_destination_conn = self.__destination_dbc.get_db_connection()
            database_helper.copy_rows(self.__source_conn, temp_destination_conn, q, table_name, self.schema)
            temp_destination_conn.close()

        cursor.execute(f'CLOSE {cursor_name}')
        cursor.close()

    # this function generally copies all columns as is, but if the table has been selected as
    # breaking a dependency cycle, then it will insert NULLs instead of that table's foreign keys
    # to the child dependency that breaks the cycle
    def __columns_to_copy(self, table, relationships):
        child_breaks = set()
        for dep_break in ConfigReader().get_dependency_breaks():
            if dep_break['parent'] == table:
                child_breaks.add(dep_break['child'])

        if child_breaks:
            columns_to_null = set()
            for rel in relationships:
                if rel['parent_table_name'] == table and rel['child_table_name'] in child_breaks:
                    columns_to_null.add(rel['fk_column_name'])

            columns = database_helper.get_table_columns(table, self.schema, self.__source_conn)
            return ','.join([f'"{c}"' if c not in columns_to_null else f'NULL as "{c}"' for c in columns])
        else:
            return '*'

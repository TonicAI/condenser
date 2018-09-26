from topo_orderer import get_topological_order_by_tables
import database_helper
import config_reader
import shutil, os, uuid, time, itertools

class Subset:

    def __init__(self, source_dbc, destination_dbc, temp_schema, all_tables, clean_previous = True):
        self.temp_schema = temp_schema

        self.__source_dbc = source_dbc
        self.__destination_dbc = destination_dbc

        self.__source_conn = source_dbc.get_db_connection()
        self.__destination_conn = destination_dbc.get_db_connection()

        self.__all_tables = all_tables

    def run_downward(self, scalePercent):
        relationships = database_helper.get_fk_relationships(self.__all_tables, self.__source_conn)
        order = get_topological_order_by_tables(relationships, self.__all_tables)
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
            q = f'SELECT {columns_query} FROM "{schema_name(t)}"."{table_name(t)}" WHERE random() < {scalePercent/100}'
            database_helper.copy_rows(self.__source_conn, self.__destination_conn, q, table_name(t), schema_name(t))

        for t in passthrough_tables:
            #copy passthrough tables directly to new database
            q = f'SELECT * FROM "{schema_name(t)}"."{table_name(t)}"'
            database_helper.copy_rows(self.__source_conn, self.__destination_conn, q, table_name(t), schema_name(t))

        for c in range(1, len(order)):

            for t in order[c]:
                if t in passthrough_tables:
                    continue

                self.subset_via_parents(t, relationships)

        database_helper.run_query(f'DROP SCHEMA IF EXISTS {self.temp_schema} CASCADE', self.__destination_conn)

    def run_middle_out(self):
        relationships = database_helper.get_fk_relationships(self.__all_tables, self.__source_conn)
        order = get_topological_order_by_tables(relationships, self.__all_tables)
        order = list(order)

        database_helper.run_query(f'CREATE SCHEMA IF NOT EXISTS {self.temp_schema}', self.__destination_conn)

        # randomly sample the targets, per their target percentage
        targets = compute_targets(config_reader.get_target_table(), order)
        print('Beginning subsetting with these direct targets: ' + str(targets))
        start_time = time.time()
        for t in targets:
            columns_query = self.__columns_to_copy(t, relationships)
            q = f'SELECT {columns_query} FROM "{schema_name(t)}"."{table_name(t)}" WHERE random() < {targets[t]/100}'
            database_helper.copy_rows(self.__source_conn, self.__destination_conn, q, table_name(t), schema_name(t))
        print(f'Direct target tables completed in {time.time()-start_time}s')


        # greedily grab as many downstream rows as the target strata can support
        downstream_tables = compute_downstream_tables(config_reader.get_target_table(), order)
        print('Beginning greedy downstream subsetting with these tables: ' + str(downstream_tables))
        start_time = time.time()
        processed_tables = set(targets.keys())
        for t in downstream_tables:
            self.__subset_greedily(t, processed_tables, relationships)
            processed_tables.add(t)
        print(f'Greedy subsettings completed in {time.time()-start_time}s')


        # use subset_via_parents to get all supporting rows according to existing needs
        upstream_tables = list(reversed(compute_upstream_tables(config_reader.get_target_table(), order)))
        print('Beginning upstream subsetting with these tables: ' + str(upstream_tables))
        start_time = time.time()
        for t in upstream_tables:
            self.subset_via_parents(t, relationships)
        print(f'Upstream subsetting completed in {time.time()-start_time}s')


    def __subset_greedily(self, target, processed_tables, relationships):

        destination_conn = self.__destination_dbc.get_db_connection()
        temp_target_name = 'subset_temp_' + table_name(target)

        try:
            # copy the whole table
            columns_query = self.__columns_to_copy(target, relationships)
            database_helper.run_query(f'CREATE TABLE "{self.temp_schema}"."{temp_target_name}" AS SELECT * FROM "{schema_name(target)}"."{table_name(target)}" LIMIT 0', destination_conn)
            query = f'SELECT {columns_query} FROM "{schema_name(target)}"."{table_name(target)}"'
            database_helper.copy_rows(self.__source_conn, destination_conn, query, temp_target_name, self.temp_schema)

            # filter it down in the target database
            relevant_key_constraints = list(filter(lambda r: r["child_table_name"] in processed_tables and r["parent_table_name"] == target, relationships))
            clauses = map(lambda kc: f"\"{temp_target_name}\".\"{kc['fk_column_name']}\" IN (SELECT \"{kc['pk_column_name']}\" FROM \"{schema_name(kc['child_table_name'])}\".\"{table_name(kc['child_table_name'])}\")", relevant_key_constraints)
            query = f'SELECT * FROM \"{self.temp_schema}\".\"{temp_target_name}\" WHERE TRUE AND {" AND ".join(clauses)}'
            database_helper.run_query(f'INSERT INTO "{schema_name(target)}"."{table_name(target)}" {query}', destination_conn)
            destination_conn.commit()

        finally:
            # delete temporary table
            database_helper.run_query(f'DROP TABLE IF EXISTS "{self.temp_schema}"."{temp_target_name}"', destination_conn)
            destination_conn.close()


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
        passthrough_tables = config_reader.get_passthrough_tables()
        passthrough_threshold = config_reader.get_passthrough_threshold()

        for o in order:
            for t in o:
                c = database_helper.get_table_count(table_name(t), schema_name(t), self.__source_conn)
                if c<= passthrough_threshold:
                    passthrough_tables.append(t)
        #an explicitly marked passthrough table canhave under 100 rows in which case it'll appear in final list twice
        return list(set(passthrough_tables))

    # Table A -> Table B and Table A has the column b_id.  So we SELECT b_id from table_a from our destination
    # database.  And we take those b_ids and run `select * from table b where id in (those list of ids)` then insert
    # that result set into table b of the destination database
    def subset_via_parents(self, table, relationships):
        referencing_tables = database_helper.get_referencing_tables(table, self.__all_tables, self.__source_conn)

        temp_table_name = database_helper.create_id_temp_table(self.__destination_conn, self.temp_schema, 'varchar')

        if len(referencing_tables) > 0:
            pk_name = referencing_tables[0]['pk_column_name']

        for r in referencing_tables:
            parent_table = r['parent_table_name']
            fk_name = r['fk_column_name']

            q=f'SELECT "{fk_name}" FROM "{schema_name(parent_table)}"."{table_name(parent_table)}"'
            database_helper.copy_rows(self.__destination_conn, self.__destination_conn, q, temp_table_name, self.temp_schema)

        cursor = self.__destination_conn.cursor()
        cursor_name='table_cursor_'+str(uuid.uuid4()).replace('-','')
        q =f'DECLARE {cursor_name} SCROLL CURSOR FOR SELECT DISTINCT t FROM "{self.temp_schema}"."{temp_table_name}"'
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
            columns_query = self.__columns_to_copy(table, relationships)
            q = f'SELECT {columns_query} FROM "{schema_name(table)}"."{table_name(table)}" WHERE {pk_name} IN ({ids_to_query})'
            temp_destination_conn = self.__destination_dbc.get_db_connection()
            database_helper.copy_rows(self.__source_conn, temp_destination_conn, q, table_name(table), schema_name(table))
            temp_destination_conn.close()

        cursor.execute(f'CLOSE {cursor_name}')
        cursor.close()

    # this function generally copies all columns as is, but if the table has been selected as
    # breaking a dependency cycle, then it will insert NULLs instead of that table's foreign keys
    # to the child dependency that breaks the cycle
    def __columns_to_copy(self, table, relationships):
        child_breaks = set()
        for dep_break in config_reader.get_dependency_breaks():
            if dep_break['parent'] == table:
                child_breaks.add(dep_break['child'])

        columns_to_null = set()
        for rel in relationships:
            if rel['parent_table_name'] == table and rel['child_table_name'] in child_breaks:
                columns_to_null.add(rel['fk_column_name'])

        columns = database_helper.get_table_columns(table_name(table), schema_name(table), self.__source_conn)
        return ','.join([f'"{table_name(table)}"."{c}"' if c not in columns_to_null else f'NULL as "{c}"' for c in columns])

def find(f, seq):
    """Return first item in sequence where f(item) == True."""
    for item in seq:
        if f(item):
            return item

def compute_targets(target_table, order):
    target_strata = find(lambda s: target_table in s, order)

    targets = dict()
    for table in target_strata:
        targets[table] = 100
    targets[target_table] = config_reader.get_target_percent()
    return targets

def compute_downstream_tables(target_table, order):
    downstream_tables = []
    in_downstream = False
    for strata in order:
        if in_downstream:
            downstream_tables.extend(strata)
        if target_table in strata:
            in_downstream = True
    return downstream_tables

def compute_upstream_tables(target_table, order):
    upstream_tables = []
    for strata in order:
        if target_table in strata:
            break
        upstream_tables.extend(strata)
    return upstream_tables

def schema_name(table):
    return table.split('.')[0]

def table_name(table):
    return table.split('.')[1]

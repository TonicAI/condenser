from topo_orderer import get_topological_order_by_tables
from subset_utils import UnionFind, schema_name, table_name, find, compute_disconnected_tables, compute_downstream_tables, compute_upstream_tables, columns_joined, columns_tupled, columns_to_copy, quoter, fully_qualified_table, print_progress, mysql_db_name_hack, upstream_filter_match, redact_relationships
import database_helper
import config_reader
import shutil, os, uuid, time, itertools

#
# A QUICK NOTE ON DEFINITIONS:
#
# Foreign key relationships form a graph. We make sure all subsetting happens on DAGs.
# Nodes in the DAG are tables, and FKs point from the table with a FK column to the table
# with the PK column. In other words, tables with FKs are upstream of tables with PKs.
#
# Sometimes we'll refer to tables as downstream or 'target' tables, because they are
# targeted by foreign keys. We will also use upstream or 'fk' tables, because they
# have foreign keys.
#
# Generally speaking, tables downstream of other tables have their membership defined
# by the requirements of their upstream tables. And tables upstream can be more flexible
# about their membership vis-a-vis the downstream tables (i.e. upstream tables can decide
# to include more or less).
#

class Subset:

    def __init__(self, source_dbc, destination_dbc, all_tables, clean_previous = True):
        self.__source_dbc = source_dbc
        self.__destination_dbc = destination_dbc

        self.__source_conn = source_dbc.get_db_connection(read_repeatable=True)
        self.__destination_conn = destination_dbc.get_db_connection()

        self.__all_tables = all_tables

        self.__db_helper = database_helper.get_specific_helper()

        self.__db_helper.turn_off_constraints(self.__destination_conn)


    def run_middle_out(self):
        passthrough_tables = self.__get_passthrough_tables()
        relationships = self.__db_helper.get_unredacted_fk_relationships(self.__all_tables, self.__source_conn)
        disconnected_tables = compute_disconnected_tables(config_reader.get_initial_target_tables(), passthrough_tables, self.__all_tables, relationships)
        connected_tables = [table for table in self.__all_tables if table not in disconnected_tables]
        order = get_topological_order_by_tables(relationships, connected_tables)
        order = list(order)

        # start by subsetting the direct targets
        print('Beginning subsetting with these direct targets: ' + str(config_reader.get_initial_target_tables()))
        start_time = time.time()
        processed_tables = set()
        for idx, target in enumerate(config_reader.get_initial_targets()):
            print_progress(target, idx+1, len(config_reader.get_initial_targets()))
            self.__subset_direct(target, relationships)
            processed_tables.add(target['table'])
        print('Direct target tables completed in {}s'.format(time.time()-start_time))

        # greedily grab rows with foreign keys to rows in the target strata
        upstream_tables = compute_upstream_tables(config_reader.get_initial_target_tables(), order)
        print('Beginning greedy upstream subsetting with these tables: ' + str(upstream_tables))
        start_time = time.time()
        for idx, t in enumerate(upstream_tables):
            print_progress(t, idx+1, len(upstream_tables))
            data_added = self.__subset_upstream(t, processed_tables, relationships)
            if data_added:
                processed_tables.add(t)
        print('Greedy subsettings completed in {}s'.format(time.time()-start_time))

        # process pass-through tables, you need this before subset_downstream, so you can get all required downstream rows
        print('Beginning pass-through tables: ' + str(passthrough_tables))
        start_time = time.time()
        for idx, t in enumerate(passthrough_tables):
            print_progress(t, idx+1, len(passthrough_tables))
            q = 'SELECT * FROM {} LIMIT {}'.format(fully_qualified_table(t), config_reader.get_max_rows_per_table())
            self.__db_helper.copy_rows(self.__source_conn, self.__destination_conn, q, mysql_db_name_hack(t, self.__destination_conn))
        print('Pass-through completed in {}s'.format(time.time()-start_time))

        # use subset_downstream to get all supporting rows according to existing needs
        downstream_tables = compute_downstream_tables(passthrough_tables, disconnected_tables, order)
        print('Beginning downstream subsetting with these tables: ' + str(downstream_tables))
        start_time = time.time()
        for idx, t in enumerate(downstream_tables):
            print_progress(t, idx+1, len(downstream_tables))
            self.subset_downstream(t, relationships)
        print('Downstream subsetting completed in {}s'.format(time.time()-start_time))

        if config_reader.keep_disconnected_tables():
            # get all the data for tables in disconnected components (i.e. pass those tables through)
            print('Beginning disconnected tables: ' + str(disconnected_tables))
            start_time = time.time()
            for idx, t in enumerate(disconnected_tables):
                print_progress(t, idx+1, len(disconnected_tables))
                q = 'SELECT * FROM {}'.format(fully_qualified_table(t))
                self.__db_helper.copy_rows(self.__source_conn, self.__destination_conn, q, mysql_db_name_hack(t, self.__destination_conn))
            print('Disconnected tables completed in {}s'.format(time.time()-start_time))

    def prep_temp_dbs(self):
        self.__db_helper.prep_temp_dbs(self.__source_conn, self.__destination_conn)

    def unprep_temp_dbs(self):
        self.__db_helper.unprep_temp_dbs(self.__source_conn, self.__destination_conn)

    def __subset_direct(self, target, relationships):
        t = target['table']
        columns_query = columns_to_copy(t, relationships, self.__source_conn)
        if 'where' in target:
            q = 'SELECT {} FROM {} WHERE {}'.format(columns_query, fully_qualified_table(t), target['where'])
        elif 'percent' in target:
            if config_reader.get_db_type() == 'postgres':
                q = 'SELECT {} FROM {} WHERE random() < {}'.format(columns_query, fully_qualified_table(t), float(target['percent'])/100)
            else:
                q = 'SELECT {} FROM {} WHERE rand() < {}'.format(columns_query, fully_qualified_table(t), float(target['percent'])/100)
        else:
            raise ValueError('target table {} had no \'where\' or \'percent\' term defined, check your configuration.'.format(t))
        self.__db_helper.copy_rows(self.__source_conn, self.__destination_conn, q, mysql_db_name_hack(t, self.__destination_conn))


    def __subset_upstream(self, target, processed_tables, relationships):

        redacted_relationships = redact_relationships(relationships)
        relevant_key_constraints = list(filter(lambda r: r['target_table'] in processed_tables and r['fk_table'] == target, redacted_relationships))
        # this table isn't referenced by anything we've already processed, so let's leave it empty
        #  OR
        # table was already added, this only happens if the upstream table was also a direct target
        if len(relevant_key_constraints) == 0 or target in processed_tables:
            return False

        temp_target_name = 'subset_temp_' + table_name(target)

        try:
            # copy the whole table
            columns_query = columns_to_copy(target, relationships, self.__source_conn)
            self.__db_helper.run_query('CREATE TEMPORARY TABLE {} AS SELECT * FROM {} LIMIT 0'.format(quoter(temp_target_name), fully_qualified_table(mysql_db_name_hack(target, self.__destination_conn))), self.__destination_conn)
            query = 'SELECT {} FROM {}'.format(columns_query, fully_qualified_table(target))
            self.__db_helper.copy_rows(self.__source_conn, self.__destination_conn, query, temp_target_name)

            # filter it down in the target database
            table_columns = self.__db_helper.get_table_columns(table_name(target), schema_name(target), self.__source_conn)
            clauses = ['{} IN (SELECT {} FROM {})'.format(columns_tupled(kc['fk_columns']), columns_joined(kc['target_columns']), fully_qualified_table(mysql_db_name_hack(kc['target_table'], self.__destination_conn))) for kc in relevant_key_constraints]
            clauses.extend(upstream_filter_match(target, table_columns))

            select_query = 'SELECT * FROM {} WHERE TRUE AND {}'.format(quoter(temp_target_name), ' AND '.join(clauses))
            select_query += " LIMIT {}".format(config_reader.get_max_rows_per_table())
            insert_query = 'INSERT INTO {} {}'.format(fully_qualified_table(mysql_db_name_hack(target, self.__destination_conn)), select_query)
            self.__db_helper.run_query(insert_query, self.__destination_conn)
            self.__destination_conn.commit()

        finally:
            # delete temporary table
            mysql_temporary = 'TEMPORARY' if config_reader.get_db_type() == 'mysql' else ''
            self.__db_helper.run_query('DROP {} TABLE IF EXISTS {}'.format(mysql_temporary, quoter(temp_target_name)), self.__destination_conn)

        return True


    def __get_passthrough_tables(self):
        passthrough_tables = config_reader.get_passthrough_tables()
        return list(set(passthrough_tables))

    # Table A -> Table B and Table A has the column b_id.  So we SELECT b_id from table_a from our destination
    # database.  And we take those b_ids and run `select * from table b where id in (those list of ids)` then insert
    # that result set into table b of the destination database
    def subset_downstream(self, table, relationships):
        referencing_tables = self.__db_helper.get_redacted_table_references(table, self.__all_tables, self.__source_conn)

        if len(referencing_tables) > 0:
            pk_columns = referencing_tables[0]['target_columns']
        else:
            return

        temp_table = self.__db_helper.create_id_temp_table(self.__destination_conn, len(pk_columns))

        for r in referencing_tables:
            fk_table = r['fk_table']
            fk_columns = r['fk_columns']

            q='SELECT {} FROM {} WHERE {} NOT IN (SELECT {} FROM {})'.format(columns_joined(fk_columns), fully_qualified_table(mysql_db_name_hack(fk_table, self.__destination_conn)), columns_tupled(fk_columns), columns_joined(pk_columns), fully_qualified_table(mysql_db_name_hack(table, self.__destination_conn)))
            self.__db_helper.copy_rows(self.__destination_conn, self.__destination_conn, q, temp_table)

        columns_query = columns_to_copy(table, relationships, self.__source_conn)

        cursor_name='table_cursor_'+str(uuid.uuid4()).replace('-','')
        cursor = self.__destination_conn.cursor(name=cursor_name, withhold=True)
        cursor_query ='SELECT DISTINCT * FROM {}'.format(fully_qualified_table(temp_table))
        cursor.execute(cursor_query)
        fetch_row_count = 100000
        while True:
            rows = cursor.fetchmany(fetch_row_count)
            if len(rows) == 0:
                break

            ids = ['('+','.join(['\'' + str(c) + '\'' for c in row])+')' for row in rows if all([c is not None for c in row])]

            if len(ids) == 0:
                break

            ids_to_query = ','.join(ids)
            q = 'SELECT {} FROM {} WHERE {} IN ({})'.format(columns_query, fully_qualified_table(table), columns_tupled(pk_columns), ids_to_query)
            self.__db_helper.copy_rows(self.__source_conn, self.__destination_conn, q, mysql_db_name_hack(table, self.__destination_conn))

        cursor.close()

import os, uuid, csv
import config_reader
from pathlib import Path
from subset_utils import columns_joined, columns_tupled, quoter, schema_name, table_name, fully_qualified_table, redact_relationships

system_schemas_str = ','.join(['\'' + schema + '\'' for schema in  ['information_schema', 'performance_schema', 'sys', 'mysql', 'innodb','tmp']])
temp_db = 'tonic_subset_temp_db_398dhjr23'

def prep_temp_dbs(source_conn, destination_conn):
    run_query('DROP DATABASE IF EXISTS ' + temp_db, source_conn)
    run_query('DROP DATABASE IF EXISTS ' + temp_db, destination_conn)
    run_query('CREATE DATABASE IF NOT EXISTS ' + temp_db, source_conn)
    run_query('CREATE DATABASE IF NOT EXISTS ' + temp_db, destination_conn)

def unprep_temp_dbs(source_conn, destination_conn):
    run_query('DROP DATABASE IF EXISTS ' + temp_db, source_conn)
    run_query('DROP DATABASE IF EXISTS ' + temp_db, destination_conn)

def turn_off_constraints(connection):
    cur = connection.cursor()
    try:
        cur.execute('SET UNIQUE_CHECKS=0, FOREIGN_KEY_CHECKS=0;')
    finally:
        cur.close()

def copy_rows(source, destination, query, destination_table):
    cursor = source.cursor()

    try:
        cursor.execute(query)
        fetch_row_count = 1000
        while True:
            rows = cursor.fetchmany(fetch_row_count)
            if len(rows) == 0:
                break

            template = ','.join(['%s']*len(rows[0]))
            destination_cursor = destination.cursor()
            insert_query = 'INSERT INTO {} VALUES ({})'.format(fully_qualified_table(destination_table), template)
            destination_cursor.executemany(insert_query, rows)

            destination_cursor.close()
            destination.commit()

            if len(rows) < fetch_row_count:
                # necessary because mysql doesn't behave if you fetchmany after the last row
                break
    except Exception as e:
        if hasattr(e, 'msg') and e.msg.startswith('Table') and e.msg.endswith('doesn\'t exist'):
            raise ValueError('Your database has foreign keys to another database. This is not currently supported.')
        else:
            raise e
    finally:
        cursor.close()

def create_id_temp_table(conn, number_of_columns):
    temp_table = temp_db + '.' + str(uuid.uuid4())
    cursor = conn.cursor()
    column_defs = ',\n'.join(['    col' + str(aye) + '  text' for aye in range(number_of_columns)])
    q = 'CREATE TABLE {} (\n {} \n)'.format(fully_qualified_table(temp_table), column_defs)
    cursor.execute(q)
    cursor.close()
    return temp_table

def copy_to_temp_table(conn, query, target_table, pk_columns = None):
    cur = conn.cursor()
    temp_table = fully_qualified_table(source_db_temp_table(target_table))
    try:
        cur.execute('CREATE TABLE IF NOT EXISTS ' + temp_table + ' AS ' + query + ' LIMIT 0')
        if pk_columns:
            query = query + ' WHERE {} NOT IN (SELECT {} FROM {})'.format(columns_tupled(pk_columns), columns_joined(pk_columns), temp_table)
        cur.execute('INSERT INTO ' + temp_table + ' ' + query)
        conn.commit()
    finally:
        cur.close()

def clean_temp_table_cells(fk_table, fk_columns, target_table, target_columns, conn):
    fk_alias = 'tonic_subset_398dhjr23_fk'
    target_alias = 'tonic_subset_398dhjr23_target'

    fk_table = fully_qualified_table(source_db_temp_table(fk_table))
    target_table = fully_qualified_table(source_db_temp_table(target_table))
    assignment_list = ','.join(['{}.{} = NULL'.format(fk_alias, quoter(c)) for c in fk_columns])
    column_matching = ' AND '.join(['{}.{} = {}.{}'.format(fk_alias, quoter(fc), target_alias, quoter(tc)) for fc, tc in zip(fk_columns, target_columns)])
    target_columns_null = ' AND '.join(['{}.{} IS NULL'.format(target_alias, quoter(tc)) for tc in target_columns]
        + ['{}.{} IS NOT NULL'.format(fk_alias, quoter(c)) for c in fk_columns])
    q = 'UPDATE {} {} LEFT JOIN {} {} ON {} SET {} WHERE {}'.format(fk_table, fk_alias, target_table, target_alias, column_matching, assignment_list, target_columns_null)
    run_query(q, conn)

def source_db_temp_table(target_table):
    return temp_db + '.' + schema_name(target_table) + '_' + table_name(target_table)

def get_redacted_table_references(table_name, tables, conn):
    relationships = get_unredacted_fk_relationships(tables, conn)
    redacted = redact_relationships(relationships)
    return [r for r in redacted if r['target_table']==table_name]

def get_unredacted_fk_relationships(tables, conn):
    cur = conn.cursor()

    q = '''
    SELECT
        concat(table_schema, '.', table_name) AS fk_table,
        group_concat(column_name ORDER BY ordinal_position) AS fk_column,
        concat(referenced_table_schema, '.', referenced_table_name) AS pk_name,
        group_concat(referenced_column_name ORDER BY ordinal_position) AS pk_name
    FROM
        information_schema.key_column_usage
    WHERE
        referenced_table_schema NOT IN ({})
    GROUP BY 1, 3, constraint_schema, constraint_name;
    '''.format(system_schemas_str)

    cur.execute(q)

    relationships = list()

    for row in cur.fetchall():
        d = dict()
        d['fk_table'] = row[0]
        d['fk_columns'] = row[1].split(',')
        d['target_table'] = row[2]
        d['target_columns'] = row[3].split(',')

        if d['fk_table'] in tables and d['target_table'] in tables:
            relationships.append( d )
    cur.close()

    for augment in config_reader.get_fk_augmentation():
        not_present = True
        for r in relationships:
            not_present = not_present and not all([r[key] == augment[key] for key in r.keys()])
            if not not_present:
                break

        if augment['fk_table'] in tables and augment['target_table'] in tables and not_present:
            relationships.append(augment)

    return relationships

def run_query(query, conn, commit=True):
    cur = conn.cursor()
    try:
        cur.execute(query)
        if commit:
            conn.commit()
    finally:
        cur.close()

def get_table_count_estimate(table_name, schema, conn):
    cur = conn.cursor()
    try:
        cur.execute('SELECT table_rows AS count FROM information_schema.tables WHERE table_schema=\'{}\' AND table_name=\'{}\''.format(conn.db_name, table_name))
        return cur.fetchone()[0]
    finally:
        cur.close()

def get_table_columns(table, schema, conn):
    cur = conn.cursor()
    try:
        cur.execute('SELECT column_name FROM information_schema.columns WHERE table_schema = \'{}\' AND table_name = \'{}\' ORDER BY ordinal_position'.format(conn.db_name, table))
        return [r[0] for r in cur.fetchall()]
    finally:
        cur.close()

def list_all_tables(db_connect):
    conn = db_connect.get_db_connection()
    cur = conn.cursor()
    config_reader.get_source_db_connection_info()
    try:
        cur.execute('''SELECT
                            table_name
                        FROM
                            information_schema.tables
                        WHERE
                            table_schema = '{}' AND table_type = 'BASE TABLE';'''.format(db_connect.db_name))
        return [r[0] for r in cur.fetchall()]
    finally:
        cur.close()

def truncate_table(target_table, conn):
    cur = conn.cursor()
    try:
        cur.execute("TRUNCATE TABLE {}".format(target_table))
        conn.commit()
    finally:
        cur.close()

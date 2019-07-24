import os, uuid, csv
import config_reader
from pathlib import Path
from psycopg2.extras import execute_values, register_default_json, register_default_jsonb
from subset_utils import columns_joined, columns_tupled, schema_name, table_name, fully_qualified_table

register_default_json(loads=lambda x: str(x))
register_default_jsonb(loads=lambda x: str(x))

def prep_temp_dbs(_, __):
    pass

def unprep_temp_dbs(_, __):
    pass

def turn_off_constraints(connection):
    # can't be done in postgres
    pass

def copy_rows(source, destination, query, destination_table):
    datatypes = get_table_datatypes(table_name(destination_table), schema_name(destination_table), destination)

    def template_piece(dt):
        if dt == '_json':
            return '%s::json[]'
        elif dt == '_jsonb':
            return '%s::jsonb[]'
        else:
            return '%s'

    template = '(' + ','.join([template_piece(dt) for dt in datatypes]) + ')'


    cursor_name='table_cursor_'+str(uuid.uuid4()).replace('-','')
    cursor = source.cursor(name=cursor_name)
    cursor.execute(query)

    fetch_row_count = 100000
    while True:
        rows = cursor.fetchmany(fetch_row_count)
        if len(rows) == 0:
            break

        # we end up doing a lot of execute statements here, copying data.
        # using the inner_cursor means we don't log all the noise
        destination_cursor = destination.cursor().inner_cursor

        insert_query = 'INSERT INTO {} VALUES %s'.format(fully_qualified_table(destination_table))

        execute_values(destination_cursor, insert_query, rows, template)

        destination_cursor.close()

    cursor.close()
    destination.commit()

def source_db_temp_table(target_table):
    return  'tonic_subset_' + schema_name(target_table) + '_' + table_name(target_table)

def create_id_temp_table(conn, number_of_columns):
    table_name = 'tonic_subset_' + str(uuid.uuid4())
    cursor = conn.cursor()
    column_defs = ',\n'.join(['    col' + str(aye) + '  varchar' for aye in range(number_of_columns)])
    q = 'CREATE TEMPORARY TABLE "{}" (\n {} \n)'.format(table_name, column_defs)
    cursor.execute(q)
    cursor.close()
    return table_name

def copy_to_temp_table(conn, query, target_table, pk_columns = None):
    temp_table = fully_qualified_table(source_db_temp_table(target_table))
    with conn.cursor() as cur:
        cur.execute('CREATE TEMPORARY TABLE IF NOT EXISTS ' + temp_table + ' AS ' + query + ' LIMIT 0')
        if pk_columns:
            query = query + ' WHERE {} NOT IN (SELECT {} FROM {})'.format(columns_tupled(pk_columns), columns_joined(pk_columns), temp_table)
        cur.execute('INSERT INTO ' + temp_table + ' ' + query)
        conn.commit()

def get_referencing_tables(table_name, tables, conn):
    return [r for r in __get_redacted_fk_relationships(tables, conn) if r['target_table']==table_name]

def __get_redacted_fk_relationships(tables, conn):
    relationships = get_unredacted_fk_relationships(tables, conn)
    breaks = config_reader.get_dependency_breaks()
    relationships = [r for r in relationships if (r['fk_table'], r['target_table']) not in breaks]
    return relationships

def get_unredacted_fk_relationships(tables, conn):
    cur = conn.cursor()

    q = '''
    SELECT fk_nsp.nspname || '.' || fk_table AS fk_table, array_agg(fk_att.attname ORDER BY fk_att.attnum) AS fk_columns, tar_nsp.nspname || '.' || target_table AS target_table, array_agg(tar_att.attname ORDER BY fk_att.attnum) AS target_columns
    FROM (
        SELECT
            fk.oid AS fk_table_id,
            fk.relnamespace AS fk_schema_id,
            fk.relname AS fk_table,
            unnest(con.conkey) as fk_column_id,

            tar.oid AS target_table_id,
            tar.relnamespace AS target_schema_id,
            tar.relname AS target_table,
            unnest(con.confkey) as target_column_id,

            con.connamespace AS constraint_nsp,
            con.conname AS constraint_name

        FROM pg_constraint con
        JOIN pg_class fk ON con.conrelid = fk.oid
        JOIN pg_class tar ON con.confrelid = tar.oid
        WHERE con.contype = 'f'
    ) sub
    JOIN pg_attribute fk_att ON fk_att.attrelid = fk_table_id AND fk_att.attnum = fk_column_id
    JOIN pg_attribute tar_att ON tar_att.attrelid = target_table_id AND tar_att.attnum = target_column_id
    JOIN pg_namespace fk_nsp ON fk_schema_id = fk_nsp.oid
    JOIN pg_namespace tar_nsp ON target_schema_id = tar_nsp.oid
    GROUP BY 1, 3, sub.constraint_nsp, sub.constraint_name;
    '''

    cur.execute(q)

    relationships = list()

    for row in cur.fetchall():
        d = dict()
        d['fk_table'] = row[0]
        d['fk_columns'] = row[1]
        d['target_table'] = row[2]
        d['target_columns'] = row[3]

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
    with conn.cursor() as cur:
        cur.execute(query)
        if commit:
            conn.commit()

def get_table_count_estimate(table_name, schema, conn):
    with conn.cursor() as cur:
        cur.execute('SELECT reltuples::BIGINT AS count FROM pg_class WHERE oid=\'"{}"."{}"\'::regclass'.format(schema, table_name))
        return cur.fetchone()[0]

def get_table_columns(table, schema, conn):
    with conn.cursor() as cur:
        cur.execute('SELECT attname FROM pg_attribute WHERE attrelid=\'"{}"."{}"\'::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum;'.format(schema, table))
        return [r[0] for r in cur.fetchall()]

def list_all_user_schemas(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname NOT LIKE 'pg\_%' and nspname != 'information_schema';")
        return [r[0] for r in cur.fetchall()]

def list_all_tables(db_connect):
    conn = db_connect.get_db_connection()
    with conn.cursor() as cur:
        cur.execute("""SELECT concat(concat(nsp.nspname,'.'),cls.relname)
                        FROM pg_class cls
                        JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace
                        WHERE nsp.nspname NOT IN ('information_schema', 'pg_catalog') AND cls.relkind = 'r';""")
        return [r[0] for r in cur.fetchall()]

def get_table_datatypes(table, schema, conn):
    if not schema:
        table_clause = "cl.relname = '{}'".format(table)
    else:
        table_clause = "cl.relname = '{}' AND ns.nspname = '{}'".format(table, schema)
    with conn.cursor() as cur:
        cur.execute("""SELECT ty.typname
                        FROM pg_attribute att
                        JOIN pg_class cl ON cl.oid = att.attrelid
                        JOIN pg_type ty ON ty.oid = att.atttypid
                        JOIN pg_namespace ns ON ns.oid = cl.relnamespace
                        WHERE {} AND att.attnum > 0 AND
                        NOT att.attisdropped
                        ORDER BY att.attnum;
                    """.format(table_clause))

        return [r[0] for r in cur.fetchall()]

def truncate_table(target_table, conn):
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE {}".format(target_table))
        conn.commit()

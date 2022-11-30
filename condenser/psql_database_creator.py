import os, urllib, subprocess
from condenser.db_connect import DbConnect
from condenser import database_helper

class PsqlDatabaseCreator:
    def __init__(self, source_dbc, destination_dbc, use_existing_dump = False):
        self.destination_dbc = destination_dbc
        self.source_dbc = source_dbc
        self.__source_db_connection = source_dbc.get_db_connection()

        self.use_existing_dump = use_existing_dump

        self.output_path = os.path.join(os.getcwd(),'SQL')
        if not os.path.isdir(self.output_path):
            os.mkdir(self.output_path)

        self.add_constraint_output_path = os.path.join(os.getcwd(), 'SQL', 'add_constraint_output.txt')
        self.add_constraint_error_path = os.path.join(os.getcwd(), 'SQL', 'add_constraint_error.txt')

        if os.path.exists(self.add_constraint_output_path):
            os.remove(self.add_constraint_output_path)
        if os.path.exists(self.add_constraint_error_path):
            os.remove(self.add_constraint_error_path)


        self.create_output_path = os.path.join(os.getcwd(), 'SQL', 'create_output.txt')
        self.create_error_path = os.path.join(os.getcwd(), 'SQL', 'create_error.txt')

        if os.path.exists(self.create_output_path):
            os.remove(self.create_output_path)
        if os.path.exists(self.create_error_path):
            os.remove(self.create_error_path)

    def create(self):

        if self.use_existing_dump == True:
            pass
        else:
            cur_path = os.getcwd()

            pg_dump_path = get_pg_bin_path()
            if pg_dump_path != '':
                os.chdir(pg_dump_path)

            connection = '--dbname=postgresql://{0}@{2}:{3}/{4}?{1}'.format(self.source_dbc.user, urllib.parse.urlencode({'password': self.source_dbc.password}), self.source_dbc.host, self.source_dbc.port, self.source_dbc.db_name)

            result = subprocess.run(['pg_dump', connection, '--schema-only', '--no-owner', '--no-privileges', '--section=pre-data']
                    , stdout = subprocess.PIPE, stderr = subprocess.PIPE)
            if result.returncode != 0 or contains_errors(result.stderr):
                raise Exception('Captuing pre-data schema failed. Details:\n{}'.format(result.stderr))
            os.chdir(cur_path)

            pre_data_sql = self.__filter_commands(result.stdout.decode('utf-8'))
            self.run_psql(pre_data_sql)

    def teardown(self):
        user_schemas = database_helper.get_specific_helper().list_all_user_schemas(self.__source_db_connection)

        if len(user_schemas) == 0:
            raise Exception("Couldn't find any non system schemas.")

        drop_statements = ["DROP SCHEMA IF EXISTS \"{}\" CASCADE".format(s) for s in user_schemas if s != 'public']

        q = ';'.join(drop_statements)
        q += ";DROP SCHEMA IF EXISTS public CASCADE;CREATE SCHEMA IF NOT EXISTS public;"

        self.run_query(q)


    def add_constraints(self):
        if self.use_existing_dump == True:
            pass
        else:
            cur_path = os.getcwd()

            pg_dump_path = get_pg_bin_path()
            if pg_dump_path != '':
                os.chdir(pg_dump_path)
            connection = '--dbname=postgresql://{0}@{2}:{3}/{4}?{1}'.format(self.source_dbc.user, urllib.parse.urlencode({'password': self.source_dbc.password}), self.source_dbc.host, self.source_dbc.port, self.source_dbc.db_name)
            result = subprocess.run(['pg_dump', connection, '--schema-only', '--no-owner', '--no-privileges', '--section=post-data']
                    , stderr = subprocess.PIPE, stdout = subprocess.PIPE)
            if result.returncode != 0 or contains_errors(result.stderr):
                raise Exception('Captuing post-data schema failed. Details:\n{}'.format(result.stderr))

            os.chdir(cur_path)

            self.run_psql(result.stdout.decode('utf-8'))

    def __filter_commands(self, input):

        input = input.split('\n')
        filtered_key_words = [
            'COMMENT ON CONSTRAINT',
            'COMMENT ON EXTENSION'
        ]

        retval = []
        for line in input:
            l = line.rstrip()
            filtered = False
            for key in filtered_key_words:
                if l.startswith(key):
                    filtered = True

            if not filtered:
                retval.append(l)

        return '\n'.join(retval)

    def run_query(self, query):

        pg_dump_path = get_pg_bin_path()
        cur_path = os.getcwd()

        if(pg_dump_path != ''):
            os.chdir(pg_dump_path)

        connection_info = self.destination_dbc
        connection_string = '--dbname=postgresql://{0}@{2}:{3}/{4}?{1}'.format(
                    connection_info.user, urllib.parse.urlencode({'password': connection_info.password}), connection_info.host,
                    connection_info.port, connection_info.db_name)


        result = subprocess.run(['psql', connection_string, '-c {0}'.format(query)], stderr = subprocess.PIPE, stdout = subprocess.DEVNULL)
        if result.returncode != 0 or contains_errors(result.stderr):
            raise Exception('Running query: "{}" failed. Details:\n{}'.format(query, result.stderr))

        os.chdir(cur_path)

    def run_psql(self, queries):

        pg_dump_path = get_pg_bin_path()
        cur_path = os.getcwd()

        if(pg_dump_path != ''):
            os.chdir(pg_dump_path)

        connect = self.destination_dbc
        connection_string = '--dbname=postgresql://{0}@{2}:{3}/{4}?{1}'.format(
            connect.user, urllib.parse.urlencode({'password': connect.password}), connect.host,
            connect.port, connect.db_name)

        input = queries.encode('utf-8')
        result = subprocess.run(['psql', connection_string], stderr = subprocess.PIPE, input = input, stdout= subprocess.DEVNULL)
        if result.returncode != 0 or contains_errors(result.stderr):
            raise Exception('Creating schema failed. Details:\n{}'.format(result.stderr))

        os.chdir(cur_path)

def get_pg_bin_path():
    if 'POSTGRES_PATH' in os.environ:
        pg_dump_path = os.environ['POSTGRES_PATH']
    else:
        pg_dump_path = ''
    err = os.system('"' + os.path.join(pg_dump_path, 'pg_dump') + '"' + ' --help > ' + os.devnull)
    if err != 0:
        raise Exception("Couldn't find Postgres utilities, consider specifying POSTGRES_PATH environment variable if Postgres isn't " +
            "in your PATH.")
    return pg_dump_path

def contains_errors(stderr):
    msgs = stderr.decode('utf-8')
    return any(filter(lambda msg: msg.strip().startswith('ERROR'), msgs.split('\n')))

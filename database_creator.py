import os, urllib, subprocess
from db_connect import DbConnect
from psql_runner import PsqlRunner
from psql_runner import get_pg_bin_path
from database_helper import list_all_user_schemas

class DatabaseCreator:
    def __init__(self, source_dbc, destination_dbc, temp_schema, use_existing_dump = False):
        self.destination_connection_info = destination_dbc.get_db_connection_info()
        self.source_connection_info = source_dbc.get_db_connection_info()
        self.__source_db_connection = source_dbc.get_db_connection()

        self.use_existing_dump = use_existing_dump
        self.destination_psql_client = PsqlRunner(self.destination_connection_info)

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

        self.temp_schema = temp_schema

    def create(self):

        if self.use_existing_dump == True:
            pass
        else:
            cur_path = os.getcwd()

            pg_dump_path = get_pg_bin_path()
            if pg_dump_path != '':
                os.chdir(pg_dump_path)

            connection = '--dbname=postgresql://{0}@{2}:{3}/{4}?{1}'.format(self.source_connection_info['user_name'], urllib.parse.urlencode({'password': self.source_connection_info['password']}), self.source_connection_info['host'], self.source_connection_info['port'], self.source_connection_info['db_name'])

            result = subprocess.run(['pg_dump', connection, '--schema-only', '--no-owner', '--no-privileges', '--section=pre-data']
                    , stdout = subprocess.PIPE, stderr = subprocess.PIPE)
            if result.returncode != 0:
                raise Exception('Captuing pre-data schema failed. Details:\n{}'.format(result.stderr))
            os.chdir(cur_path)

            pre_data_sql = self.__filter_commands(result.stdout.decode('utf-8'))
            self.destination_psql_client.run(pre_data_sql)

    def teardown(self):
        user_schemas = list_all_user_schemas(self.__source_db_connection)

        if len(user_schemas) == 0:
            raise Exception("Couldn't find any non system schemas.")

        drop_statements = ["DROP SCHEMA IF EXISTS {} CASCADE;".format(s) for s in user_schemas if s != 'public']

        q = ';'.join(drop_statements)
        q += "DROP SCHEMA IF EXISTS public CASCADE;CREATE SCHEMA IF NOT EXISTS public;"

        self.destination_psql_client.run_query(q)

        q = 'DROP SCHEMA IF EXISTS {schema} CASCADE;CREATE SCHEMA IF NOT EXISTS {schema};'.format(schema=self.temp_schema)
        self.destination_psql_client.run_query(q)


    def add_constraints(self):
        if self.use_existing_dump == True:
            pass
        else:
            cur_path = os.getcwd()

            pg_dump_path = get_pg_bin_path()
            if pg_dump_path != '':
                os.chdir(pg_dump_path)
            connection = '--dbname=postgresql://{0}@{2}:{3}/{4}?{1}'.format(self.source_connection_info['user_name'], urllib.parse.urlencode({'password': self.source_connection_info['password']}), self.source_connection_info['host'], self.source_connection_info['port'], self.source_connection_info['db_name'])
            result = subprocess.run(['pg_dump', connection, '--schema-only', '--no-owner', '--no-privileges', '--section=post-data']
                    , stderr = subprocess.PIPE, stdout = subprocess.PIPE)
            if result.returncode != 0:
                raise Exception('Captuing post-data schema failed. Details:\n{}'.format(result.stderr))

            os.chdir(cur_path)

            self.destination_psql_client.run(result.stdout.decode('utf-8'))

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







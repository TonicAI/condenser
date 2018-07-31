import os
from db_connect import DbConnect
from config_reader import ConfigReader
from psql_runner import PsqlRunner
from psql_runner import get_pg_bin_path

class DatabaseCreator:
    def __init__(self, source_dbc, destination_dbc, temp_schema, use_existing_dump = False):
        self.destination_connection_info = destination_dbc.get_db_connection_info()
        self.source_connection_info = source_dbc.get_db_connection_info()

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

            tt = ConfigReader().get_all_tables()
            tables = ' '.join(['-t ' + t for t in tt])

            cur_path = os.getcwd()

            pg_dump_path = get_pg_bin_path()
            if pg_dump_path != '':
                os.chdir(pg_dump_path)

            pg_dumpsql_path = os.path.join(self.output_path, 'schema_dump.sql')
            os.system('pg_dump --dbname=postgresql://{0}:{1}@{2}:{3}/{4} > {5} --schema-only --no-owner --no-privileges {6}'
                .format(self.source_connection_info['user_name'], self.source_connection_info['password'], self.source_connection_info['host'], self.source_connection_info['port'], self.source_connection_info['db_name'], pg_dumpsql_path, tables))

            os.chdir(cur_path)

            self.__filter_commands(self.output_path)
            self.destination_psql_client.run(os.path.join(self.output_path, 'dump_create.sql'), self.create_output_path, self.create_error_path, True, True)

    def teardown(self):
        q = 'DROP SCHEMA public CASCADE;CREATE SCHEMA public;GRANT ALL ON SCHEMA public TO postgres;GRANT ALL ON SCHEMA public TO public;'
        self.destination_psql_client.run_query(q)

        q = f'DROP SCHEMA IF EXISTS {self.temp_schema} CASCADE;CREATE SCHEMA IF NOT EXISTS {self.temp_schema};GRANT ALL ON SCHEMA {self.temp_schema} TO postgres;GRANT ALL ON SCHMA {self.temp_schema} TO public;'

    def add_constraints(self):
        self.destination_psql_client.run(os.path.join(self.output_path, 'dump_constraints_unique.sql'), self.add_constraint_output_path,  self.add_constraint_error_path, False, False )
        self.destination_psql_client.run(os.path.join(self.output_path, 'dump_constraints_pk.sql'), self.add_constraint_output_path,  self.add_constraint_error_path, False, False )
        self.destination_psql_client.run(os.path.join(self.output_path, 'dump_constraints_create_unique.sql'), self.add_constraint_output_path,  self.add_constraint_error_path, False, False )
        self.destination_psql_client.run(os.path.join(self.output_path, 'dump_constraints_fk.sql'), self.add_constraint_output_path,  self.add_constraint_error_path, False, False )
        self.destination_psql_client.run(os.path.join(self.output_path, 'dump_constraints_other.sql'), self.add_constraint_output_path,  self.add_constraint_error_path, False, False )

    def validate_database_create(self):
        with open(self.create_error_path,'r',encoding='utf-8') as fp:
            lines = fp.readlines()
            fp.close()

        if len(lines) > 0:
            raise Exception(f'Creating tables failed.  See {self.create_error_path} for details')

    def validate_constraints(self):
        with open(self.add_constraint_error_path,'r',encoding='utf-8') as fp:
            lines = fp.readlines()
            fp.close()

        if len(lines) > 0:
            raise Exception(f'Adding constraints failed.  See {self.add_constraint_error_path} for details')

    def __filter_commands(self, output_path):

        #commands that add constraints, references, etc.
        constraint_command_keywords = [
            'ADD CONSTRAINT',
            'CREATE INDEX',
            'CREATE UNIQUE INDEX',
            'CREATE TRIGGER'
        ]

        with open(os.path.join(output_path,'schema_dump.sql'), 'r') as fp:
            command=''
            commands = list()
            for line in fp:

                #Skip comments
                if(line.startswith('--')):
                    continue

                command += line

                if(line.rstrip().endswith(';')):
                    commands.append(command)
                    command=''

        create_with_no_constraint_commands = list()
        pk_commands = list()
        create_unique_commands = list()
        unique_constraint_commands = list()
        other_commands = list()
        fk_commands = list()
        for c in commands:
            constraintCommandFound = False
            for s in constraint_command_keywords:
                if(c.find(s) != -1):
                    constraintCommandFound = True
                    break

            if(constraintCommandFound == False):
                create_with_no_constraint_commands.append(c)
            else:
                if c.find('FOREIGN KEY') != -1:
                    fk_commands.append(c)
                elif c.find(' UNIQUE (') != -1:
                    unique_constraint_commands.append(c)
                elif c.find('CREATE UNIQUE INDEX') != -1 or c.find('CREATE INDEX') != -1:
                    create_unique_commands.append(c)
                elif c.find('PRIMARY KEY') != -1:
                    pk_commands.append(c)
                else:
                    other_commands.append(c)

        save_path = os.path.join(output_path, 'dump_create.sql')
        with open(save_path, 'w') as fp:
            fp.writelines(create_with_no_constraint_commands)

        save_path = os.path.join(output_path, 'dump_constraints_pk.sql')
        with open(save_path, 'w') as fp:
            fp.writelines(pk_commands)

        save_path = os.path.join(output_path, 'dump_constraints_fk.sql')
        with open(save_path, 'w') as fp:
            fp.writelines(fk_commands)

        save_path = os.path.join(output_path, 'dump_constraints_unique.sql')
        with open(save_path, 'w') as fp:
            fp.writelines(unique_constraint_commands)

        save_path = os.path.join(output_path, 'dump_constraints_create_unique.sql')
        with open(save_path, 'w') as fp:
            fp.writelines(create_unique_commands)

        save_path = os.path.join(output_path, 'dump_constraints_other.sql')
        with open(save_path, 'w') as fp:
            fp.writelines(other_commands)

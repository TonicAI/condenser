import psycopg2
import os.path
import json
import getpass

class DbConnect:

    def __init__(self, config_file):

        if not os.path.isfile(config_file):
            raise Exception(f'Missing {config_file} file')

        self.config_file = config_file
        self.__set_db_connection_info()

    def get_db_connection(self):

        host = self.connection_info['host']
        user_name = self.connection_info['user_name']
        db_name = self.connection_info['db_name']
        ssl_mode = self.connection_info['ssl_mode'] if 'ssl_mode' in self.connection_info else None
        password = self.connection_info['password']

        connection_string = 'dbname={0} user={1} password={2} host={3}'.format(db_name, user_name, password, host)
        if ssl_mode :
            connection_string = connection_string + ' sslmode={0}'.format(ssl_mode)

        return psycopg2.connect(connection_string)

    def __set_db_connection_info(self):
        requiredKeys = [
            'user_name',
            'host',
            'db_name',
            'port'
        ]

        try:
            with open(self.config_file, 'r') as fp:
                ci = json.load(fp)
        except json.decoder.JSONDecodeError:
            raise Exception(f'Could not process {self.config_file}')

        for r in requiredKeys:
            if r not in ci.keys():
                raise Exception('Missing required key in .db_connection_info: ' + r)
        if 'password' not in ci.keys():
            ci['password'] = getpass.getpass('Enter password for {0} on host {1}: '.format(ci['user_name'], ci['host']))

        self.connection_info = ci

    def get_db_connection_info(self):
        return self.connection_info


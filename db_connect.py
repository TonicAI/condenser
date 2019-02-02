import psycopg2
import os.path
import json
import getpass

class DbConnect:

    def __init__(self, connection_info):
        requiredKeys = [
            'user_name',
            'host',
            'db_name',
            'port'
        ]

        for r in requiredKeys:
            if r not in connection_info.keys():
                raise Exception('Missing required key in database connection info: ' + r)
        if 'password' not in connection_info.keys():
            connection_info['password'] = getpass.getpass('Enter password for {0} on host {1}: '.format(connection_info['user_name'], connection_info['host']))

        self.connection_info = connection_info

    def get_db_connection(self):

        host = self.connection_info['host']
        user_name = self.connection_info['user_name']
        db_name = self.connection_info['db_name']
        ssl_mode = self.connection_info['ssl_mode'] if 'ssl_mode' in self.connection_info else None
        password = self.connection_info['password']
        port = self.connection_info['port']

        connection_string = 'dbname={0} user={1} password={2} host={3} port={4}'.format(db_name, user_name, password, host, port)
        if ssl_mode :
            connection_string = connection_string + ' sslmode={0}'.format(ssl_mode)

        return psycopg2.connect(connection_string)

    def get_db_connection_info(self):
        return self.connection_info


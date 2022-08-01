import datetime
import getpass
import sys
import time

import mysql.connector
import psycopg2

import config_reader


class DbConnect:

    def __init__(self, db_type, connection_info):
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
            connection_info['password'] = getpass.getpass('Enter password for {0} on host {1}: '.format(
                connection_info['user_name'],
                connection_info['host']))

        self.user = connection_info['user_name']
        self.password = connection_info['password']
        self.host = connection_info['host']
        self.port = connection_info['port']
        self.db_name = connection_info['db_name']
        self.ssl_mode = connection_info['ssl_mode'] if 'ssl_mode' in connection_info else None
        self.__db_type = db_type.lower()

    def get_db_connection(self, read_repeatable=False):

        if self.__db_type == 'postgres':
            return PsqlConnection(self, read_repeatable)
        elif self.__db_type == 'mysql':
            return MySqlConnection(self, read_repeatable)
        else:
            raise ValueError('unknown db_type ' + self.__db_type)


class DbConnection:
    def __init__(self, connection):
        self.connection = connection

    def commit(self):
        self.connection.commit()

    def close(self):
        self.connection.close()


class LoggingCursor:
    def __init__(self, cursor):
        self.inner_cursor = cursor

    def execute(self, query):
        start_time = time.time()
        if config_reader.verbose_logging():
            print('Beginning query @ {}:\n\t{}'.format(str(datetime.datetime.now()), query))
            sys.stdout.flush()
        retval = self.inner_cursor.execute(query)
        if config_reader.verbose_logging():
            print('\tQuery completed in {}s'.format(time.time() - start_time))
            sys.stdout.flush()
        return retval

    def __getattr__(self, name):
        return self.inner_cursor.__getattribute__(name)

    def __exit__(self, a, b, c):
        return self.inner_cursor.__exit__(a, b, c)

    def __enter__(self):
        return LoggingCursor(self.inner_cursor.__enter__())


# small wrapper to the connection class that gives us a common interface to the cursor()
# method across MySQL and Postgres. This one is for Postgres
class PsqlConnection(DbConnection):
    def __init__(self,  connect, read_repeatable):
        connection_string = 'dbname=\'{0}\' user=\'{1}\' password=\'{2}\' host={3} port={4}'.format(
            connect.db_name, connect.user, connect.password, connect.host, connect.port)

        if connect.ssl_mode :
            connection_string = connection_string + ' sslmode={0}'.format(connect.ssl_mode)

        DbConnection.__init__(self, psycopg2.connect(connection_string))
        if read_repeatable:
            self.connection.isolation_level = psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ

    def cursor(self, name=None, withhold=False):
        return LoggingCursor(self.connection.cursor(name=name, withhold=withhold))


# small wrapper to the connection class that gives us a common interface to the cursor()
# method across MySQL and Postgres. This one is for MySQL
class MySqlConnection(DbConnection):
    def __init__(self,  connect, read_repeatable):
        DbConnection.__init__(self, mysql.connector.connect(
            host=connect.host,
            port=connect.port,
            user=connect.user,
            password=connect.password,
            database=connect.db_name))

        self.db_name = connect.db_name

        if read_repeatable:
            self.connection.start_transaction(isolation_level='REPEATABLE READ')

    def cursor(self, name=None, withhold=False):
        return LoggingCursor(self.connection.cursor())

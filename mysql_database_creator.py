import os
import subprocess


class MySqlDatabaseCreator:
    def __init__(self, source_connect, destination_connect):
        self.__source_connect = source_connect
        self.__destination_connect = destination_connect

    def create(self):
        cur_path = os.getcwd()

        mysql_bin_path = get_mysql_bin_path()
        if mysql_bin_path != '':
            os.chdir(mysql_bin_path)

        ca = connection_args(self.__source_connect)
        args = ['mysqldump', '--no-data', '--routines'] + ca + [self.__source_connect.db_name]
        result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode != 0:
            raise Exception('Capturing schema failed. Details:\n{}'.format(result.stderr))
        commands_to_create_schema = result.stdout

        ca = connection_args(self.__destination_connect)
        args = ['mysql'] + ca + ['-e', 'CREATE DATABASE ' + self.__destination_connect.db_name]
        result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode != 0:
            raise Exception('Creating destination database failed. Details:\n{}'.format(result.stderr))

        args = ['mysql', '-D', self.__destination_connect.db_name] + ca
        result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, input=commands_to_create_schema)
        if result.returncode != 0:
            raise Exception('Creating destination schema. Details:\n{}'.format(result.stderr))

        os.chdir(cur_path)

    def teardown(self):
        self.run_query_on_destination('DROP DATABASE IF EXISTS ' + self.__destination_connect.db_name + ';')

    def add_constraints(self):
        # no-op for mysql
        pass

    def run_query_on_destination(self, command):
        cur_path = os.getcwd()
        mysql_bin_path = get_mysql_bin_path()
        if mysql_bin_path != '':
            os.chdir(mysql_bin_path)

        ca = connection_args(self.__destination_connect)
        args = ['mysql'] + ca + ['-e', command]
        result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        os.chdir(cur_path)
        if result.returncode != 0:
            raise Exception('Failed to run command \'{}\'. Details:\n{}'.format(command, result.stderr))


def get_mysql_bin_path():
    if 'MYSQL_PATH' in os.environ:
        mysql_bin_path = os.environ['MYSQL_PATH']
    else:
        mysql_bin_path = ''
    err = os.system('"' + os.path.join(mysql_bin_path, 'mysqldump') + '"' + ' --help > ' + os.devnull)
    if err != 0:
        raise Exception("Couldn't find MySQL utilities, consider specifying MYSQL_PATH environment variable if"
                        " MySQL isn't in your PATH.")
    return mysql_bin_path


def connection_args(connect):
    host_arg = '--host={}'.format(connect.host)
    port_arg = '--port={}'.format(connect.port)
    user_arg = '--user={}'.format(connect.user)
    password_arg = '--password={}'.format(connect.password)
    return [host_arg, port_arg, user_arg, password_arg]


# This is just for unit testing the creation and tear down processes
if __name__ == '__main__':
    import config_reader
    import db_connect
    config_reader.initialize()
    src_connect = db_connect.DbConnect(config_reader.get_source_db_connection_info(), 'mysql')
    dest_connect = db_connect.DbConnect(config_reader.get_destination_db_connection_info(), 'mysql')
    msdbc = MySqlDatabaseCreator(src_connect, dest_connect)
    msdbc.teardown()
    msdbc.create()

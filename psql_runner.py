import os, pathlib, re
class PsqlRunner:
    def __init__(self, database_connection_info):
        self.connection_info = database_connection_info

    def run_query(self, query, output_path = None, error_path = None, overwrite_output = False, overwrite_error = False):

        pg_dump_path = get_pg_bin_path()
        cur_path = os.getcwd()

        if(pg_dump_path != ''):
            os.chdir(pg_dump_path)

        if overwrite_error is False:
            error_redirect = '2>>'
        else:
            error_redirect = '2>'

        if overwrite_output is False:
            output_redirect = '>>'
        else:
            output_redirect = '>'

        if output_path is None and error_path is None:
            os.system('psql --dbname=postgresql://{0}:{1}@{2}:{3}/{4} -c "{5}" > {6} 2> {6}'
                .format(self.connection_info['user_name'],self.connection_info['password'],self.connection_info['host'], self.connection_info['port'], self.connection_info['db_name'], query, os.devnull))

        elif output_path is None and error_path is not None:
            os.system('psql --dbname=postgresql://{0}:{1}@{2}:{3}/{4} -c "{5}" {6} {7} > {8}'
                .format(self.connection_info['user_name'],self.connection_info['password'],self.connection_info['host'], self.connection_info['port'], self.connection_info['db_name'], query, error_redirect, error_path, os.devnull))

        elif output_path is not None and error_path is None:
            os.system('psql --dbname=postgresql://{0}:{1}@{2}:{3}/{4} -c "{5}" {6} {7} 2> {8}'
                .format(self.connection_info['user_name'],self.connection_info['password'],self.connection_info['host'], self.connection_info['port'], self.connection_info['db_name'], query, output_redirect, output_path, os.devnull))

        elif output_path is not None and error_path is not None:
            os.system('psql --dbname=postgresql://{0}:{1}@{2}:{3}/{4} -c "{5}" {6} {7} {8} {9}'
                .format(self.connection_info['user_name'],self.connection_info['password'],self.connection_info['host'], self.connection_info['port'], self.connection_info['db_name'], query, output_redirect, output_path, error_redirect, error_path))

        os.chdir(cur_path)

    def run(self, file_path, output_path = None, error_path = None, overwrite_output = False, overwrite_error = False):

        if not os.path.exists(file_path):
            raise Exception(f'Could not find file {file_path}')

        pg_dump_path = get_pg_bin_path()
        cur_path = os.getcwd()

        if(pg_dump_path != ''):
            os.chdir(pg_dump_path)

        if overwrite_error is False:
            error_redirect = '2>>'
        else:
            error_redirect = '2>'

        if overwrite_output is False:
            output_redirect = '>>'
        else:
            output_redirect = '>'

        if output_path is None and error_path is None:
            os.system('psql --dbname=postgresql://{0}:{1}@{2}:{3}/{4} -a -f {5}'
                .format(self.connection_info['user_name'],self.connection_info['password'],self.connection_info['host'], self.connection_info['port'], self.connection_info['db_name'], file_path))
        elif output_path is None and error_path is not None:
            os.system('psql --dbname=postgresql://{0}:{1}@{2}:{3}/{4} -a -f {5} {6} {7} > {8}'
                .format(self.connection_info['user_name'],self.connection_info['password'],self.connection_info['host'], self.connection_info['port'], self.connection_info['db_name'], file_path, error_redirect, error_path, os.devnull))
        elif output_path is not None and error_path is None:
            os.system('psql --dbname=postgresql://{0}:{1}@{2}:{3}/{4} -a -f {5} {6} {7} 2> {8}'
                .format(self.connection_info['user_name'],self.connection_info['password'],self.connection_info['host'], self.connection_info['port'], self.connection_info['db_name'], file_path, output_redirect, output_path, os.devnull))
        elif output_path is not None and error_path is not None:
            os.system('psql --dbname=postgresql://{0}:{1}@{2}:{3}/{4} -a -f {5} {6} {7} {8} {9}'
                .format(self.connection_info['user_name'],self.connection_info['password'],self.connection_info['host'], self.connection_info['port'], self.connection_info['db_name'], file_path, output_redirect, output_path, error_redirect, error_path))

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

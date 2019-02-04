import os, pathlib, re, urllib, subprocess
class PsqlRunner:
    def __init__(self, database_connection_info):
        self.connection_info = database_connection_info

    def run_query(self, query):

        pg_dump_path = get_pg_bin_path()
        cur_path = os.getcwd()

        if(pg_dump_path != ''):
            os.chdir(pg_dump_path)

        connection_string = '--dbname=postgresql://{0}@{2}:{3}/{4}?{1}'.format(
                    self.connection_info['user_name'], urllib.parse.urlencode({'password': self.connection_info['password']}), self.connection_info['host'],
                    self.connection_info['port'], self.connection_info['db_name'])


        result = subprocess.run(['psql', connection_string, '-c {0}'.format(query)], stderr = subprocess.PIPE, stdout = subprocess.DEVNULL)
        if result.returncode != 0:
            raise Exception('Running query: "{}" failed. Details:\n{}'.format(query, result.stderr))

        os.chdir(cur_path)

    def run(self, queries):

        pg_dump_path = get_pg_bin_path()
        cur_path = os.getcwd()

        if(pg_dump_path != ''):
            os.chdir(pg_dump_path)

        connection_string = '--dbname=postgresql://{0}@{2}:{3}/{4}?{1}'.format(
            self.connection_info['user_name'], urllib.parse.urlencode({'password': self.connection_info['password']}), self.connection_info['host'],
            self.connection_info['port'], self.connection_info['db_name'])

        input = queries.encode('utf-8')
        result = subprocess.run(['psql', connection_string], stderr = subprocess.PIPE, input = input, stdout= subprocess.DEVNULL)
        if result.returncode != 0:
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

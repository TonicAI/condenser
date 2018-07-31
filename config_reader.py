import json

class ConfigReader:
    class __ConfigReader:
        def __init__(self):
            with open('config.json','r') as fp:
                self.config = json.load(fp)

        def get_passthrough_threshold(self):
            return self.config['passthrough_threshold']

        def get_passthrough_tables(self):
            return list(self.config['passthrough_tables'])

        def get_tables(self):
            return list(self.config['tables'])

        def get_all_tables(self):
            return list(self.config['tables'] + self.config['passthrough_tables'])

        def get_dependency_breaks(self):
            return list(self.config['dependency_breaks'])

        def get_desired_result(self):
            return dict(self.config['desired_result'])

        def get_max_tries(self):
            return self.config['max_tries']

    instance = None
    def __init__(self):
        if not ConfigReader.instance:
            ConfigReader.instance = ConfigReader.__ConfigReader()

    def __getattr__(self, name):
        return getattr(self.instance, name)


import json, sys, collections

_config = None

def initialize(file_like = None):
    global _config
    if _config != None:
        print('WARNING: Attempted to initialize configuration twice.', file=sys.stderr)

    if not file_like:
        with open('config.json', 'r') as fp:
            _config = json.load(fp)
    else:
        _config = json.load(file_like)

    if "desired_result" in _config:
        raise ValueError("desired_result is a key in the old config spec. Check the README.md and example-config.json for the latest configuration parameters.")

def get_dependency_breaks():
    DependencyBreak = collections.namedtuple('DependencyBreak', ['fk_table', 'target_table'])
    return set([DependencyBreak(b['fk_table'], b['target_table']) for b in _config['dependency_breaks']])

def get_initial_targets():
    return _config['initial_targets']

def get_initial_target_tables():
    return [target["table"] for target in _config['initial_targets']]

def keep_disconnected_tables():
    return 'keep_disconnected_tables' in _config and bool(_config['keep_disconnected_tables'])

def get_db_type():
    return _config['db_type']

def get_source_db_connection_info():
    return _config['source_db_connection_info']

def get_destination_db_connection_info():
    return _config['destination_db_connection_info']

def get_excluded_tables():
    return list(_config['excluded_tables'])

def get_passthrough_tables():
    return list(_config['passthrough_tables'])

def get_fk_augmentation():
    return list(map(__convert_tonic_format, _config['fk_augmentation']))

def get_upstream_filters():
    return {f["table"] : f["condition"] for f in _config["upstream_filters"]}

def __convert_tonic_format(obj):
    if "fk_schema" in obj:
        return {
            "fk_table": obj["fk_schema"] + "." + obj["fk_table"],
            "fk_columns": obj["fk_columns"],
            "target_table": obj["target_schema"] + "." + obj["target_table"],
            "target_columns": obj["target_columns"],
        }
    else:
        return obj

def verbose_logging():
    return '-v' in sys.argv

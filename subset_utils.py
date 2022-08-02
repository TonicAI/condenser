import config_reader
import database_helper
from db_connect import MySqlConnection


# this function generally copies all columns as is, but if the table has been selected as
# breaking a dependency cycle, then it will insert NULLs instead of that table's foreign keys
# to the downstream dependency that breaks the cycle
def columns_to_copy(table, relationships, conn):
    target_breaks = set()
    opportunists = config_reader.get_preserve_fk_opportunistically()
    for dep_break in config_reader.get_dependency_breaks():
        if dep_break.fk_table == table and dep_break not in opportunists:
            target_breaks.add(dep_break.target_table)

    columns_to_null = set()
    for rel in relationships:
        if rel['fk_table'] == table and rel['target_table'] in target_breaks:
            columns_to_null.update(rel['fk_columns'])

    columns = database_helper.get_specific_helper().get_table_columns(table_name(table), schema_name(table), conn)
    return ','.join(['{}.{}'.format(quoter(table_name(table)), quoter(c))
                     if c not in columns_to_null else 'NULL as {}'.format(quoter(c)) for c in columns])


def upstream_filter_match(target, table_columns):
    ret_val = []
    filters = config_reader.get_upstream_filters()
    for f in filters:
        if "table" in f and target == f["table"]:
            ret_val.append(f["condition"])
        if "column" in f and f["column"] in table_columns:
            ret_val.append(f["condition"])
    return ret_val


def redact_relationships(relationships):
    breaks = config_reader.get_dependency_breaks()
    ret_val = [r for r in relationships if (r['fk_table'], r['target_table']) not in breaks]
    return ret_val


def find(f, seq):
    """Return first item in sequence where f(item) == True."""
    for item in seq:
        if f(item):
            return item


def compute_upstream_tables(target_tables, order):
    upstream_tables = []
    in_upstream = False
    for strata in order:
        if in_upstream:
            upstream_tables.extend(strata)
        if any([tt in strata for tt in target_tables]):
            in_upstream = True
    return upstream_tables


def compute_downstream_tables(passthrough_tables, disconnected_tables, order):
    downstream_tables = []
    for strata in order:
        downstream_tables.extend(strata)
    downstream_tables = list(reversed(list(filter(
        lambda table: table not in passthrough_tables and table not in disconnected_tables, downstream_tables))))
    return downstream_tables


def compute_disconnected_tables(target_tables, passthrough_tables, all_tables, relationships):
    uf = UnionFind()
    for t in all_tables:
        uf.make_set(t)
    for rel in relationships:
        uf.link(rel['fk_table'], rel['target_table'])

    connected_components = set([uf.find(tt) for tt in target_tables])
    connected_components.update([uf.find(pt) for pt in passthrough_tables])
    return [t for t in all_tables if uf.find(t) not in connected_components]


def fully_qualified_table(table):
    if '.' in table:
        return quoter(schema_name(table)) + '.' + quoter(table_name(table))
    else:
        return quoter(table_name(table))


def schema_name(table):
    return table.split('.')[0] if '.' in table else None


def table_name(table):
    split = table.split('.')
    return split[1] if len(split) > 1 else split[0]


def columns_tupled(columns):
    return '(' + ','.join([quoter(c) for c in columns]) + ')'


def columns_joined(columns):
    return ','.join([quoter(c) for c in columns])


def quoter(string_to_quote):
    q = '"' if config_reader.get_db_type() == 'postgres' else '`'
    return q + string_to_quote + q


def print_progress(target, idx, count):
    print('Processing {} of {}: {}'.format(idx, count, target))


class UnionFind:

    def __init__(self):
        self.elementsToId = dict()
        self.elements = []
        self.roots = []
        self.ranks = []

    def __len__(self):
        return len(self.roots)

    def make_set(self, elem):
        self.id_of(elem)

    def find(self, elem):
        x = self.elementsToId[elem]
        if x is None:
            return None

        root_id = self.find_internal(x)
        return self.elements[root_id]

    def find_internal(self, x):
        x0 = x
        while self.roots[x] != x:
            x = self.roots[x]

        while self.roots[x0] != x:
            y = self.roots[x0]
            self.roots[x0] = x
            x0 = y

        return x

    def id_of(self, elem):
        if elem not in self.elementsToId:
            idx = len(self.roots)
            self.elements.append(elem)
            self.elementsToId[elem] = idx
            self.roots.append(idx)
            self.ranks.append(0)

        return self.elementsToId[elem]

    def link(self, elem1, elem2):
        x = self.id_of(elem1)
        y = self.id_of(elem2)

        xr = self.find_internal(x)
        yr = self.find_internal(y)
        if xr == yr:
            return

        xd = self.ranks[xr]
        yd = self.ranks[yr]
        if xd < yd:
            self.roots[xr] = yr
        elif yd < xd:
            self.roots[yr] = xr
        else:
            self.roots[yr] = xr
            self.ranks[xr] = self.ranks[xr] + 1

    def members_of(self, elem):
        member_id = self.elementsToId[elem]
        if member_id is None:
            raise ValueError("Tried calling membersOf on an unknown element.")

        elem_root = self.find_internal(id)
        ret_val = []
        for idx in range(len(self.elements)):
            other_root = self.find_internal(idx)
            if elem_root == other_root:
                ret_val.append(self.elements[idx])

        return ret_val


def mysql_db_name_hack(target, conn):
    if not isinstance(conn, MySqlConnection) or '.' not in target:
        return target
    else:
        return conn.db_name + '.' + table_name(target)

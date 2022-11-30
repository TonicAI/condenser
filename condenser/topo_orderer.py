from toposort import toposort, toposort_flatten
from condenser import config_reader

def get_topological_order_by_tables(relationships, tables):
    topsort_input =  __prepare_topsort_input(relationships, tables)
    return list(toposort(topsort_input))

def __prepare_topsort_input(relationships, tables):
    dep_breaks = config_reader.get_dependency_breaks()
    deps = dict()
    for r in relationships:
        p =r['fk_table']
        c =r['target_table']

        #break circ dependency
        dep_break_found = False
        for dep_break in dep_breaks:
            if p == dep_break.fk_table and c == dep_break.target_table:
                dep_break_found = True
                break

        if dep_break_found == True:
            continue

        # toposort ignores self circularities for some reason, but we cannot
        if p == c:
            raise ValueError('Circular dependency, {} depends on itself!'.format(p))

        if tables is not None and len(tables) > 0 and (p not in tables or c not in tables):
            continue

        if p in deps:
            deps[p].add(c)
        else:
            deps[p] = set()
            deps[p].add(c)

    return deps

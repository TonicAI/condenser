from toposort import toposort, toposort_flatten
from config_reader import ConfigReader

class TopoOrderer:
    def __init__(self):
        pass


    def get_topological_order_by_tables(self, relationships):
        topsort_input =  self.__prepare_topsort_input(relationships)
        return list(toposort(topsort_input))

    #parent -> child  (outbound reference)
    def get_topological_order_by_relations(self, relationships):
        topsort_input =  self.__prepare_topsort_input(relationships)

        topological_order = list(toposort_flatten(topsort_input))

        dep_breaks = ConfigReader().get_dependency_breaks()
        tables_to_order = ConfigReader().get_all_tables()

        tables_to_delete = list()
        for r in relationships:
            p =r['parent_table_name']
            c =r['child_table_name']

            dep_break_found = False
            for dep_break in dep_breaks:
                if p == dep_break['parent'] and c == dep_break['child']:
                    dep_break_found = True
                    break

            if dep_break_found == True:
                continue

            if tables_to_order is not None and (p not in tables_to_order or c not in tables_to_order):
                continue

            if c in topological_order:
                tables_to_delete.append(r)

        return list(reversed(tables_to_delete))

    def __prepare_topsort_input(self, relationships):
        dep_breaks = ConfigReader().get_dependency_breaks()
        tables_to_order = ConfigReader().get_all_tables()
        deps = dict()
        for r in relationships:
            p =r['parent_table_name']
            c =r['child_table_name']

            #break circ dependency
            dep_break_found = False
            for dep_break in dep_breaks:
                if p == dep_break['parent'] and c == dep_break['child']:
                    dep_break_found = True
                    break

            if dep_break_found == True:
                continue

            if tables_to_order is not None and len(tables_to_order) > 0 and (p not in tables_to_order or c not in tables_to_order):
                continue

            if p in deps:
                deps[p].add(c)
            else:
                deps[p] = set()
                deps[p].add(c)

        return deps

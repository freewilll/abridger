from functools import reduce
from abridger.exc import CyclicDependencyError


class Generator(object):
    def __init__(self, schema, extractor):
        self.schema = schema
        self.extraction_model = extractor.extraction_model
        self.extractor = extractor
        self.make_table_order()
        self.make_deferred_update_rules()
        self.generate_statements()

    def not_null_tables_graph(self, tables):
        graph = {}
        for table in tables:
            graph[table] = set()

        for table in tables:
            for fk in table.foreign_keys:
                if fk.src_cols[0].notnull:
                    graph[table].add(fk.dst_cols[0].table)

        for not_null_col in self.extraction_model.not_null_cols:
            src_table = not_null_col.table
            dst_table = not_null_col.foreign_key.dst_cols[0].table
            graph[src_table].add(dst_table)

        return graph

    def topologically_sort(self, data):
        for k, v in list(data.items()):
            v.discard(k)  # Ignore self dependencies
        extra_items_in_deps = reduce(set.union, list(data.values())) - \
            set(data.keys())

        for item in extra_items_in_deps:
            data[item] = set()

        while True:
            ordered = set(item for item, dep in list(data.items()) if not dep)
            if not ordered:
                break
            yield sorted(ordered)

            new_data = {}
            for item, dep in list(data.items()):
                if item not in ordered:
                    new_data[item] = (dep - ordered)
            data = new_data

        if data:
            raise CyclicDependencyError(
                "A cyclic dependency exists amongst %r" % data)

    def make_table_order(self):
        topologically_sorted = self.topologically_sort(
            self.not_null_tables_graph(self.schema.tables))

        self.table_order = []
        for sublist in topologically_sorted:
            for table in sorted(sublist):
                self.table_order.append(table)

    def make_deferred_update_rules(self):
        extra_table_not_null_cols = set()
        for not_null_col in self.extraction_model.not_null_cols:
            for col in not_null_col.foreign_key.src_cols:
                extra_table_not_null_cols.add(col)

        order_dict = {table: i for i, table in enumerate(self.table_order)}

        self.deferred_update_rules = {}
        for table in self.table_order:
            src_index = order_dict[table]
            columns = set()
            for fk in table.foreign_keys:
                for col in fk.src_cols:
                    dst_index = order_dict[col.table]
                    extra_notnull = col in extra_table_not_null_cols
                    notnull = col.notnull or extra_notnull
                    if not notnull and dst_index >= src_index:
                        columns.add(col)

            self.deferred_update_rules[table] = columns

    def generate_statements(self):
        self.insert_statements = []
        self.update_statements = []
        for table in self.table_order:
            col_indexes = {col: table.cols.index(col) for col in table.cols}
            if table not in self.extractor.results:
                continue

            epk = table.effective_primary_key
            results_rows = self.extractor.results[table][epk]
            for results_row in sorted(results_rows.values()):
                row = results_row.row
                deferred_update_cols = self.deferred_update_rules[table]
                deferred_update_cols = tuple(deferred_update_cols)

                row = list(row)
                final_update_cols = []
                final_update_values = []
                for col in deferred_update_cols:
                    index = col_indexes[col]
                    value = row[index]
                    if value is not None:
                        final_update_cols.append(col)
                        final_update_values.append(value)
                        row[index] = None

                pk_values = []
                for pk_col in epk:
                    pk_values.append(row[col_indexes[pk_col]])

                if len(final_update_cols) > 0:
                    self.update_statements.append((table,
                                                  epk,
                                                  tuple(pk_values),
                                                  tuple(final_update_cols),
                                                  tuple(final_update_values)))

                for i in range(results_row.count):
                    self.insert_statements.append((table, tuple(row)))

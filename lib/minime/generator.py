class Generator(object):
    def __init__(self, schema, rocket):
        self.schema = schema
        self.extraction_model = rocket.extraction_model
        self.results = rocket.results
        self.make_table_order()

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

    # From http://code.activestate.com/recipes/577413-topological-sort/
    def topologically_sort(self, data):
        for k, v in data.items():
            v.discard(k)  # Ignore self dependencies
        extra_items_in_deps = reduce(set.union, data.values()) - \
            set(data.keys())

        for item in extra_items_in_deps:
            data[item] = set()

        while True:
            ordered = set(item for item, dep in data.items() if not dep)
            if not ordered:
                break
            yield sorted(ordered)

            new_data = {}
            for item, dep in data.items():
                if item not in ordered:
                    new_data[item] = (dep - ordered)
            data = new_data

        assert not data, "A cyclic dependency exists amongst %r" % data

    def make_table_order(self):
        topologically_sorted = self.topologically_sort(
            self.not_null_tables_graph(self.schema.tables))

        self.table_order = []
        for sublist in topologically_sorted:
            for table in sorted(sublist):
                self.table_order.append(table)

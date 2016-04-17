class ForeignKeyConstraint(object):
    def __init__(self, name, src_col, dst_col):
        self.name = name
        self.src_col = src_col
        self.dst_col = dst_col

    @staticmethod
    def create_and_add_to_tables(name, src_table, src_col, dst_table, dst_col):
        fkc = ForeignKeyConstraint(name, src_col, dst_col)
        src_table.fks.append(fkc)
        src_table.fks_by_col[src_col] = fkc
        dst_table.incoming_fks.append(fkc)
        return fkc

    def __str__(self):
        return '%s: %s:%s -> %s:%s' % (
            self.name,
            self.src_col.table.name, self.src_col.name,
            self.dst_col.table.name, self.dst_col.name)


class Column(object):
    def __init__(self, table, name, notnull):
        self.table = table
        self.name = name
        self.notnull = notnull

    def __repr__(self):
        return '<Column %s.%s>' % (self.table.name, self.name)

    def __str__(self):
        return '%s.%s' % (self.table.name, self.name)


class UniqueIndex(object):
    def __init__(self, name, cols):
        self.name = name
        self.cols = cols

    @staticmethod
    def create_and_add_to_table(table, name, cols):
        ui = UniqueIndex(name, cols)
        table.unique_indexes.append(ui)
        return ui

    def __str__(self):
        return self.name

    def __repr__(self):
        cols_str = ','.join(sorted([c.name for c in list(self.cols)]))
        return '<UniqueIndex %s on (%s)>' % (self.name, cols_str)


class Table(object):
    def __init__(self, name,):
        self.name = name
        self.cols = []
        self.cols_by_name = {}
        self.primary_key = None
        self.fks = []
        self.fks_by_col = {}
        self.incoming_fks = []
        self.unique_indexes = []

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<Table %s>' % self.name

    def add_column(self, name, notnull):
        col = Column(self, name, notnull)
        self.cols.append(col)
        self.cols_by_name[name] = col
        return col


class Schema(object):
    def __init__(self):
        self.tables = []
        self.tables_by_name = {}

    def relations(self):
        results = []
        for table in self.tables:
            for fkc in table.fks:
                results.append({
                    'name': fkc.name,
                    'table': table.name,
                    'column': fkc.src_col.name,
                })
        return results

    def dump_relations(self, f):
        f.write('relations:\n')
        for relation in self.relations():
            f.write('  - table: %s\n' % relation['table'])
            f.write('    column: %s\n' % relation['column'])
            f.write('    name: %s\n' % relation['name'])

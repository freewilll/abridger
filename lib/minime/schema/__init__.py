class ForeignKeyConstraint(object):
    def __init__(self, name, src_cols, dst_cols):
        # Check table is the same within the src/dst columns & len matches
        assert len(set([c.table.name for c in src_cols])) == 1
        assert len(set([c.table.name for c in dst_cols])) == 1
        assert len(src_cols) == len(dst_cols)

        self.name = name
        self.src_cols = src_cols
        self.dst_cols = dst_cols

    @staticmethod
    def create_and_add_to_tables(name, src_cols, dst_cols):
        fk = ForeignKeyConstraint(name, src_cols, dst_cols)
        src_table = src_cols[0].table
        dst_table = dst_cols[0].table
        src_table.foreign_keys.append(fk)
        dst_table.incoming_foreign_keys.append(fk)
        return fk

    def __str__(self):
        src_table = self.src_cols[0].table
        dst_table = self.dst_cols[0].table
        src_cols_csv = ','.join(sorted([str(c) for c in self.src_cols]))
        dst_cols_csv = ','.join(sorted([str(c) for c in self.dst_cols]))
        return '%s: %s:(%s) -> %s:(%s)' % (
            self.name,
            src_table, src_cols_csv,
            dst_table, dst_cols_csv)

    def __repr__(self):
        return '<ForeignKeyConstraint %s>' % str(self)


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
        self.foreign_keys = []
        self.incoming_foreign_keys = []
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

    def _relation_fk_col_or_cols(self, fk):
        if len(fk.src_cols) == 1:
            return {'column': fk.src_cols[0].name}
        else:
            return {'columns':  [c.name for c in fk.src_cols]}

    def relations(self):
        results = []
        for table in self.tables:
            for fk in table.foreign_keys:
                relation = {
                    'name': fk.name,
                    'table': table.name,
                }
                relation.update(self._relation_fk_col_or_cols(fk))
                results.append(relation)
        return results

    def dump_relations(self, f):
        f.write('relations:\n')
        for relation in self.relations():
            f.write('  - table: %s\n' % relation['table'])
            if 'column' in relation:
                f.write('    column: %s\n' % relation['column'])
            else:
                f.write('    columns:\n')
                for col in sorted(relation['columns']):
                    f.write('    - %s\n' % col)
            f.write('    name: %s\n' % relation['name'])

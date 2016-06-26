from abridger.exc import RelationIntegrityError
from abridger.extraction_model import Relation


class ForeignKeyConstraint(object):
    def __init__(self, name, src_cols, dst_cols):
        # Check table is the same within the src/dst columns & len matches
        assert len(set([c.table.name for c in src_cols])) == 1
        assert len(set([c.table.name for c in dst_cols])) == 1
        assert len(src_cols) == len(dst_cols)

        self.name = name
        self.src_cols = src_cols
        self.dst_cols = dst_cols
        self.notnull = all([s.notnull for s in src_cols])

        if self.notnull and self.src_cols[0].table == self.dst_cols[0].table:
            raise RelationIntegrityError(
                'Table %s has a self referencing not null foreign key. ' +
                'This could lead to situations where no data can be added ' +
                'without disabling foreign key constraints.')

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
        return '%s: %s:(%s) -> %s:(%s) %s' % (
            self.name,
            src_table, src_cols_csv,
            dst_table, dst_cols_csv,
            'not null' if self.notnull else 'nullable')

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

    def __lt__(self, other):
        return self.name < other.name

    def add_column(self, name, notnull):
        col = Column(self, name, notnull)
        self.cols.append(col)
        self.cols_by_name[name] = col
        return col

    def _set_effective_primary_key(self):
        if self.primary_key is not None:
            self.effective_primary_key = self.primary_key
            self.can_have_duplicated_rows = False
        elif self.alternate_primary_key is not None:
            self.effective_primary_key = self.alternate_primary_key
            self.can_have_duplicated_rows = False
        else:
            self.effective_primary_key = tuple(self.cols)
            self.can_have_duplicated_rows = True

        self.effective_primary_key_col_indexes = \
            [self.cols.index(c) for c in self.effective_primary_key]


class Schema(object):
    def __init__(self):
        self.tables = []
        self.tables_by_name = {}

    def _relation_fk_col_or_cols(self, fk):
        if len(fk.src_cols) == 1:
            return {'column': fk.src_cols[0].name}
        else:
            return {'columns':  [c.name for c in fk.src_cols]}

    def _add_alternate_primary_keys(self):
        for table in self.tables:
            table.alternate_primary_key = None
            if table.primary_key is not None:
                continue

            for unique_index in table.unique_indexes:
                pk_len = len(table.alternate_primary_key or [])
                ui_len = len(unique_index.cols)
                if table.alternate_primary_key is None or ui_len < pk_len:
                    table.alternate_primary_key = tuple(unique_index.cols)

        for table in self.tables:
            table._set_effective_primary_key()

    def relations(self):
        results = []
        for table in self.tables:
            for fk in table.foreign_keys:
                relation = {
                    'name': fk.name,
                    'table': table.name,
                    'type': Relation.TYPE_INCOMING,
                }
                relation.update(self._relation_fk_col_or_cols(fk))
                results.append(relation)

                relation = dict(relation)
                relation['type'] = Relation.TYPE_OUTGOING
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
            if relation['type'] == Relation.TYPE_INCOMING:
                f.write('    type: %s\n' % Relation.TYPE_INCOMING)
            f.write('    name: %s\n' % relation['name'])

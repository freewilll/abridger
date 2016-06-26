from collections import defaultdict
import re

from .base import Schema, Table, ForeignKeyConstraint, UniqueIndex
from abridger.exc import UnknownTableError, UnknownColumnError


class SqliteSchema(Schema):
    @classmethod
    def create_from_conn(cls, conn):
        schema = cls()
        schema._add_tables_from_conn(conn)
        schema._add_columns_from_conn(conn)
        schema._add_foreign_key_constraints_from_conn(conn)
        schema._add_unique_indexes(conn)
        schema._add_alternate_primary_keys()
        return schema

    def _add_table(self, name):
        table = Table(name)
        self.tables.append(table)
        self.tables_by_name[name] = table
        return table

    def _add_tables_from_conn(self, conn):
        stmt = '''
            SELECT name FROM sqlite_master
            WHERE type='table' ORDER BY name
        '''

        rs = conn.execute(stmt)
        for (name,) in rs:
            self._add_table(name)

    def _add_columns_from_conn(self, conn):
        for table in self.tables:
            stmt = "PRAGMA table_info('%s')" % table.name
            rs = conn.execute(stmt)

            primary_key = list()
            for row in rs:
                (name, notnull, primary_key_index) = (
                    row[1], bool(row[3]), row[5])
                col = table.add_column(name, notnull)
                if primary_key_index > 0:
                    primary_key.append(col)

            if len(primary_key) > 0:
                table.primary_key = tuple(primary_key)
            else:
                table.primary_key = None

    def _add_foreign_key_constraints_from_conn(self, conn):
        for src_table in self.tables:
            stmt = "PRAGMA foreign_key_list('%s')" % src_table.name
            rs = conn.execute(stmt)
            foreign_keys = defaultdict(list)
            fks = {}
            for row in rs:
                (fk_index, dst_table_name, src_col_name,
                 dst_col_name) = (row[0], row[2], row[3], row[4])
                foreign_keys[fk_index].append((dst_table_name,
                                               src_col_name, dst_col_name))

            for fk_index, fk_cols in list(foreign_keys.items()):
                dst_table_names = set()
                src_cols = []
                dst_cols = []
                src_col_names = []
                dst_col_names = []
                for i, (dst_table_name, src_col_name, dst_col_name) in \
                        enumerate(fk_cols):
                    dst_table = self.tables_by_name.get(dst_table_name)
                    if dst_table is None:
                        raise UnknownTableError(
                            'Unknown table "%s" in foreign key '
                            'constraint on table "%s", column "%s"' % (
                                dst_table_name, src_table, src_col_name))

                    src_col = src_table.cols_by_name[src_col_name]

                    if dst_col_name is None:
                        dst_col = list(dst_table.primary_key)[i]
                    else:
                        dst_col = dst_table.cols_by_name.get(dst_col_name)
                        if dst_col is None:
                            raise UnknownColumnError(
                                'Unknown column "%s" on table "%s"' % (
                                    dst_table.name, dst_col_name))

                    dst_table_names.add(dst_table_name)
                    src_cols.append(src_col)
                    dst_cols.append(dst_col)
                    src_col_names.append(src_col_name)
                    dst_col_names.append(dst_col_name)

                fk = ForeignKeyConstraint.create_and_add_to_tables(
                    None, tuple(src_cols), tuple(dst_cols))
                fks[(tuple(src_col_names), tuple(dst_col_names))] = fk

                assert len(dst_table_names) == 1

            # Find constraint names by parsing the schema SQL
            stmt = '''
                SELECT sql FROM sqlite_master WHERE name = '%s'
                AND type = 'table'
            ''' % src_table.name
            rs = conn.execute(stmt)
            table_sql = rs.fetchone()[0]

            fk_pattern = (
                '(?:CONSTRAINT (\w+) +)?'
                'FOREIGN KEY *\( *(.+?) *\) +'
                'REFERENCES +(?:(?:"(.+?)")|([a-z0-9_]+)) *(?:\((.+?)\))?'
            )

            for match in re.finditer(fk_pattern, table_sql, re.I):
                (name, src_col_name, dst_table_quoted, dst_table_unquoted,
                 dst_col_name) = match.group(1, 2, 3, 4, 5)

                dst_table_name = dst_table_quoted or dst_table_unquoted
                if name is not None:
                    fks[(tuple(src_col_names), tuple(dst_col_names))] = fk
                    t = tuple([(src_col_name,), (dst_col_name,)])
                    if t in fks:
                        fks[t].name = name

    def _add_unique_indexes(self, conn):
        for table in self.tables:
            stmt = "PRAGMA index_list('%s')" % table.name
            rs = conn.execute(stmt)
            for row in rs:
                (index_name, is_unique) = (row[1], row[2])
                if not is_unique:
                    continue

                stmt = "PRAGMA index_info('%s')" % index_name
                rs = conn.execute(stmt)
                columns = set()
                for row in rs:
                    column_name = row[2]
                    columns.add(table.cols_by_name[column_name])
                UniqueIndex.create_and_add_to_table(table, index_name, columns)

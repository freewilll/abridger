import re

from . import Schema, Table, ForeignKeyConstraint


class SqliteSchema(Schema):
    @classmethod
    def create_from_conn(cls, conn):
        schema = cls()
        schema.add_tables_from_conn(conn)
        schema.add_columns_from_conn(conn)
        schema.add_foreign_key_constraints_from_conn(conn)
        return schema

    def add_table(self, name):
        table = Table(name)
        self.tables.append(table)
        self.tables_by_name[name] = table
        return table

    def add_tables_from_conn(self, conn):
        sql = '''
            SELECT name FROM sqlite_master
            WHERE type='table' ORDER BY name
        '''

        rs = conn.execute(sql)
        for (name,) in rs:
            table = self.add_table(name)
            assert str(table) is not None
            assert repr(table) is not None

    def add_columns_from_conn(self, conn):
        for table in self.tables:
            sql = "PRAGMA table_info('%s')" % table.name
            rs = conn.execute(sql)

            primary_key = set()
            for row in rs:
                (name, notnull, primary_key_index) = (
                    row[1], bool(row[3]), row[5])
                col = table.add_column(name, notnull)
                if primary_key_index > 0:
                    primary_key.add(col)

            table.primary_key = None if len(primary_key) == 0 else primary_key

    def add_foreign_key_constraints_from_conn(self, conn):
        def fkc_tuple(src_table, dst_table_name, src_col_name, dst_col_name):
            dst_table = self.tables_by_name[dst_table_name]
            src_col = src_table.cols_by_name[src_col_name]

            if dst_col_name is None:
                # Composite foreign keys aren't supported
                dst_col = list(dst_table.primary_key)[0]
            else:
                dst_col = dst_table.cols_by_name[dst_col_name]

            return (src_table, src_col, dst_table, dst_col)

        for src_table in self.tables:
            fkcs = {}
            sql = "PRAGMA foreign_key_list('%s')" % src_table.name
            rs = conn.execute(sql)
            for row in rs:
                (dst_table_name, src_col_name, dst_col_name) = (
                    row[2], row[3], row[4])
                t = fkc_tuple(src_table, dst_table_name, src_col_name,
                              dst_col_name)
                fkc = ForeignKeyConstraint.create_and_add_to_tables(
                    None, *t)
                fkcs[t] = fkc

            # Find constraint names by parsing the schema SQL
            sql = '''
                SELECT sql FROM sqlite_master WHERE name = '%s'
                AND type = 'table'
            ''' % src_table.name
            rs = conn.execute(sql)
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
                    t = fkc_tuple(src_table, dst_table_name, src_col_name,
                                  dst_col_name)
                    if t in fkcs:
                        fkcs[t].name = name

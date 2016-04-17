import re

from . import Schema, Table, ForeignKeyConstraint, UniqueIndex


class SqliteSchema(Schema):
    @classmethod
    def create_from_conn(cls, conn):
        schema = cls()
        schema.add_tables_from_conn(conn)
        schema.add_columns_from_conn(conn)
        schema.add_foreign_key_constraints_from_conn(conn)
        schema.add_unique_indexes(conn)
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
            self.add_table(name)

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
            dst_table = self.tables_by_name.get(dst_table_name)
            if dst_table is None:
                raise Exception('Unknown table "%s"in foreign key '
                                'constraint on table "%s", column "%s"' % (
                                    dst_table_name, src_table, src_col_name))

            src_col = src_table.cols_by_name[src_col_name]

            if dst_col_name is None:
                # Composite foreign keys aren't supported
                dst_col = list(dst_table.primary_key)[0]
            else:
                dst_col = dst_table.cols_by_name.get(dst_col_name)
                if dst_col is None:
                    raise Exception('Unknown column "%s" on table "%s"' % (
                        dst_table.name, dst_col_name))

            return (src_table, src_col, dst_table, dst_col)

        for src_table in self.tables:
            fkcs = {}
            sql = "PRAGMA foreign_key_list('%s')" % src_table.name
            rs = conn.execute(sql)
            foreign_keys = set()
            for row in rs:
                (fk_col_index, dst_table_name, src_col_name,
                 dst_col_name) = (row[1], row[2], row[3], row[4])
                t = fkc_tuple(src_table, dst_table_name, src_col_name,
                              dst_col_name)
                fkc = ForeignKeyConstraint.create_and_add_to_tables(
                    None, *t)
                fkcs[t] = fkc
                foreign_keys.add(fk_col_index)

            if len(foreign_keys) > 1:
                raise Exception(
                    'Compound foreign keys are not supported '
                    'on table "%s"' % src_table.name)

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

    def add_unique_indexes(self, conn):
        for table in self.tables:
            sql = "PRAGMA index_list('%s')" % table.name
            rs = conn.execute(sql)
            for row in rs:
                (index_name, is_unique) = (row[1], row[2])
                if not is_unique:
                    continue

                sql = "PRAGMA index_info('%s')" % index_name
                rs = conn.execute(sql)
                columns = set()
                for row in rs:
                    column_name = row[2]
                    columns.add(table.cols_by_name[column_name])
                UniqueIndex.create_and_add_to_table(table, index_name, columns)

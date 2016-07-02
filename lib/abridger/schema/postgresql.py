from collections import defaultdict

from .base import Schema, Table, Column, ForeignKeyConstraint, UniqueIndex


class PostgresqlColumn(Column):
    def __init__(self, table, name, notnull, attrnum):
        super(PostgresqlColumn, self).__init__(table, name, notnull)
        self.attrnum = attrnum


class PostgresqlTable(Table):
    def __init__(self, name, oid):
        super(PostgresqlTable, self).__init__(name)
        self.cols_by_attrnum = {}

    def add_column(self, name, notnull, attrnum):
        col = PostgresqlColumn(self, name, notnull, attrnum)
        self.cols.append(col)
        self.cols_by_name[name] = col
        self.cols_by_attrnum[attrnum] = col
        return col


class PostgresqlSchema(Schema):
    @classmethod
    def create_from_conn(cls, conn):
        schema = cls()
        schema.tables_by_oid = {}

        schema._add_tables_from_conn(conn)
        schema._add_columns_from_conn(conn)
        schema._add_foreign_key_constraints_from_conn(conn)
        schema._add_primary_key_constraints(conn)
        schema._add_unique_indexes(conn)
        schema._add_alternate_primary_keys()
        return schema

    def _add_table(self, name, oid):
        table = PostgresqlTable(name, oid)
        self.tables.append(table)
        self.tables_by_name[name] = table
        self.tables_by_oid[oid] = table
        return table

    def _add_tables_from_conn(self, conn):
        stmt = '''
            SELECT pg_class.oid, relname
            FROM pg_class
            LEFT JOIN pg_namespace ON (relnamespace = pg_namespace.oid)
            LEFT JOIN pg_description ON
                (pg_class.oid = pg_description.objoid AND
                pg_description.objsubid = 0)
            WHERE relkind = 'r' AND
                nspname not in ('information_schema', 'pg_catalog')
            ORDER BY relname
        '''

        cur = conn.cursor()
        cur.execute(stmt)
        for (oid, name) in cur.fetchall():
            self._add_table(name, oid)
        cur.close()

    def _add_columns_from_conn(self, conn):
        stmt = '''
            SELECT pg_class.oid, attname, attnum, attnotnull
            FROM pg_class
              LEFT JOIN pg_namespace ON (relnamespace = pg_namespace.oid)
              LEFT JOIN pg_attribute ON (pg_class.oid=attrelid)
              LEFT JOIN pg_type ON (atttypid=pg_type.oid)
              LEFT JOIN pg_description ON
                (pg_class.oid = pg_description.objoid AND
                pg_description.objsubid = attnum)
            WHERE relkind = 'r' AND
                nspname not in ('information_schema', 'pg_catalog') AND
                typname IS NOT NULL AND
                attnum > 0
            ORDER BY relname, attnum
        '''

        cur = conn.cursor()
        cur.execute(stmt)
        for (oid, name, attrnum, notnull) in cur.fetchall():
            table = self.tables_by_oid[oid]
            table.add_column(name, notnull, attrnum)
        cur.close()

    def _add_foreign_key_constraints_from_conn(self, conn):
        stmt = '''
            SELECT conname, base_table.oid, conkey, ref_table.oid, confkey
            FROM pg_class AS base_table
                INNER JOIN pg_constraint ON (base_table.oid = conrelid)
                LEFT JOIN pg_class AS ref_table ON (confrelid = ref_table.oid)
            WHERE contype = 'f'
        '''

        cur = conn.cursor()
        cur.execute(stmt)
        for (name, src_oid, src_attrnum, dst_oid, dst_attrnum) \
                in cur.fetchall():
            src_table = self.tables_by_oid[src_oid]
            dst_table = self.tables_by_oid[dst_oid]

            assert type(src_attrnum) is list
            assert type(dst_attrnum) is list
            assert len(src_attrnum) == len(dst_attrnum)

            src_cols = []
            dst_cols = []
            for i in range(0, len(src_attrnum)):
                src_cols.append(src_table.cols_by_attrnum[src_attrnum[i]])
                dst_cols.append(dst_table.cols_by_attrnum[dst_attrnum[i]])

            ForeignKeyConstraint.create_and_add_to_tables(
                name, tuple(src_cols), tuple(dst_cols))
        cur.close()

    def _add_primary_key_constraints(self, conn):
        stmt = '''
            SELECT conname, base_table.oid, conkey
            FROM pg_class AS base_table
                INNER JOIN pg_constraint ON (base_table.oid = conrelid)
            WHERE contype = 'p'
        '''

        cur = conn.cursor()
        cur.execute(stmt)
        for (name, oid, attrnums) in cur.fetchall():
            table = self.tables_by_oid[oid]
            assert type(attrnums) is list
            primary_key = list()
            for attrnum in attrnums:
                primary_key.append(table.cols_by_attrnum[attrnum])
            table.primary_key = tuple(primary_key)

        cur.close()

    def _add_unique_indexes(self, conn):
        stmt = '''
            SELECT c1.oid, c2.relname, i.indisunique, i.indkey
            FROM pg_class c1
            JOIN pg_index i on c1.oid = i.indrelid
            JOIN pg_class c2 on c2.oid = i.indexrelid
            LEFT JOIN pg_constraint c ON (
                i.indrelid = c.conrelid AND
                i.indexrelid = c.conindid AND
                c.contype in ('p', 'u'))
            WHERE c1.relkind = 'r'
        '''

        cur = conn.cursor()
        cur.execute(stmt)
        unique_indexes = defaultdict(dict)
        for row in cur.fetchall():
            (table_oid, name, is_unique, attrs) = (
                row[0], row[1], bool(row[2]), row[3])
            if not is_unique:
                continue

            if table_oid not in self.tables_by_oid:
                continue

            table = self.tables_by_oid[table_oid]
            col_attrs = [int(a.strip()) for a in attrs.split()]

            # Ignore indexes that have an expression on one of the columns
            if 0 in col_attrs:
                continue

            cols = set([table.cols_by_attrnum[a] for a in col_attrs])
            unique_indexes[table][name] = cols

        cur.close()

        for table in unique_indexes:
            for name in unique_indexes[table]:
                cols = unique_indexes[table][name]
                UniqueIndex.create_and_add_to_table(table, name, cols)

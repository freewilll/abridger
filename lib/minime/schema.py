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


class Table(object):
    def __init__(self, name, oid):
        self.name = name
        self.oid = oid
        self.cols = []
        self.cols_by_name = {}
        self.cols_by_attrnum = {}
        self.pk = None
        self.fks = []
        self.fks_by_col = {}
        self.incoming_fks = []

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<Table %s>' % self.name

    def add_column(self, name, attrnum, notnull):
        col = Column(self, name, attrnum, notnull)
        self.cols.append(col)
        self.cols_by_name[name] = col
        self.cols_by_attrnum[attrnum] = col
        return col


class Column(object):
    def __init__(self, table, name, attrnum, notnull):
        self.table = table
        self.name = name
        self.attrnum = attrnum
        self.notnull = notnull


class Schema(object):
    def __init__(self):
        self.tables = []
        self.tables_by_name = {}
        self.tables_by_oid = {}

    @staticmethod
    def create_from_conn(conn):
        schema = Schema()
        schema.add_tables_from_conn(conn)
        schema.add_columns_from_conn(conn)
        schema.add_foreign_key_constraints_from_conn(conn)
        schema.add_primary_key_constraints(conn)
        return schema

    def add_table(self, name, oid):
        table = Table(name, oid)
        self.tables.append(table)
        self.tables_by_name[name] = table
        self.tables_by_oid[oid] = table
        return table

    def add_tables_from_conn(self, conn):
        sql = '''
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

        with conn.cursor() as cur:
            cur.execute(sql)
            for (oid, name) in cur.fetchall():
                self.add_table(name, oid)

    def add_columns_from_conn(self, conn):
        sql = '''
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

        with conn.cursor() as cur:
            cur.execute(sql)
            for (oid, name, attrnum, notnull) in cur.fetchall():
                table = self.tables_by_oid[oid]
                table.add_column(name, attrnum, notnull)

    def add_foreign_key_constraints_from_conn(self, conn):
        sql = '''
            SELECT conname, base_table.oid, conkey, ref_table.oid, confkey
            FROM pg_class AS base_table
                INNER JOIN pg_constraint ON (base_table.oid = conrelid)
                LEFT JOIN pg_class AS ref_table ON (confrelid = ref_table.oid)
            WHERE contype = 'f'
        '''

        with conn.cursor() as cur:
            cur.execute(sql)
            for (name, src_oid, src_attrnum, dst_oid, dst_attrnum) \
                    in cur.fetchall():
                src_table = self.tables_by_oid[src_oid]
                dst_table = self.tables_by_oid[dst_oid]

                assert type(src_attrnum) is list
                assert type(dst_attrnum) is list
                if len(src_attrnum) != 1 or len(dst_attrnum) != 1:
                    raise Exception(
                        'Composite foreign keys are not supported '
                        'on table "%s"' % src_table.name)

                src_col = src_table.cols_by_attrnum[src_attrnum[0]]
                dst_col = dst_table.cols_by_attrnum[dst_attrnum[0]]

                ForeignKeyConstraint.create_and_add_to_tables(
                    name, src_table, src_col, dst_table, dst_col)

    def add_primary_key_constraints(self, conn):
        sql = '''
            SELECT conname, base_table.oid, conkey
            FROM pg_class AS base_table
                INNER JOIN pg_constraint ON (base_table.oid = conrelid)
            WHERE contype = 'p'
        '''

        with conn.cursor() as cur:
            cur.execute(sql)
            for (name, oid, attrnum) in cur.fetchall():
                table = self.tables_by_oid[oid]
                assert type(attrnum) is list
                if len(attrnum) != 1:
                    raise Exception(
                        'Composite primary keys are not supported '
                        'on table "%s"' % table.name)

                col = table.cols_by_attrnum[attrnum[0]]
                table.pk = col

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

class DbConn(object):
    def connect(self, input):  # pragma: no cover
        return

    def create_schema(self, schema_cls):
        self.schema = schema_cls.create_from_conn(self.connection)

    def disconnect(self):
        if self.connection is not None:
            self.connection.close()
        self.connection = None

    def execute(self, *args, **kwargs):
        cursor = self.connection.cursor()
        cursor.execute(*args, **kwargs)

    def execute_and_fetchall(self, *args, **kwargs):
        cursor = self.connection.cursor()
        cursor.execute(*args, **kwargs)
        return cursor.fetchall()

    def fetch_rows(self, table, cols, values):
        phs = self.placeholder_symbol
        cols_csv = ', '.join([c.name for c in table.cols])
        sql = 'SELECT %s FROM %s' % (cols_csv, table.name)

        if cols is None:
            sql = 'SELECT %s FROM %s' % (cols_csv, table.name)
            values = ()
        elif len(cols) == 1:
            ph_with_comma = '%s, ' % phs
            q = ph_with_comma.join([''] * len(values)) + phs
            sql += ' WHERE %s IN (%s)' % (cols[0].name, q)
            values = [v[0] for v in values]
        else:
            raise Exception('TODO: multi col where')  # pragma: no cover

        return list(self.execute_and_fetchall(sql, values))

    def insert_rows(self, rows, cursor=None):
        if cursor is None:
            cursor = self.connection.cursor()

        table_cols = {}
        phs = self.placeholder_symbol

        for (table, values) in rows:
            if table not in table_cols:
                cols_csv = ', '.join([c.name for c in table.cols])
                ph_with_comma = '%s, ' % phs
                q = ph_with_comma.join([''] * len(table.cols)) + \
                    phs
                table_cols[table] = (cols_csv, q)
            else:
                (cols_csv, q) = table_cols[table]

            sql = 'INSERT INTO %s (%s) VALUES(%s)' % (table.name, cols_csv, q)
            self.execute(sql, values)

    def update_rows(self, rows, cursor=None):
        if cursor is None:
            cursor = self.connection.cursor()

        table_cols = {}
        phs = self.placeholder_symbol

        def get_col_names(table, cols):
            if (table, cols) not in table_cols:
                col_names = [c.name for c in cols]
                table_cols[(table, cols)] = col_names
            else:
                col_names = table_cols[(table, cols)]
            return col_names

        for (table, pk_cols, pk_values, value_cols, values) in rows:
            assert len(pk_cols) > 0

            value_col_names = get_col_names(table, value_cols)
            pk_col_names = get_col_names(table, pk_cols)

            placeholder_values = []
            sets = []
            where = []
            for i, col_name in enumerate(value_col_names):
                value = values[i]
                assert value is not None
                sets.append("%s=%s" % (col_name, phs))
                placeholder_values.append(value)

            for i, col_name in enumerate(pk_col_names):
                pk_value = pk_values[i]
                assert pk_value is not None
                where.append("%s=%s" % (col_name, phs))
                placeholder_values.append(pk_value)

            sql = 'UPDATE %s SET %s WHERE %s' % (
                table.name,
                ', '.join(sets),
                ' AND '.join(where))
            self.execute(sql, placeholder_values)

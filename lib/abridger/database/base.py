class Database(object):
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
        if values is not None and len(values) == 0:
            return []

        phs = self.placeholder_symbol
        cols_csv = ', '.join([c.name for c in table.cols])
        stmt = 'SELECT %s FROM %s' % (cols_csv, table.name)

        if cols is None:
            stmt = 'SELECT %s FROM %s' % (cols_csv, table.name)
            stmt_values = ()
        elif len(cols) == 1:
            ph_with_comma = '%s, ' % phs
            q = ph_with_comma.join([''] * len(values)) + phs
            stmt += ' WHERE %s IN (%s)' % (cols[0].name, q)
            stmt_values = [v[0] for v in values]
        else:
            (where_clause, stmt_values) = self.make_multi_col_where_clause(
                table, cols, values)
            stmt += ' WHERE ' + where_clause

        return list(self.execute_and_fetchall(stmt, stmt_values))

    def make_multi_col_where_clause(self, table, cols, values):
        # Produce something like
        # (col1=%s AND col2=%s) OR (col1=%s AND col2=%s) ...
        # This function should be written by databases that can produce
        # more efficient SQL, like e.g. postgresql.

        phs = self.placeholder_symbol
        stmt_values = []
        or_clauses = []

        for value_tuple in values:
            where_clause = ''
            for i, (col, val) in enumerate(zip(cols, value_tuple)):
                if i > 0:
                    where_clause += ' AND '
                where_clause += '%s=%s' % (col.name, phs)
                stmt_values.append(val)
            or_clauses.append('(%s)' % where_clause)

        return ' OR '.join(or_clauses), stmt_values

    def make_insert_statement(self, row, placeholder_symbol=None):
        table_cols = {}
        phs = placeholder_symbol or self.placeholder_symbol

        (table, values) = row
        if table not in table_cols:
            cols_csv = ', '.join([c.name for c in table.cols])
            ph_with_comma = '%s, ' % phs
            q = ph_with_comma.join([''] * len(table.cols)) + \
                phs
            table_cols[table] = (cols_csv, q)
        else:
            (cols_csv, q) = table_cols[table]

        stmt = 'INSERT INTO %s (%s) VALUES(%s)' % (table.name, cols_csv, q)
        return stmt, values

    def insert_rows(self, rows, cursor=None):
        if cursor is None:
            cursor = self.connection.cursor()
        for row in rows:
            (stmt, values) = self.make_insert_statement(row)
            self.execute(stmt, values)

    def make_update_statement(self, row, placeholder_symbol=None):
        table_cols = {}
        phs = placeholder_symbol or self.placeholder_symbol

        def get_col_names(table, cols):
            if (table, cols) not in table_cols:
                col_names = [c.name for c in cols]
                table_cols[(table, cols)] = col_names
            else:
                col_names = table_cols[(table, cols)]
            return col_names

        (table, pk_cols, pk_values, value_cols, values) = row
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

        stmt = 'UPDATE %s SET %s WHERE %s' % (
            table.name,
            ', '.join(sets),
            ' AND '.join(where))
        return stmt, placeholder_values

    def update_rows(self, rows, cursor=None):
        if cursor is None:
            cursor = self.connection.cursor()
        for row in rows:
            (stmt, values) = self.make_update_statement(row)
            self.execute(stmt, values)

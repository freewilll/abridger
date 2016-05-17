import sqlite3
from . import DbConn


class SqliteDbConn(DbConn):
    def __init__(self, path):
        self.path = path

    def connect(self):
        self.connection = sqlite3.connect(self.path)
        self.connection.execute('pragma foreign_keys=ON')
        return self.connection

    def execute(self, *args, **kwargs):
        return self.connection.execute(*args, **kwargs)

    def fetch_rows(self, table, cols, values):
        cols_csv = ', '.join([c.name for c in table.cols])
        sql = 'SELECT %s FROM %s' % (cols_csv, table.name)

        if cols is None:
            sql = 'SELECT %s FROM %s' % (cols_csv, table.name)
            values = ()
        elif len(cols) == 1:
            q = '?, '.join([''] * len(values)) + '?'
            sql += ' WHERE %s IN (%s)' % (cols[0].name, q)
            values = [v[0] for v in values]
        else:
            raise Exception('TODO: multi col where on sqlite')

        return list(self.connection.execute(sql, values))

    def insert_rows(self, rows):
        table_cols = {}

        for (table, values) in rows:
            if table not in table_cols:
                cols_csv = ', '.join([c.name for c in table.cols])
                q = '?, '.join([''] * len(table.cols)) + '?'
                table_cols[table] = (cols_csv, q)
            else:
                (cols_csv, q) = table_cols[table]

            sql = 'INSERT INTO %s (%s) VALUES(%s)' % (table.name, cols_csv, q)
            self.connection.execute(sql, values)

    def update_rows(self, rows):
        table_cols = {}

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
                sets.append("%s=?" % col_name)
                placeholder_values.append(value)

            for i, col_name in enumerate(pk_col_names):
                pk_value = pk_values[i]
                assert pk_value is not None
                where.append("%s=?" % col_name)
                placeholder_values.append(pk_value)

            sql = 'UPDATE %s SET %s WHERE %s' % (
                table.name,
                ', '.join(sets),
                ' AND '.join(where))
            self.connection.execute(sql, placeholder_values)

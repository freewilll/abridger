import sqlite3
from . import DbConn


class SqliteDbConn(DbConn):
    def __init__(self, path):
        self.path = path

    def connect(self):
        self.connection = sqlite3.connect(self.path)
        return self.connection

    def execute(self, *args, **kwargs):
        return self.connection.execute(*args, **kwargs)

    def fetch_rows(self, table, column, values):
        cols_csv = ', '.join([c.name for c in table.cols])
        sql = 'SELECT %s FROM %s' % (cols_csv, table.name)

        if column is not None:
            q = '?, '.join([''] * len(values)) + '?'
            sql += ' WHERE %s IN (%s)' % (column.name, q)
        else:
            values = ()

        return self.connection.execute(sql, values)

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

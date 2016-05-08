import psycopg2
from . import DbConn


class PostgresqlDbConn(DbConn):
    def __init__(self, host=None, port=None, dbname=None, user=None,
                 password=None):
        if dbname is None:
            raise Exception('dbname must have a value')
        if user is None:
            raise Exception('user must have a value')

        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password

    def connect(self):
        self.connection = psycopg2.connect(
            database=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port)
        return self.connection

    def execute(self, *args, **kwargs):
        with self.connection.cursor() as cur:
            cur.execute(*args, **kwargs)
            return cur.fetchall()

    def fetch_rows(self, table, cols, values):
        cols_csv = ', '.join([c.name for c in table.cols])
        sql = 'SELECT %s FROM %s' % (cols_csv, table.name)

        if cols is None:
            sql = 'SELECT %s FROM %s' % (cols_csv, table.name)
            values = ()
        elif len(cols) == 1:
            q = '%s, '.join([''] * len(values)) + '%s'
            sql += ' WHERE %s IN (%s)' % (cols[0].name, q)
            values = [v[0] for v in values]
        else:
            raise Exception('TODO: multi col where on postgresql')
        return list(self.execute(sql, values))

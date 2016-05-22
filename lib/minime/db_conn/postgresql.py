import psycopg2

from base import DbConn
from minime.schema import PostgresqlSchema


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
        self.placeholder_symbol = '%s'
        self.schema_class = PostgresqlSchema
        self.connect()
        self.create_schema(PostgresqlSchema)

    def connect(self):
        self.connection = psycopg2.connect(
            database=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port)
        return self.connection

    def execute_and_fetchall(self, *args, **kwargs):
        with self.connection.cursor() as cur:
            cur.execute(*args, **kwargs)
            return cur.fetchall()

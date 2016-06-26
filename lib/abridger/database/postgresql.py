import importlib

from .base import Database
from abridger.schema import PostgresqlSchema


class PostgresqlDatabase(Database):
    CAN_GENERATE_SQL_STATEMENTS = True

    def __init__(self, host=None, port=None, dbname=None, user=None,
                 password=None, connect=True):
        if dbname is None:
            raise ValueError('dbname must have a value')
        if user is None:
            raise ValueError('user must have a value')
        if host is None:
            raise ValueError('host must have a value')

        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.placeholder_symbol = '%s'
        self.schema_class = PostgresqlSchema
        self.connection = None

        if connect:
            self.connect()
            self.create_schema(PostgresqlSchema)

    @staticmethod
    def create_from_django_database(dj_details):
        return PostgresqlDatabase(
            host=dj_details['HOST'],
            port=dj_details['PORT'] or 5432,
            dbname=dj_details['NAME'],
            user=dj_details['USER'],
            password=dj_details['PASSWORD'])

    def connect(self):
        if self.connection is not None:
            return

        psycopg2_package = 'psycopg2'
        try:
            psycopg2 = importlib.import_module(psycopg2_package)
        except ImportError:
            raise ImportError(
                'Please install {0} package.\n'
                'pip install -U {0}'.format(psycopg2_package)
            )

        self.connection = psycopg2.connect(
            database=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port)

    def url(self):
        return 'postgresql://%s%s@%s%s/%s' % (
            self.user,
            ':%s' % self.password if self.password is not None else '',
            self.host,
            ':%s' % self.port if self.port is not None else '',
            self.dbname)

    def make_multi_col_where_clause(self, table, cols, values):
        # Produce something like
        # (col1, col2) IN ((1, 'foo'), (2, 'bar'))

        phs = self.placeholder_symbol
        where_clause = '('
        where_clause += ', '.join([c.name for c in table.cols])
        where_clause += ') IN ('

        stmt_values = []
        ph_with_comma = '%s, ' % phs
        q = ph_with_comma.join([''] * len(cols)) + phs

        for i, value_tuple in enumerate(values):
            if i > 0:
                where_clause += ', '
            where_clause += '(%s)' % q
            stmt_values.extend(list(value_tuple))
        where_clause += ')'
        return where_clause, stmt_values

    def make_begin_stmts(self):
        return [b'BEGIN;', b'\\set ON_ERROR_STOP']

    def make_commit_stmts(self):
        return [b'COMMIT;']

    def make_insert_stmt(self, cursor, row):
        (stmt, placeholders) = list(self.make_insert_statements([row]))[0]
        return cursor.mogrify(stmt, placeholders) + b';'

    def make_update_stmt(self, cursor, row):
        (stmt, placeholders) = list(self.make_update_statements([row]))[0]
        return cursor.mogrify(stmt, placeholders) + b';'

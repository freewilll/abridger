import re
import six
import sqlite3

from .base import Database
from abridger.schema import SqliteSchema


class SqliteDatabase(Database):
    def __init__(self, path=None, verbose=False):
        self.path = path
        self.placeholder_symbol = '?'
        self.connection = None
        if verbose:
            print('Connecting to %s' % self.url())
        self.connect()
        self.create_schema(SqliteSchema)

    @staticmethod
    def create_from_django_database(dj_details, verbose):
        return SqliteDatabase(path=dj_details['NAME'], verbose=verbose)

    def connect(self):
        if self.connection is not None:
            return

        self.connection = sqlite3.connect(self.path)
        self.connection.execute('pragma foreign_keys=ON')

    def url(self):
        return 'sqlite:///%s' % (self.path)

    def make_begin_stmts(self):
        return [b'BEGIN;']

    def make_commit_stmts(self):
        return [b'COMMIT;']

    def _make_sql(self, stmt, values):
        conversions = (
            ("\\", "\\\\"),
            ("\000", "\\0"),
            ('\b', '\\b'),
            ('\n', '\\n'),
            ('\r', '\\r'),
            ('\t', '\\t'),
            ("%", "%%")
        )

        formatted_values = []
        for value in values:
            if value is None:
                value = 'NULL'
            elif isinstance(value, float):
                pass
            elif not isinstance(value, six.integer_types):
                value = str(value)

                for (before, after) in conversions:
                    value = value.replace(before, after)
                value = value.replace("'", "''")
                value = "'%s'" % value
            formatted_values.append(value)

        stmt = re.sub('\?', '%s', stmt)
        return six.b(stmt % tuple(formatted_values))

    def make_insert_stmt(self, cursor, row):
        (stmt, values) = list(self.make_insert_statement(row))
        return self._make_sql(stmt, values) + b';'

    def make_update_stmt(self, cursor, row):
        (stmt, values) = list(self.make_update_statement(row))
        return self._make_sql(stmt, values) + b';'

import sqlite3

from .base import Database
from abridger.schema import SqliteSchema


class SqliteDatabase(Database):
    CAN_GENERATE_SQL_STATEMENTS = False

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

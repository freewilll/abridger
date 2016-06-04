import sqlite3

from .base import DbConn
from minime.schema import SqliteSchema


class SqliteDbConn(DbConn):
    def __init__(self, path):
        self.path = path
        self.placeholder_symbol = '?'
        self.connect()
        self.create_schema(SqliteSchema)

    @staticmethod
    def create_from_django_dbconn(dj_details):
        return SqliteDbConn(dj_details['NAME'])

    def connect(self):
        self.connection = sqlite3.connect(self.path)
        self.connection.execute('pragma foreign_keys=ON')

    def url(self):
        return 'sqlite:///%s' % (self.path)

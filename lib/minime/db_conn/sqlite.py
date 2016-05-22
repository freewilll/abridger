import sqlite3

from base import DbConn


class SqliteDbConn(DbConn):
    def __init__(self, path):
        self.path = path
        self.placeholder_symbol = '?'

    def connect(self):
        self.connection = sqlite3.connect(self.path)
        self.connection.execute('pragma foreign_keys=ON')
        return self.connection

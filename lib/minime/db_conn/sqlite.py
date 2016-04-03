import sqlite3
from . import DbConn


class SqliteDbConn(DbConn):
    def __init__(self, path):
        self.path = path

    def connect(self):
        self.connection = sqlite3.connect(self.path)
        return self.connection

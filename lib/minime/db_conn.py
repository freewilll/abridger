import psycopg2
import yaml


class DbConn(object):
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

    @staticmethod
    def load(path):
        data = yaml.load(open(path))
        return DbConn(
            host=data.get('host'),
            port=data.get('port'),
            dbname=data.get('dbname'),
            user=data.get('user'),
            password=data.get('password'))

    def connect(self):
        self.connection = psycopg2.connect(
            database=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port)
        return self.connection

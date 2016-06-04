import dj_database_url

from .sqlite import SqliteDbConn
from .postgresql import PostgresqlDbConn


__all__ = [
    'load', 'SqliteDbConn', 'PostgresqlDbConn',
]

DJANGO_ENGINE_TO_DBCONN_MAP = {
    'django.db.backends.sqlite3': SqliteDbConn,
    'django.db.backends.postgresql_psycopg2': PostgresqlDbConn,
}


def load(url):
    dj_details = dj_database_url.parse(url)
    dbconn_cls = DJANGO_ENGINE_TO_DBCONN_MAP.get(dj_details['ENGINE'])
    if dbconn_cls is None:
        raise Exception('Unable to determine the database from the URL')
    return dbconn_cls.create_from_django_dbconn(dj_details)

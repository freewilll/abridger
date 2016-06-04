import dj_database_url

from .sqlite import SqliteDatabase
from .postgresql import PostgresqlDatabase


__all__ = [
    'load', 'SqliteDatabase', 'PostgresqlDatabase',
]

DJANGO_ENGINE_TO_DBCONN_MAP = {
    'django.db.backends.sqlite3': SqliteDatabase,
    'django.db.backends.postgresql_psycopg2': PostgresqlDatabase,
}


def load(url):
    dj_details = dj_database_url.parse(url)
    database_cls = DJANGO_ENGINE_TO_DBCONN_MAP.get(dj_details['ENGINE'])
    if database_cls is None:
        raise Exception('Unable to determine the database from the URL')
    return database_cls.create_from_django_database(dj_details)

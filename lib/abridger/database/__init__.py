import dj_database_url

from .postgresql import PostgresqlDatabase
from .sqlite import SqliteDatabase
from abridger.exc import DatabaseUrlError


__all__ = [
    'load', 'SqliteDatabase', 'PostgresqlDatabase',
]

DJANGO_ENGINE_TO_DBCONN_MAP = {
    'django.db.backends.sqlite3': SqliteDatabase,
    'django.db.backends.postgresql_psycopg2': PostgresqlDatabase,
}


def load(url, verbose=False):
    dj_details = dj_database_url.parse(url)
    database_cls = DJANGO_ENGINE_TO_DBCONN_MAP.get(dj_details['ENGINE'])
    if database_cls is None:
        raise DatabaseUrlError(
            'Unable to determine the database from the URL')
    return database_cls.create_from_django_database(dj_details, verbose)

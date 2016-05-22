import dj_database_url

from sqlite import SqliteDbConn
from postgresql import PostgresqlDbConn


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

    if dbconn_cls is SqliteDbConn:
        dbconn = dbconn_cls(dj_details['NAME'])
    else:
        dbconn = dbconn_cls(
            host=dj_details['HOST'],
            port=dj_details['PORT'] or 5432,
            dbname=dj_details['NAME'],
            user=dj_details['USER'],
            password=dj_details['PASSWORD'])

    return dbconn

import os.path
import pytest

from abridger.database.sqlite import SqliteDatabase
from abridger.schema import SqliteSchema
from conftest_utils import generic_conn


@pytest.fixture(scope='function')
def sqlite_database(request):
    return SqliteDatabase(':memory:')


@pytest.fixture(scope='function')
def sqlite_conn(request, sqlite_database):
    return generic_conn(request, sqlite_database)


@pytest.fixture(scope='function')
def schema1_sl(request, sqlite_conn):
    test_sql_path = os.path.join(os.path.dirname(__file__),
                                 os.pardir, 'data',
                                 'schema1.sql')
    for statement in open(test_sql_path).read().split(';'):
        sqlite_conn.execute(statement)
    return SqliteSchema.create_from_conn(sqlite_conn)

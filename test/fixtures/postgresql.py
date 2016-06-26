import os.path
import pytest
import re

from abridger.database import PostgresqlDatabase
from abridger.schema import PostgresqlSchema
from conftest_utils import generic_conn


def make_postgresql_database(postgresql_fixture):
    m = re.match(r'user=(.+) host=(.+) port=(\d+) dbname=(.+)',
                 postgresql_fixture.dsn)
    assert m is not None
    (user, host, port, dbname) = (m.group(1), m.group(2), m.group(3),
                                  m.group(4))

    return PostgresqlDatabase(user=user, host=host, port=port, dbname=dbname)


@pytest.fixture(scope='function')
def postgresql_database(request, postgresql):
    return make_postgresql_database(postgresql)


@pytest.fixture(scope='function')
def postgresql_conn(request, postgresql_database):
    return generic_conn(request, postgresql_database)


@pytest.fixture(scope='function')
def schema_pg(request, postgresql_conn):
    test_sql_path = os.path.join(os.path.dirname(__file__),
                                 os.pardir, 'data', 'schema.sql')
    with postgresql_conn.cursor() as cur:
        cur.execute(open(test_sql_path).read())
    return PostgresqlSchema.create_from_conn(postgresql_conn)

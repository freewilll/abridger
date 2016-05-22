import os.path
import pytest
import re

from minime.db_conn import PostgresqlDbConn
from minime.schema import PostgresqlSchema
from conftest_utils import generic_conn


@pytest.fixture(scope='function')
def postgresql_dbconn(request, postgresql):
    m = re.match(r'user=(.+) host=(.+) port=(\d+) dbname=(.+)',
                 postgresql.dsn)
    assert m is not None
    (user, host, port, dbname) = (m.group(1), m.group(2), m.group(3),
                                  m.group(4))

    return PostgresqlDbConn(user=user, host=host, port=port, dbname=dbname)


@pytest.fixture(scope='function')
def postgresql_conn(request, postgresql_dbconn):
    return generic_conn(request, postgresql_dbconn)


@pytest.fixture(scope='function')
def schema1_pg(request, postgresql_conn):
    test_sql_path = os.path.join(os.path.dirname(__file__),
                                 os.pardir, os.pardir, 'data',
                                 'schema1.sql')
    with postgresql_conn.cursor() as cur:
        cur.execute(open(test_sql_path).read())
    return PostgresqlSchema.create_from_conn(postgresql_conn)

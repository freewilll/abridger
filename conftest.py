import os.path
import pytest
import re

from minime.db_conn import DbConn
from minime.schema import Schema


@pytest.fixture(scope='function')
def dbconn(request, postgresql):
    m = re.match(r'user=(.+) host=(.+) port=(\d+) dbname=(.+)',
                 postgresql.dsn)
    assert m is not None
    (user, host, port, dbname) = (m.group(1), m.group(2), m.group(3),
                                  m.group(4))

    return DbConn(user=user, host=host, port=port, dbname=dbname)


@pytest.fixture(scope='function')
def conn(request, dbconn):
    result = dbconn.connect()

    def fin():
        result.close()
    request.addfinalizer(fin)

    return result


@pytest.fixture(scope='function')
def schema1(request, conn):
    test_sql_path = os.path.join(os.path.dirname(__file__), 'test', 'data',
                                 'schema1.sql')
    with conn.cursor() as cur:
        cur.execute(open(test_sql_path).read())
    return Schema.create_from_conn(conn)

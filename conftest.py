import pytest
import re

from minime.db_conn import DbConn


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

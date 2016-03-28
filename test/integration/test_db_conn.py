import pytest
import tempfile
import yaml

from minime.db_conn import DbConn


class TestDbConn(object):
    def test_load_database_conn_file(self):
        def create_conn(data, omit_key=None, expect_exception=False):
            def load_it(data):
                data = dict(data)
                if omit_key is not None:
                    del data[omit_key]

                temp = tempfile.NamedTemporaryFile(mode='wb', delete=False)
                temp.write(yaml.dump(data, default_flow_style=False))
                temp.close()
                return DbConn.load(temp.name)

            if expect_exception:
                with pytest.raises(Exception):
                    return load_it(data)
            else:
                return load_it(data)

        data = {
            'dbname': 'dummy',
            'host': 'dummy',
            'port': 5432,
            'user': 'dummy',
            'password': 'dummy',
        }

        create_conn(data)
        create_conn(data, 'dbname', expect_exception=True)
        create_conn(data, 'user', expect_exception=True)

    def test_dbconn_fixture(self, dbconn):
        conn = dbconn.connect()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM pg_class;")
            cur.fetchone()
        conn.close()

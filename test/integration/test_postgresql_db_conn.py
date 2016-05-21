import pytest
import tempfile
import yaml

from minime.db_conn.postgresql import PostgresqlDbConn
from minime.schema.postgresql import PostgresqlSchema
from dbconn import DbConnTestBase


class TestPostgresqlDbConn(DbConnTestBase):
    @pytest.fixture(autouse=True)
    def prepare(self, request, postgresql_dbconn):
        self.dbconn = postgresql_dbconn
        self.make_db(request, PostgresqlSchema)

    def test_load_database_conn_file(self):
        def create_conn(data, omit_key=None, expect_exception=False):
            def load_it(data):
                data = dict(data)
                if omit_key is not None:
                    del data[omit_key]

                temp = tempfile.NamedTemporaryFile(mode='wb', delete=False)
                temp.write(yaml.dump(data, default_flow_style=False))
                temp.close()
                return PostgresqlDbConn.load(temp.name)

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

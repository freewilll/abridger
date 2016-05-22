import pytest

from minime.schema import PostgresqlSchema
from dbconn import DbConnTestBase


class TestPostgresqlDbConn(DbConnTestBase):
    @pytest.fixture(autouse=True)
    def prepare(self, request, postgresql_dbconn):
        self.dbconn = postgresql_dbconn
        self.make_db(request, PostgresqlSchema)

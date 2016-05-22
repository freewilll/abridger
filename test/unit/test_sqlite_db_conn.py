import pytest

from dbconn import DbConnTestBase
from minime.schema import SqliteSchema


class TestSqliteDbConn(DbConnTestBase):
    @pytest.fixture(autouse=True)
    def prepare(self, request, sqlite_dbconn):
        self.dbconn = sqlite_dbconn
        self.make_db(request, SqliteSchema)

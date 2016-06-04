import pytest

from database import DatabaseTestBase
from minime.schema import SqliteSchema


class TestSqliteDatabase(DatabaseTestBase):
    @pytest.fixture(autouse=True)
    def prepare(self, request, sqlite_database):
        self.database = sqlite_database
        self.make_db(request, SqliteSchema)

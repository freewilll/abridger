import pytest

from abridger.database import load
from database import DatabaseTestBase
from abridger.schema import SqliteSchema
from abridger.exc import DatabaseUrlError


class TestSqliteDatabase(DatabaseTestBase):
    @pytest.fixture(autouse=True)
    def prepare(self, request, sqlite_database):
        self.database = sqlite_database
        self.schema_cls = SqliteSchema
        self.make_db(request)

    def test_bad_url(self):
        with pytest.raises(DatabaseUrlError):
            load("oracle://bar")

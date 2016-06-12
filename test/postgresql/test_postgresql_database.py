import pytest

from abridger.schema import PostgresqlSchema
from database import DatabaseTestBase
from abridger.database import PostgresqlDatabase
from test.conftest import got_postgresql


@pytest.mark.skipif(not got_postgresql(), reason='Needs postgresql')
class TestPostgresqlDatabase(DatabaseTestBase):
    @pytest.fixture(autouse=True)
    def prepare(self, request, postgresql_database):
        self.database = postgresql_database
        self.make_db(request, PostgresqlSchema)

    def test_bad_params(self):
        with pytest.raises(ValueError):
            PostgresqlDatabase(user='foo')
        with pytest.raises(ValueError):
            PostgresqlDatabase(dbname='foo')

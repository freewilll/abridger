import pytest

from abridger.schema import PostgresqlSchema
from database import DatabaseTestBase


class TestPostgresqlDatabase(DatabaseTestBase):
    @pytest.fixture(autouse=True)
    def prepare(self, request, postgresql_database):
        self.database = postgresql_database
        self.make_db(request, PostgresqlSchema)

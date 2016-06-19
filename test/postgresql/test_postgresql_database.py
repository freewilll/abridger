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
        for key in ('user', 'host', 'dbname'):
            params = {'host': 'host', 'user': 'user', 'dbname': 'dbname'}
            del params[key]
        with pytest.raises(ValueError):
            PostgresqlDatabase(**params)

    @pytest.mark.parametrize('params, url', [
        ({}, 'u@h/n'),
        ({'password': 'pass'}, 'u:pass@h/n'),
        ({'port': 100}, 'u@h:100/n'),
        ({'password': 'pass', 'port': 100}, 'u:pass@h:100/n'),
    ])
    def test_url_generation(self, params, url):
        full_params = {'host': 'h', 'user': 'u', 'dbname': 'n',
                       'connect': False}
        full_params.update(params)
        url = 'postgresql://' + url
        assert PostgresqlDatabase(**full_params).url() == url

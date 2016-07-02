import mock
import pytest

from abridger.database import PostgresqlDatabase
from abridger.schema import PostgresqlSchema
from database import DatabaseTestBase
from test.conftest import got_postgresql


class TestPostgresqlDatabase(DatabaseTestBase):
    @pytest.fixture(autouse=True)
    def _skip_if_not_postgres(self):
        # Note: pytest.mark.skipif cannot be used at the class level,
        # since this could have the side-effect of also disabling the sqlite
        # tests.
        #
        # It appears that adding a skip to this class causes
        # all other test classes using the DatabaseTestBase base class to be
        # skipped too.

        if not got_postgresql():
            pytest.skip('Needs postgresql')

    @pytest.fixture(autouse=True)
    def prepare(self, request, postgresql_database):
        self.database = postgresql_database
        self.schema_cls = PostgresqlSchema
        self.make_db(request)

    def test_bad_params(self):
        for key in ('user', 'host', 'dbname'):
            params = {'host': 'host', 'user': 'user', 'dbname': 'dbname'}
            del params[key]
            with pytest.raises(ValueError):
                PostgresqlDatabase(**params)

    @pytest.mark.parametrize('params, url, url_nopass', [
        ({}, 'u@h/n', 'u@h/n'),
        ({'password': 'pass'}, 'u:pass@h/n', 'u@h/n'),
        ({'port': 100}, 'u@h:100/n', 'u@h:100/n'),
        ({'password': 'pass', 'port': 100}, 'u:pass@h:100/n', 'u@h:100/n'),
    ])
    def test_url_generation(self, params, url, url_nopass):
        full_params = {'host': 'h', 'user': 'u', 'dbname': 'n',
                       'connect': False}
        full_params.update(params)

        # Check with password
        url = 'postgresql://' + url
        pg_url = PostgresqlDatabase(**full_params).url()
        assert pg_url == url

        # Check without password
        full_params.pop('password', None)
        url_nopass = 'postgresql://' + url_nopass
        pg_url = PostgresqlDatabase(**full_params).url(include_password=False)
        assert pg_url == url_nopass

    @mock.patch('abridger.database.postgresql.import_module',
                side_effect=[ImportError(), None])
    def test_missing_psycopg2_package(self, request, postgresql_database):
        self.database.disconnect()
        with pytest.raises(ImportError) as e:
            self.database.connect()
        assert 'Please install psycopg2 package' in str(e)

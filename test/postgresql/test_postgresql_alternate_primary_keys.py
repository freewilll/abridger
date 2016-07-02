import pytest

from abridger.schema import PostgresqlSchema
from alternate_primary_keys import TestAlternatePrimaryKeysBase as Base
from test.conftest import got_postgresql


@pytest.mark.skipif(not got_postgresql(), reason='Needs postgresql')
class TestPostgresqlAlternatePrimaryKeys(Base):
    @pytest.mark.parametrize('table, count', Base.alternate_primary_keys_tests)
    def test_alternate_primary_keys(self, postgresql_conn,
                                    table, count):
        cur = postgresql_conn.cursor()
        for stmt in self.test_alternate_primary_keys_stmts:
            cur.execute(stmt)
        cur.close()

        schema = PostgresqlSchema.create_from_conn(postgresql_conn)
        self.check_alternate_primary_keys(schema, table, count)

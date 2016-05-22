import pytest

from minime.schema import SqliteSchema
from alternate_primary_keys import TestAlternatePrimaryKeysBase as Base


class TestSqliteAlternatePrimaryKeys(Base):
    @pytest.mark.parametrize('table, count', Base.alternate_primary_keys_tests)
    def test_schema_sqlite_alternate_primary_keys(self, sqlite_conn, table,
                                                  count):
        for sql in self.test_alternate_primary_keys_sql:
            sqlite_conn.execute(sql)
        schema = SqliteSchema.create_from_conn(sqlite_conn)
        self.check_alternate_primary_keys(schema, table, count)

import pytest

from abridger.schema import SqliteSchema
from alternate_primary_keys import TestAlternatePrimaryKeysBase as Base


class TestSqliteAlternatePrimaryKeys(Base):
    @pytest.mark.parametrize('table, count', Base.alternate_primary_keys_tests)
    def test_schema_sqlite_alternate_primary_keys(self, sqlite_conn, table,
                                                  count):
        for stmt in self.test_alternate_primary_keys_stmts:
            sqlite_conn.execute(stmt)
        schema = SqliteSchema.create_from_conn(sqlite_conn)
        self.check_alternate_primary_keys(schema, table, count)

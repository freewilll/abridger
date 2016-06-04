from pytest_dbfixtures import factories

from test.minime_db_test_utils import TestMinimeDbBase
from test.fixtures.postgresql import make_postgresql_database


postgresql_proc2 = factories.postgresql_proc(port=5434)
postgresql2 = factories.postgresql('postgresql_proc2')


class TestMinimeDbForPostgresql(TestMinimeDbBase):
    def setup_method(self, method):
        self.src_database = None
        self.dst_database = None

    def teardown_method(self, method):
        # Belt and braces in case something unexpected fails
        if self.src_database is not None:
            self.src_database.disconnect
        if self.dst_database is not None:
            self.dst_database.disconnect

    def test_main(self, capsys, postgresql, postgresql2):
        # Prepare src
        self.src_database = make_postgresql_database(postgresql)
        self.src_conn = self.src_database.connection
        self.create_schema(self.src_conn)
        self.create_data(self.src_conn)
        self.src_conn.commit()
        self.src_database.disconnect()

        # Prepare dst
        self.dst_database = make_postgresql_database(postgresql2)
        self.dst_conn = self.dst_database.connection
        self.create_schema(self.dst_conn)
        self.dst_database.disconnect()

        # Run minime
        src_url = self.src_database.url()
        dst_url = self.dst_database.url()
        self.run_main(src_url, dst_url, self.dst_database)

        out, err = capsys.readouterr()

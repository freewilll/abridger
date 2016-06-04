from pytest_dbfixtures import factories

from test.minime_db_test_utils import TestMinimeDbBase
from test.fixtures.postgresql import make_postgresql_dbconn


postgresql_proc2 = factories.postgresql_proc(port=5434)
postgresql2 = factories.postgresql('postgresql_proc2')


class TestMinimeDbForPostgresql(TestMinimeDbBase):
    def setup_method(self, method):
        self.src_dbconn = None
        self.dst_dbconn = None

    def teardown_method(self, method):
        # Belt and braces in case something unexpected fails
        if self.src_dbconn is not None:
            self.src_dbconn.disconnect
        if self.dst_dbconn is not None:
            self.dst_dbconn.disconnect

    def test_main(self, capsys, postgresql, postgresql2):
        # Prepare src
        self.src_dbconn = make_postgresql_dbconn(postgresql)
        self.src_conn = self.src_dbconn.connect()
        self.create_schema(self.src_conn)
        self.create_data(self.src_conn)
        self.src_conn.commit()
        self.src_dbconn.disconnect()

        # Prepare dst
        self.dst_dbconn = make_postgresql_dbconn(postgresql2)
        self.dst_conn = self.dst_dbconn.connect()
        self.create_schema(self.dst_conn)
        self.dst_dbconn.disconnect()

        # Run minime
        src_url = self.src_dbconn.url()
        dst_url = self.dst_dbconn.url()
        self.run_main(src_url, dst_url, self.dst_dbconn)

        out, err = capsys.readouterr()

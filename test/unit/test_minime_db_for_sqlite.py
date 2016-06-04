import pytest
from tempfile import NamedTemporaryFile

from minime.db_conn.sqlite import SqliteDbConn
from test.minime_db_test_utils import TestMinimeDbBase


class TestMinimeDbForSqlite(TestMinimeDbBase):
    def prepare_src(self):
        self.src = NamedTemporaryFile(mode='wt', suffix='.sqlite3')
        self.src_dbconn = SqliteDbConn(self.src.name)
        src_conn = self.src_dbconn.connect()
        self.create_schema(src_conn)
        self.create_data(src_conn)
        src_conn.commit()
        self.src_dbconn.disconnect()

    def prepare_dst(self, with_schema=True):
        self.dst = NamedTemporaryFile(mode='wt', suffix='.sqlite3')
        self.dst_dbconn = SqliteDbConn(self.dst.name)
        dst_conn = self.dst_dbconn.connect()
        if with_schema:
            self.create_schema(dst_conn)
        self.dst_dbconn.disconnect()

    def run_main(self, explain=False):
        src_url = 'sqlite:///%s' % (self.src_dbconn.path)
        dst_url = 'sqlite:///%s' % (self.dst_dbconn.path)
        super(TestMinimeDbForSqlite, self).run_main(
            src_url, dst_url, self.dst_dbconn, explain=explain)

    def test_success(self, capsys):
        self.prepare_src()
        self.prepare_dst(with_schema=True)
        self.run_main()
        out, err = capsys.readouterr()

    def test_failure_rollback(self, capsys):
        self.prepare_src()
        self.prepare_dst(with_schema=False)
        with pytest.raises(Exception) as e:
            self.run_main()
        assert 'no such table' in str(e)
        out, err = capsys.readouterr()

    def test_explain(self, capsys):
        self.prepare_src()
        self.prepare_dst(with_schema=False)
        with pytest.raises(SystemExit):
            self.run_main(explain=True)
        out, err = capsys.readouterr()
        assert 'test1* -> test1.id=2 -> test2.id=2 -> test1.id=2' in out
        assert err == ''
        assert len(out.split("\n")) == 8

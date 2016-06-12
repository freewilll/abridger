import pytest
from tempfile import NamedTemporaryFile
from sqlite3 import OperationalError

from test.abridge_db_test_utils import TestAbridgeDbBase
from abridger.database.sqlite import SqliteDatabase


class TestAbridgeDbForSqlite(TestAbridgeDbBase):
    def prepare_src(self):
        self.src = NamedTemporaryFile(mode='wt', suffix='.sqlite3')
        self.src_database = SqliteDatabase(self.src.name)
        src_conn = self.src_database.connection
        self.create_schema(src_conn)
        self.create_data(src_conn)
        src_conn.commit()
        self.src_database.disconnect()

    def prepare_dst(self, with_schema=True):
        self.dst = NamedTemporaryFile(mode='wt', suffix='.sqlite3')
        self.dst_database = SqliteDatabase(self.dst.name)
        dst_conn = self.dst_database.connection
        if with_schema:
            self.create_schema(dst_conn)
        self.dst_database.disconnect()

    def run_main(self, explain=False, verbosity=1):
        src_url = self.src_database.url()
        dst_url = self.dst_database.url()
        super(TestAbridgeDbForSqlite, self).run_main(
            src_url, dst_url, self.dst_database, explain=explain,
            verbosity=verbosity)

    def test_success(self, capsys):
        self.prepare_src()
        self.prepare_dst(with_schema=True)
        self.run_main()
        out, err = capsys.readouterr()

    def test_failure_rollback(self, capsys):
        self.prepare_src()
        self.prepare_dst(with_schema=False)
        with pytest.raises(OperationalError):
            self.run_main()
        out, err = capsys.readouterr()

    def test_explain(self, capsys):
        self.prepare_src()
        self.prepare_dst(with_schema=False)
        with pytest.raises(SystemExit):
            self.run_main(explain=True)
        out, err = capsys.readouterr()
        assert 'test1* -> test1.id=2 -> test2.id=2 -> test1.id=2' in out
        assert err == ''

    def test_quiet_output(self, capsys):
        self.prepare_src()
        self.prepare_dst(with_schema=True)
        self.run_main(verbosity=0)
        out, err = capsys.readouterr()
        assert len(out) == 0
        assert len(err) == 0

    def check_verbosity1_output(self, out):
        assert 'Connecting to' in out
        assert 'Querying' in out
        assert 'Performing 5 inserts and 2 updates to 2 tables...' in out
        assert 'Extraction completed: rows=7, tables=2, queries=3, depth=2' \
            in out
        assert 'Data loading completed in' in out

    def test_default_output(self, capsys):
        self.prepare_src()
        self.prepare_dst(with_schema=True)
        self.run_main(verbosity=1)
        out, err = capsys.readouterr()
        self.check_verbosity1_output(out)

    def test_verbose_output(self, capsys):
        self.prepare_src()
        self.prepare_dst(with_schema=True)
        self.run_main(verbosity=2)
        out, err = capsys.readouterr()
        self.check_verbosity1_output(out)
        assert ('Processing pass=1     queued=0     depth=0   tables=0    '
                'rows=0       table') in out
        assert 'Inserting' in out
        assert 'Updating' in out

from sqlite3 import OperationalError
from tempfile import NamedTemporaryFile
import os
import pytest
import subprocess

from abridger.abridge_db import main
from abridger.database.sqlite import SqliteDatabase
from test.abridge_db_test_utils import TestAbridgeDbBase


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

    def run_with_dst_database(self, explain=False, verbosity=1):
        src_url = self.src_database.url()
        dst_url = self.dst_database.url()
        super(TestAbridgeDbForSqlite, self).run_with_dst_database(
            src_url, dst_url, self.dst_database, explain=explain,
            verbosity=verbosity)

    def test_success(self, capsys):
        self.prepare_src()
        self.prepare_dst(with_schema=True)
        self.run_with_dst_database()
        out, err = capsys.readouterr()

    def test_failure_rollback(self, capsys):
        self.prepare_src()
        self.prepare_dst(with_schema=False)
        with pytest.raises(OperationalError):
            self.run_with_dst_database()
        out, err = capsys.readouterr()

    def test_explain(self, capsys):
        self.prepare_src()
        self.prepare_dst(with_schema=False)
        with pytest.raises(SystemExit):
            self.run_with_dst_database(explain=True)
        out, err = capsys.readouterr()
        assert 'test1* -> test1.id=2 -> test2.id=2 -> test1.id=2' in out
        assert err == ''

    def test_quiet_output(self, capsys):
        self.prepare_src()
        self.prepare_dst(with_schema=True)
        self.run_with_dst_database(verbosity=0)
        out, err = capsys.readouterr()
        assert len(out) == 0
        assert len(err) == 0

    def check_verbosity1_output(self, out):
        assert 'Connecting to %s' % self.src_database.url() in out
        assert 'Querying' in out
        assert ('Extraction completed: '
                'fetched rows=7, '
                'tables=2, '
                'queries=3, '
                'depth=2') in out

    def check_verbosity1_output_for_url(self, out):
        self.check_verbosity1_output(out)
        assert 'Connecting to %s' % self.dst_database.url() in out
        assert 'Performing 5 inserts and 2 updates to 2 tables...' in out
        assert 'Data loading completed in' in out

    def check_verbosity1_output_for_file(self, out):
        assert 'Writing SQL for 5 inserts and 2 updates in 2 tables...' in out
        assert 'Done' in out

    def test_default_output(self, capsys):
        self.prepare_src()
        self.prepare_dst(with_schema=True)
        self.run_with_dst_database(verbosity=1)
        out, err = capsys.readouterr()
        self.check_verbosity1_output_for_url(out)

    def test_verbose_output(self, capsys):
        self.prepare_src()
        self.prepare_dst(with_schema=True)
        self.run_with_dst_database(verbosity=2)
        out, err = capsys.readouterr()
        self.check_verbosity1_output_for_url(out)
        assert ('Processing pass=1     queued=0     depth=0   tables=0    '
                'rows=0       table') in out
        assert 'Inserting' in out
        assert 'Updating' in out

    def test_f_and_u_args_mutual_exclusion(self, capsys):
        with pytest.raises(SystemExit):
            main(['foo', 'bar', '-u', 'foo', '-f', 'bar'])
        out, err = capsys.readouterr()
        assert 'Either -u or -f must be passed' in out

    def test_e_f_and_u_args(self, capsys):
        for arg in ['-u', '-f']:
            with pytest.raises(SystemExit):
                main(['foo', 'bar', '-e', arg, 'foo'])
            out, err = capsys.readouterr()
            assert '%s is meaningless when using -e' % arg in out

    def check_statements(self, stmts):
        self.prepare_dst(with_schema=True)
        self.dst_database.connect()
        dst_conn = self.dst_database.connection
        for stmt in stmts.split("\n"):
            if stmt != 'COMMIT;':
                dst_conn.execute(stmt)
        dst_conn.commit()
        self.check_dst_database(self.dst_database)

    def test_output_to_stdout(self):
        self.prepare_src()
        config_tempfile = self.make_config_tempfile()
        executable = os.path.join(
            os.path.dirname(__file__), os.pardir, os.pardir, 'bin',
            'abridge-db')
        stmts = subprocess.check_output([
            executable,
            config_tempfile.name, self.src_database.url(), '-q', '-f', '-']
        ).decode('UTF-8')
        self.check_statements(stmts)

    def test_output_to_file(self, capsys):
        self.prepare_src()
        src_url = self.src_database.url()
        config_tempfile = self.make_config_tempfile()
        dst = NamedTemporaryFile(mode='wb')
        dst.close()
        main([config_tempfile.name, src_url, '-f', dst.name])
        with open(dst.name) as f:
            self.check_statements(f.read())
        out, err = capsys.readouterr()
        self.check_verbosity1_output_for_file(out)

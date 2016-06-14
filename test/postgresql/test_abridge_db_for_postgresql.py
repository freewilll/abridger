from pytest_dbfixtures import factories
from tempfile import NamedTemporaryFile
import os
import pytest
import re
import subprocess
import tempfile

from abridger.abridge_db import main
from abridger.database.sqlite import SqliteDatabase
from test.abridge_db_test_utils import TestAbridgeDbBase
from test.conftest import got_postgresql
from test.fixtures.postgresql import make_postgresql_database


postgresql_proc2 = factories.postgresql_proc(port=5434)
postgresql2 = factories.postgresql('postgresql_proc2')


@pytest.mark.skipif(not got_postgresql(), reason='Needs postgresql')
class TestAbridgeDbForPostgresql(TestAbridgeDbBase):
    def setup_method(self, method):
        self.src_database = None
        self.dst_database = None

    def teardown_method(self, method):
        # Belt and braces in case something unexpected fails
        if self.src_database is not None:
            self.src_database.disconnect
        if self.dst_database is not None:
            self.dst_database.disconnect

    def prepare_src(self, postgresql):
        self.src_database = make_postgresql_database(postgresql)
        self.src_conn = self.src_database.connection
        self.create_schema(self.src_conn)
        self.create_data(self.src_conn)
        self.src_conn.commit()
        self.src_database.disconnect()

    def prepare_dst(self, postgresql, disconnect=True):
        self.dst_database = make_postgresql_database(postgresql)
        self.dst_conn = self.dst_database.connection
        self.create_schema(self.dst_conn)
        if disconnect:
            self.dst_database.disconnect()

    def test_main(self, capsys, postgresql, postgresql2):
        self.prepare_src(postgresql)
        self.prepare_dst(postgresql2, disconnect=True)
        self.run_with_dst_database(
            self.src_database.url(),
            self.dst_database.url(),
            self.dst_database)
        out, err = capsys.readouterr()

    def check_statements(self, postgresql2, stmts):
        self.prepare_dst(postgresql2, disconnect=False)

        for stmt in stmts.split("\n"):
            if stmt == '' or re.match('^\\\\set.*', stmt):
                continue
            self.dst_database.execute(stmt)

        self.check_dst_database(self.dst_database)

    def test_output_to_stdout(self, postgresql, postgresql2):
        config_filename = self.make_config()
        self.prepare_src(postgresql)

        executable = os.path.join(
            os.path.dirname(__file__), os.pardir, os.pardir, 'bin',
            'abridge-db')
        stmts = subprocess.check_output([
            executable,
            config_filename, self.src_database.url(), '-q', '-f', '-']
        ).decode('UTF-8')
        self.check_statements(postgresql2, stmts)

    def test_output_to_file(self, postgresql, postgresql2):
        dst_filename = tempfile.NamedTemporaryFile(mode='wb')
        dst_filename.close()
        config_filename = self.make_config()
        self.prepare_src(postgresql)

        executable = os.path.join(
            os.path.dirname(__file__), os.pardir, os.pardir, 'bin',
            'abridge-db')
        subprocess.check_output([
            executable,
            config_filename, self.src_database.url(), '-q',
            '-f', dst_filename.name]
        ).decode('UTF-8')
        with open(dst_filename.name) as f:
            self.check_statements(postgresql2, f.read())

    def test_src_dst_type_mismatch(self, capsys, postgresql):
        config_filename = self.make_config()
        self.prepare_src(postgresql)
        dst = NamedTemporaryFile(mode='wt', suffix='.sqlite3')
        dst_database = SqliteDatabase(dst.name)

        with pytest.raises(SystemExit):
            main([config_filename, self.src_database.url(), '-q',
                  '-u', dst_database.url()])
        out, err = capsys.readouterr()
        assert 'src and dst databases must be of the same type' in out

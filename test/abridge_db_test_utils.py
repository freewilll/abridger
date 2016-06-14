from abridger.abridge_db import main
from test.unit.utils import make_temp_yaml_file


class TestAbridgeDbBase(object):
    def create_schema(self, conn):
        cur = conn.cursor()
        for sql in (
            '''
                CREATE TABLE test1(
                    id INTEGER PRIMARY KEY)
            ''', '''
                CREATE TABLE test2(
                    id INTEGER PRIMARY KEY,
                    test1_id INT NOT NULL REFERENCES test1)
            ''', '''
                ALTER TABLE test1 ADD COLUMN test2_id INT REFERENCES test2
                '''):
            cur.execute(sql)
        conn.commit()

    def create_data(self, conn):
        cur = conn.cursor()
        cur.execute('INSERT INTO test1 VALUES (1, null), (2, null), (3, null)')
        cur.execute('INSERT INTO test2 VALUES (1, 1), (2, 2), (3, 3)')
        cur.execute('UPDATE test1 SET test2_id = 1 WHERE id=1')
        cur.execute('UPDATE test1 SET test2_id = 2 WHERE id=2')
        conn.commit()

    def make_config(self):
        return make_temp_yaml_file([
            {'subject': [{'tables': [{'table': 'test1'}]}]}
        ])

    def check_dst_database(self, dst_database):
        dst_database.connect()
        dst_conn = dst_database.connection
        cur = dst_conn.cursor()
        try:
            cur.execute('SELECT id, test2_id FROM test1 ORDER BY id')
            rows = list(cur.fetchall())
            assert rows == [(1, 1), (2, 2), (3, None)]

            cur.execute('SELECT id, test1_id FROM test2 ORDER BY id')
            rows = list(cur.fetchall())
            assert rows == [(1, 1), (2, 2)]
        finally:
            dst_database.disconnect()

    def run_with_dst_database(self, src_url, dst_url, dst_database,
                              explain=False, verbosity=1, check=True):
        config_filename = self.make_config()
        args = [config_filename, src_url, '-u', dst_url]
        if explain:
            args.append('--explain')
        if verbosity == 0:
            args.append('-q')
        if verbosity == 2:
            args.append('-v')
        main(args)

        if check:
            self.check_dst_database(dst_database)

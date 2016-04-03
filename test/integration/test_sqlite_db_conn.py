class TestSqlitelDbConn(object):
    def test_sqlite_db_conn_fixture(self, sqlite_dbconn):
        conn = sqlite_dbconn.connect()
        cur = conn.cursor()
        cur.execute("CREATE TABLE table1 (id INTEGER PRIMARY KEY, name TEXT)")
        cur.execute("INSERT INTO table1 (name) VALUES ('foo')")
        conn.commit()
        cur.execute('SELECT * FROM table1')
        conn.close()

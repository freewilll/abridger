import pytest


class DbConnTestBase(object):
    def make_db(self, request, schema_cls):
        dbconn = self.dbconn
        conn = dbconn.connection
        dbconn.execute(
            "CREATE TABLE table1 (id SERIAL PRIMARY KEY, name TEXT)")
        conn.commit()

        self.schema = schema_cls.create_from_conn(dbconn.connection)
        self.table1 = self.schema.tables[0]

        def fin():
            conn.close()
        request.addfinalizer(fin)

    def test_fetch_rows(self):
        dbconn = self.dbconn
        dbconn.execute("INSERT INTO table1 (id, name) VALUES (1, 'foo')")
        dbconn.execute("INSERT INTO table1 (id, name) VALUES (2, 'bar')")
        dbconn.connection.commit()
        results = dbconn.execute_and_fetchall('SELECT * FROM table1')
        inserts = [(1, 'foo'), (2, 'bar')]
        assert results == inserts
        fetch_result = self.dbconn.fetch_rows(self.table1, None, None)
        assert list(fetch_result) == inserts

    @pytest.mark.parametrize('cols, values, start, end', [
        (None, None, 0, 2),
        ([0], [[1]], 0, 1),
        ([0], [[2]], 1, 2),
        ([0], [[1], [2]], 0, 2),
        ([1], [['foo']], 0, 1),
        ([1], [['bar']], 1, 2),
        ([1], [['foo'], ['bar']], 0, 2),
    ])
    def test_insert_rows(self, cols, values, start, end):
        dbconn = self.dbconn
        dbconn.execute("DELETE FROM table1")
        dbconn.insert_rows([
            (self.table1, (1, 'foo')),
            (self.table1, (2, 'bar')),
        ])

        if cols is not None:
            cols = [self.table1.cols[i] for i in cols]

        fetch_result = self.dbconn.fetch_rows(self.table1, cols, values)
        inserts = [(1, 'foo'), (2, 'bar')]
        assert list(fetch_result) == inserts[start:end]

    @pytest.mark.parametrize('pk_cols, pk_values, cols, values, result', [
        # Match id
        ([0], [1], [0], [3],     [(3, 'foo'), (2, 'bar')]),
        ([0], [1], [1], ['baz'], [(1, 'baz'), (2, 'bar')]),
        ([0], [2], [0], [3],     [(1, 'foo'), (3, 'bar')]),
        ([0], [2], [1], ['baz'], [(1, 'foo'), (2, 'baz')]),

        # Match name
        ([1], ['foo'], [0], [3],     [(3, 'foo'), (2, 'bar')]),
        ([1], ['foo'], [1], ['baz'], [(1, 'baz'), (2, 'bar')]),
        ([1], ['bar'], [0], [3],     [(1, 'foo'), (3, 'bar')]),
        ([1], ['bar'], [1], ['baz'], [(1, 'foo'), (2, 'baz')]),

        # Match both
        ([0, 1], [1, 'foo'], [1], ['baz'], [(1, 'baz'), (2, 'bar')]),

        # No matches
        ([0], [4], [0], [3],     [(1, 'foo'), (2, 'bar')]),
        ([1], ['baz'], [0], [3], [(1, 'foo'), (2, 'bar')]),
    ])
    def test_update_rows(self, pk_cols, pk_values, cols, values, result):
        dbconn = self.dbconn
        dbconn.execute("DELETE FROM table1")
        dbconn.insert_rows([
            (self.table1, (1, 'foo')),
            (self.table1, (2, 'bar')),
        ])

        pk_cols = [self.table1.cols[i] for i in pk_cols]
        cols = [self.table1.cols[i] for i in cols]
        dbconn.update_rows([(self.table1, tuple(pk_cols),
                             tuple(pk_values), tuple(cols), tuple(values))])
        fetch_result = self.dbconn.fetch_rows(self.table1, None, None)
        assert sorted(list(fetch_result)) == sorted(result)

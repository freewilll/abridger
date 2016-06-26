import pytest


class DatabaseTestBase(object):
    def make_db(self, request):
        database = self.database
        conn = database.connection
        database.execute(
            "CREATE TABLE table1 (id SERIAL PRIMARY KEY, name TEXT)")
        conn.commit()

        self.schema = self.schema_cls.create_from_conn(database.connection)
        self.table1 = self.schema.tables[0]

        def fin():
            conn.close()
        request.addfinalizer(fin)

    def test_double_connect(self):
        self.database.connect()

    @pytest.mark.parametrize('cols, values, results', [
        (None, None, [(1, 'foo'), (2, 'bar')]),
        ([0], [], []),
        ([0], [(1,)], [(1, 'foo')]),
        ([0], [(2,)], [(2, 'bar')]),
        ([0], [(1,), (2,)], [(1, 'foo'), (2, 'bar')]),
        ([1], [], []),
        ([1], [('foo',)], [(1, 'foo')]),
        ([1], [('bar',)], [(2, 'bar')]),
        ([1], [('foo',), ('bar',)], [(1, 'foo'), (2, 'bar')]),
        ([0], [(3,)], []),
        ([1], [('baz',)], []),
        ([0, 1], [], []),
        ([0, 1], [(1, 'foo',)], [(1, 'foo')]),
        ([0, 1], [(1, 'foo',), (2, 'bar',)], [(1, 'foo'), (2, 'bar')]),
    ])
    def test_fetch_rows(self, cols, values, results):
        database = self.database
        database.execute("INSERT INTO table1 (id, name) VALUES (1, 'foo')")
        database.execute("INSERT INTO table1 (id, name) VALUES (2, 'bar')")
        database.connection.commit()

        if cols is not None:
            cols = [self.schema.tables[0].cols[i] for i in cols]
        fetch_result = self.database.fetch_rows(self.table1, cols, values)
        assert list(fetch_result) == results

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
        database = self.database
        database.execute("DELETE FROM table1")
        database.insert_rows([
            (self.table1, (1, 'foo')),
            (self.table1, (2, 'bar')),
        ])

        if cols is not None:
            cols = [self.table1.cols[i] for i in cols]

        fetch_result = self.database.fetch_rows(self.table1, cols, values)
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
        database = self.database
        database.execute("DELETE FROM table1")
        database.insert_rows([
            (self.table1, (1, 'foo')),
            (self.table1, (2, 'bar')),
        ])

        pk_cols = [self.table1.cols[i] for i in pk_cols]
        cols = [self.table1.cols[i] for i in cols]
        database.update_rows([(self.table1, tuple(pk_cols),
                             tuple(pk_values), tuple(cols), tuple(values))])
        fetch_result = self.database.fetch_rows(self.table1, None, None)
        assert sorted(list(fetch_result)) == sorted(result)

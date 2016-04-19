import pytest
from minime.schema.sqlite import SqliteSchema


class TestSqliteSchema(object):
    test_relations_sql = [
        '''
            CREATE TABLE test1 (
                id INTEGER PRIMARY KEY,
                alt_id INTEGER UNIQUE
            );
        ''',

        '''
            CREATE TABLE test2 (
                id INTEGER PRIMARY KEY,
                fk1 INTEGER REFERENCES test1,
                fk2 INTEGER REFERENCES test1(id),
                fk3 INTEGER REFERENCES "test1",
                fk4 INTEGER REFERENCES test1("id"),
                fk5 INTEGER REFERENCES test1(alt_id),
                fk6 INTEGER CONSTRAINT test_constraint3 REFERENCES test1,
                fk7 INTEGER CONSTRAINT test_constraint4 REFERENCES test1(id),
                fk8 INTEGER,
                fk9 INTEGER,
                fk10 INTEGER,
                fk11 INTEGER,
                FOREIGN KEY(fk8) REFERENCES test1,
                FOREIGN KEY(fk9) REFERENCES test1(id)
                CONSTRAINT test_fk10 FOREIGN KEY(fk10) REFERENCES test1,
                CONSTRAINT test_fk11 FOREIGN KEY(fk11) REFERENCES test1(id)
            );
        ''',

        'ALTER TABLE test2 ADD COLUMN fk12 INTEGER REFERENCES test1;',
        'ALTER TABLE test2 ADD COLUMN fk13 INTEGER REFERENCES test1(id);',
    ]

    def test_schema_tables(self, sqlite_conn):
        sqlite_conn.execute('CREATE TABLE test1 (id INTEGER PRIMARY KEY);')
        sqlite_conn.execute('CREATE TABLE test2 (id INTEGER PRIMARY KEY);')

        schema = SqliteSchema.create_from_conn(sqlite_conn)
        assert 'test1' in schema.tables_by_name
        assert 'test2' in schema.tables_by_name
        assert len(schema.tables) == 2
        assert len(schema.tables_by_name.keys()) == 2
        assert str(schema.tables[0]) is not None
        assert repr(schema.tables[0]) is not None

    def test_schema_columns(self, sqlite_conn):
        sqlite_conn.execute('''
                CREATE TABLE test1 (
                    id INTEGER PRIMARY KEY,
                    not_null text NOT NULL,
                    nullable text
                );
            ''')

        schema = SqliteSchema.create_from_conn(sqlite_conn)
        assert len(schema.tables) == 1
        table = schema.tables[0]
        assert len(table.cols) == 3
        assert len(table.cols_by_name.keys()) == 3
        assert str(table.cols[0]) is not None
        assert repr(table.cols[0]) is not None

        names = [c.name for c in table.cols]
        assert 'id' in names
        assert 'not_null' in names
        assert 'nullable' in names
        id_col = filter(lambda r: r.name == 'id', table.cols)[0]
        not_null_col = filter(lambda r: r.name == 'not_null', table.cols)[0]
        nullable_col = filter(lambda r: r.name == 'nullable', table.cols)[0]
        assert id_col.notnull is False
        assert not_null_col.notnull is True
        assert nullable_col.notnull is False

    def test_schema_foreign_key_constraints(self, sqlite_conn):
        for sql in self.test_relations_sql:
            sqlite_conn.execute(sql)

        schema = SqliteSchema.create_from_conn(sqlite_conn)
        table1 = schema.tables[0]
        table2 = schema.tables[1]

        table1_id = table1.cols[0]
        table1_alt_id = table1.cols[1]

        assert len(table1.cols) == 2
        assert len(table2.cols) == 14
        assert len(table1.incoming_foreign_keys) == 13
        assert len(table2.foreign_keys) == 13

        fks_by_col = {}
        for i in range(1, 14):
            for fk in table2.foreign_keys:
                assert len(fk.src_cols) == 1
                assert len(fk.dst_cols) == 1
                fks_by_col[fk.src_cols[0]] = fk

        for i in range(1, 14):
            assert table2.cols[i] in fks_by_col
            assert fk in table1.incoming_foreign_keys
            assert str(fk) is not None
            assert repr(fk) is not None
            if fk.src_cols[0].name in ('fk10', 'fk11'):
                assert fk.name == 'test_%s' % fk.src_cols[0].name
            if fk.src_cols[0].name == 'fk5':
                assert fk.dst_cols == (table1_alt_id,)
            else:
                assert fk.dst_cols == (table1_id,)

    def test_schema_primary_key_constraints(self, sqlite_conn):
        sqls = [
            'CREATE TABLE test1 (id1 INTEGER PRIMARY KEY, name text);',
            'CREATE TABLE test2 (name text, id2 INTEGER PRIMARY KEY);',
            'CREATE TABLE test3 (id3 INTEGER);',
        ]
        for sql in sqls:
            sqlite_conn.execute(sql)

        schema = SqliteSchema.create_from_conn(sqlite_conn)

        assert schema.tables[0].primary_key == (schema.tables[0].cols[0],)
        assert schema.tables[1].primary_key == (schema.tables[1].cols[1],)
        assert schema.tables[2].primary_key is None

    def test_schema_compound_primary_key_constraints(self, sqlite_conn):
        sqlite_conn.execute('''
            CREATE TABLE test1 (
                id INTEGER,
                name TEXT,
                PRIMARY KEY(id, name)
            );
        ''')

        schema = SqliteSchema.create_from_conn(sqlite_conn)
        pk = schema.tables[0].primary_key
        assert pk == (schema.tables[0].cols[0], schema.tables[0].cols[1],)

    def test_schema_compound_foreign_key_constraints(self, sqlite_conn):
        for sql in [
            '''CREATE TABLE test1 (
                    id SERIAL PRIMARY KEY,
                    name TEXT,
                    UNIQUE(id, name)
                );''',
            '''CREATE TABLE test2 (
                    id SERIAL PRIMARY KEY,
                    fk1 INTEGER,
                    fk2 TEXT,
                    FOREIGN KEY(fk1, fk2) REFERENCES test1(id, name)
                );''']:
            sqlite_conn.execute(sql)

        with pytest.raises(Exception) as e:
            SqliteSchema.create_from_conn(sqlite_conn)
        assert 'Compound foreign keys are not supported on table' in str(e)

    def test_schema_non_existent_foreign_key_unknown_column(self, sqlite_conn):
        for sql in [
            '''CREATE TABLE test1 (
                    id SERIAL PRIMARY KEY
                );''',
            '''CREATE TABLE test2 (
                    id SERIAL PRIMARY KEY,
                    fk1 INTEGER,
                    FOREIGN KEY(fk1) REFERENCES test1(name)
                );''']:
            sqlite_conn.execute(sql)

        with pytest.raises(Exception) as e:
            SqliteSchema.create_from_conn(sqlite_conn)
        assert 'Unknown column' in str(e)

    def test_schema_non_existent_foreign_key_unknown_table(self, sqlite_conn):
        for sql in [
            '''CREATE TABLE test1 (
                    id SERIAL PRIMARY KEY
                );''',
            '''CREATE TABLE test2 (
                    id SERIAL PRIMARY KEY,
                    fk1 INTEGER,
                    FOREIGN KEY(fk1) REFERENCES foo(id)
                );''']:
            sqlite_conn.execute(sql)

        with pytest.raises(Exception) as e:
            SqliteSchema.create_from_conn(sqlite_conn)
        assert 'Unknown table' in str(e)

    def test_schema_unique_indexes(self, sqlite_conn):
        for sql in [
            '''CREATE TABLE test1 (
                    id SERIAL PRIMARY KEY,
                    col1 TEXT UNIQUE,
                    col2 TEXT UNIQUE,
                    col3 TEXT UNIQUE,
                    col4 TEXT UNIQUE,
                    UNIQUE(col1, col2)
                );
            ''',
            'CREATE INDEX index1 ON test1(col1, col2);',
            'CREATE UNIQUE INDEX uindex1 ON test1(col1, col2);',
            'CREATE UNIQUE INDEX uindex2 ON test1(col3, col4);'
        ]:
            sqlite_conn.execute(sql)

        schema = SqliteSchema.create_from_conn(sqlite_conn)
        assert len(schema.tables[0].unique_indexes) == 8  # including PK
        tuples = set()
        named_tuples = set()
        for ui in schema.tables[0].unique_indexes:
            assert str(ui) is not None
            assert repr(ui) is not None
            col_names = sorted([c.name for c in ui.cols])
            named_tuple_list = list(col_names)
            named_tuple_list.insert(0, ui.name)
            named_tuples.add(tuple(named_tuple_list))
            tuples.add(tuple(col_names))

        assert ('uindex1', 'col1', 'col2') in named_tuples
        assert ('uindex2', 'col3', 'col4') in named_tuples

        # One assertion for each index
        assert('id',) in tuples
        assert('col1',) in tuples
        assert('col2',) in tuples
        assert('col3',) in tuples
        assert('col4',) in tuples
        assert('col1', 'col2') in tuples
        assert('col3', 'col4') in tuples

        # Just because you're paranoid doesn't mean they aren't after you
        assert('col1', 'col3') not in tuples
        assert('col1', 'col4') not in tuples
        assert('col2', 'col3') not in tuples
        assert('col2', 'col4') not in tuples

import pytest
from minime.schema.sqlite import SqliteSchema


class TestSqliteSchema(object):
    test_relations_sql = [
        '''
            CREATE TABLE test1 (
                id INTEGER PRIMARY KEY
            );
        ''',

        '''
            CREATE TABLE test2 (
                id INTEGER PRIMARY KEY,
                fk1 INTEGER REFERENCES test1,
                fk2 INTEGER REFERENCES test1(id),
                fk3 INTEGER REFERENCES "test1",
                fk4 INTEGER REFERENCES test1("id"),
                fk5 INTEGER CONSTRAINT test_constraint3 REFERENCES test1,
                fk6 INTEGER CONSTRAINT test_constraint4 REFERENCES test1(id),
                fk7 INTEGER,
                fk8 INTEGER,
                fk9 INTEGER,
                fk10 INTEGER,
                FOREIGN KEY(fk7) REFERENCES test1,
                FOREIGN KEY(fk8) REFERENCES test1(id)
                CONSTRAINT test_fk9 FOREIGN KEY(fk9) REFERENCES test1,
                CONSTRAINT test_fk10 FOREIGN KEY(fk10) REFERENCES test1(id)
            );
        ''',

        'ALTER TABLE test2 ADD COLUMN fk11 INTEGER REFERENCES test1;',
        'ALTER TABLE test2 ADD COLUMN fk12 INTEGER REFERENCES test1(id);',
    ]

    def test_schema_tables(self, sqlite_conn):
        sqlite_conn.execute('CREATE TABLE test1 (id INTEGER PRIMARY KEY);')
        sqlite_conn.execute('CREATE TABLE test2 (id INTEGER PRIMARY KEY);')

        schema = SqliteSchema.create_from_conn(sqlite_conn)
        assert 'test1' in schema.tables_by_name
        assert 'test2' in schema.tables_by_name
        assert len(schema.tables) == 2
        assert len(schema.tables_by_name.keys()) == 2

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

        assert len(table1.cols) == 1
        assert len(table2.cols) == 13
        assert len(table1.incoming_fks) == 12
        assert len(table2.fks) == 12
        assert len(table2.fks_by_col) == 12
        for i in range(1, 13):
            assert table2.cols[i] in table2.fks_by_col
            fkc = table2.fks_by_col[table2.cols[i]]
            assert fkc in table1.incoming_fks
            if fkc.src_col.name in ('fk9', 'fk10'):
                assert fkc.name == 'test_%s' % fkc.src_col.name

    def test_schema_primary_key_constraints(self, sqlite_conn):
        sqls = [
            'CREATE TABLE test1 (id1 INTEGER PRIMARY KEY, name text);',
            'CREATE TABLE test2 (name text, id2 INTEGER PRIMARY KEY);',
            'CREATE TABLE test3 (id3 INTEGER);',
        ]
        for sql in sqls:
            sqlite_conn.execute(sql)

        schema = SqliteSchema.create_from_conn(sqlite_conn)

        assert schema.tables[0].primary_key == set([schema.tables[0].cols[0]])
        assert schema.tables[1].primary_key == set([schema.tables[1].cols[1]])
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
        assert pk == set([schema.tables[0].cols[0], schema.tables[0].cols[1]])

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

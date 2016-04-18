import tempfile
import pytest
import yaml
from minime.schema.postgresql import PostgresqlSchema


class TestPostgresqlSchema(object):
    test_relations_sql = '''
        CREATE TABLE test1 (
            id SERIAL PRIMARY KEY
        );

        CREATE TABLE test2 (
            id SERIAL PRIMARY KEY,
            fk1 INTEGER REFERENCES test1,
            fk2 INTEGER CONSTRAINT test_constraint REFERENCES test1
        );
    '''

    def test_schema_tables(self, postgresql_conn):
        with postgresql_conn.cursor() as cur:
            cur.execute('''
                CREATE TABLE test1 (id serial PRIMARY KEY);
                CREATE TABLE test2 (id serial PRIMARY KEY);
            ''')
        cur.close()

        schema = PostgresqlSchema.create_from_conn(postgresql_conn)
        assert 'test1' in schema.tables_by_name
        assert 'test2' in schema.tables_by_name
        assert len(schema.tables) == 2
        assert len(schema.tables_by_name.keys()) == 2
        assert len(schema.tables_by_oid.keys()) == 2

    def test_schema_columns(self, postgresql_conn):
        with postgresql_conn.cursor() as cur:
            cur.execute('''
                CREATE TABLE test1 (
                    id serial PRIMARY KEY,
                    not_null text NOT NULL,
                    nullable text
                );
            ''')
        cur.close()

        schema = PostgresqlSchema.create_from_conn(postgresql_conn)
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
        assert id_col.notnull is True
        assert not_null_col.notnull is True
        assert nullable_col.notnull is False

    def test_schema_foreign_key_constraints(self, postgresql_conn):
        with postgresql_conn.cursor() as cur:
            cur.execute(self.test_relations_sql)
        cur.close()

        schema = PostgresqlSchema.create_from_conn(postgresql_conn)
        table1 = schema.tables[0]
        table2 = schema.tables[1]

        assert len(table1.incoming_foreign_keys) == 2
        assert len(table2.foreign_keys) == 2
        fk1 = table2.foreign_keys[0]
        fk2 = table2.foreign_keys[1]
        assert fk1 in table1.incoming_foreign_keys
        assert fk2 in table1.incoming_foreign_keys
        assert fk1.name is not None
        assert fk2.name == 'test_constraint'
        assert str(fk1) is not None
        assert str(fk2) is not None
        assert repr(fk1) is not None
        assert repr(fk2) is not None

    def test_schema_compound_foreign_key_constraints(self, postgresql_conn):
        with postgresql_conn.cursor() as cur:
            cur.execute('''
                CREATE TABLE test1 (
                    id SERIAL PRIMARY KEY,
                    name TEXT,
                    UNIQUE(id, name)
                );

                CREATE TABLE test2 (
                    id SERIAL PRIMARY KEY,
                    fk1 INTEGER,
                    fk2 TEXT,
                    FOREIGN KEY(fk1, fk2) REFERENCES test1(id, name)
                );
        ''')
        cur.close()
        with pytest.raises(Exception) as e:
            PostgresqlSchema.create_from_conn(postgresql_conn)
        assert 'Compound foreign keys are not supported on table' in str(e)

    def test_schema_primary_key_constraints(self, postgresql_conn):
        with postgresql_conn.cursor() as cur:
            cur.execute('''
                CREATE TABLE test1 (id1 serial PRIMARY KEY, name text);
                CREATE TABLE test2 (name text, id2 serial PRIMARY KEY);
                CREATE TABLE test3 (id3 serial);
            ''')
        cur.close()

        schema = PostgresqlSchema.create_from_conn(postgresql_conn)

        assert schema.tables[0].primary_key == set([schema.tables[0].cols[0]])
        assert schema.tables[1].primary_key == set([schema.tables[1].cols[1]])
        assert schema.tables[2].primary_key is None

    def test_schema_compound_primary_key_constraints(self, postgresql_conn):
        with postgresql_conn.cursor() as cur:
            cur.execute('''
                CREATE TABLE test1 (
                    id INTEGER,
                    name TEXT,
                    PRIMARY KEY(id, name)
                );
        ''')
        cur.close()
        schema = PostgresqlSchema.create_from_conn(postgresql_conn)
        pk = schema.tables[0].primary_key
        assert pk == set([schema.tables[0].cols[0], schema.tables[0].cols[1]])

    def test_dump_relations(self, postgresql_conn):
        with postgresql_conn.cursor() as cur:
            cur.execute(self.test_relations_sql)
        cur.close()

        schema = PostgresqlSchema.create_from_conn(postgresql_conn)

        temp = tempfile.NamedTemporaryFile(mode='wb', delete=False)
        schema.dump_relations(temp)
        temp.close()

        data = yaml.load(open(temp.name).read())
        assert data == {'relations': [
            {'column': 'fk1', 'table': 'test2', 'name': 'test2_fk1_fkey'},
            {'column': 'fk2', 'table': 'test2', 'name': 'test_constraint'}]}

    def test_schema_unique_indexes(self, postgresql_conn):
        with postgresql_conn.cursor() as cur:
            cur.execute('''
                CREATE TABLE test1 (
                    id SERIAL PRIMARY KEY,
                    col1 TEXT UNIQUE,
                    col2 TEXT UNIQUE,
                    col3 TEXT UNIQUE,
                    col4 TEXT UNIQUE,
                    UNIQUE(col1, col2)
                );

                CREATE INDEX index1 ON test1(col1, col2);
                CREATE UNIQUE INDEX uindex1 ON test1(col1, col2);
                CREATE UNIQUE INDEX uindex2 ON test1(col3, col4);
        ''')

        schema = PostgresqlSchema.create_from_conn(postgresql_conn)
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

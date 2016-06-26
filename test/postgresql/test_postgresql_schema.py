import pytest
import tempfile
import yaml

from abridger.exc import RelationIntegrityError
from abridger.extraction_model import Relation
from abridger.schema import PostgresqlSchema
from test.conftest import got_postgresql


@pytest.mark.skipif(not got_postgresql(), reason='Needs postgresql')
class TestPostgresqlSchema(object):
    test_relations_sql = '''
        CREATE TABLE test1 (
            id SERIAL PRIMARY KEY,
            alt_id SERIAL UNIQUE,
            alt_id1 SERIAL,
            alt_id2 SERIAL,
            UNIQUE(alt_id1, alt_id2)
        );

        CREATE TABLE test2 (
            id SERIAL PRIMARY KEY,
            fk1 INTEGER REFERENCES test1,
            fk2 INTEGER CONSTRAINT test_constraint REFERENCES test1,
            fk3 INTEGER REFERENCES test1(alt_id),
            fk4 INTEGER,
            fk5 INTEGER,
            FOREIGN KEY (fk4, fk5) REFERENCES test1(alt_id1, alt_id2)
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
        assert len(list(schema.tables_by_name.keys())) == 2
        assert len(list(schema.tables_by_oid.keys())) == 2

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
        assert len(list(table.cols_by_name.keys())) == 3
        assert str(table.cols[0]) is not None
        assert repr(table.cols[0]) is not None

        names = [c.name for c in table.cols]
        assert 'id' in names
        assert 'not_null' in names
        assert 'nullable' in names

        id_col = list(
            filter(lambda r: r.name == 'id', table.cols))[0]
        not_null_col = list(
            filter(lambda r: r.name == 'not_null', table.cols))[0]
        nullable_col = list(
            filter(lambda r: r.name == 'nullable', table.cols))[0]

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

        table1_id = table1.cols[0]
        table1_alt_id = table1.cols[1]
        table1_alt_id1 = table1.cols[2]
        table1_alt_id2 = table1.cols[3]
        table2_fk1 = table2.cols[1]
        table2_fk2 = table2.cols[2]
        table2_fk3 = table2.cols[3]
        table2_fk4 = table2.cols[4]
        table2_fk5 = table2.cols[5]

        assert len(table1.incoming_foreign_keys) == 4
        assert len(table2.foreign_keys) == 4

        fk1 = table2.foreign_keys[0]
        fk2 = table2.foreign_keys[1]
        fk3 = table2.foreign_keys[2]
        fk4 = table2.foreign_keys[3]

        assert fk1 in table1.incoming_foreign_keys
        assert fk2 in table1.incoming_foreign_keys
        assert fk3 in table1.incoming_foreign_keys
        assert fk4 in table1.incoming_foreign_keys

        assert fk1.name is not None
        assert fk2.name == 'test_constraint'
        assert fk3.name is not None
        assert fk4.name is not None

        assert fk1.src_cols == (table2_fk1,)
        assert fk1.dst_cols == (table1_id,)
        assert fk2.src_cols == (table2_fk2,)
        assert fk2.dst_cols == (table1_id,)
        assert fk3.src_cols == (table2_fk3,)
        assert fk3.dst_cols == (table1_alt_id,)
        assert fk4.src_cols == (table2_fk4, table2_fk5)
        assert fk4.dst_cols == (table1_alt_id1, table1_alt_id2)

        assert str(fk1) is not None
        assert repr(fk1) is not None

    def test_schema_primary_key_constraints(self, postgresql_conn):
        with postgresql_conn.cursor() as cur:
            cur.execute('''
                CREATE TABLE test1 (id1 serial PRIMARY KEY, name text);
                CREATE TABLE test2 (name text, id2 serial PRIMARY KEY);
                CREATE TABLE test3 (id3 serial);
            ''')
        cur.close()

        schema = PostgresqlSchema.create_from_conn(postgresql_conn)

        assert schema.tables[0].primary_key == (schema.tables[0].cols[0],)
        assert schema.tables[1].primary_key == (schema.tables[1].cols[1],)
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
        assert pk == (schema.tables[0].cols[0], schema.tables[0].cols[1])

    def test_dump_relations(self, postgresql_conn):
        with postgresql_conn.cursor() as cur:
            cur.execute(self.test_relations_sql)
        cur.close()

        schema = PostgresqlSchema.create_from_conn(postgresql_conn)

        temp = tempfile.NamedTemporaryFile(mode='wt')
        schema.dump_relations(temp)
        temp.seek(0)
        data = yaml.load(open(temp.name).read())

        keys = [
            (['fk1'], 'test2', 'test2_fk1_fkey'),
            (['fk2'], 'test2', 'test_constraint'),
            (['fk3'], 'test2', 'test2_fk3_fkey'),
            (['fk4', 'fk5'], 'test2', 'test2_fk4_fkey'),
        ]

        expected_data = []
        for (cols, table, name) in keys:
            for is_incoming in [True, False]:
                row = {'table': table, 'name': name}
                if len(cols) == 1:
                    row['column'] = cols[0]
                else:
                    row['columns'] = cols
                if is_incoming:
                    row['type'] = Relation.TYPE_INCOMING
                expected_data.append(row)

        expected_data = {'relations': expected_data}
        assert data == expected_data

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
                CREATE UNIQUE INDEX uindex3 ON test1(col3, SUBSTR(col4, 4));
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

    def test_self_referencing_non_null_foreign_key(self, postgresql_conn):
        with postgresql_conn.cursor() as cur:
            for sql in [
                '''CREATE TABLE test1 (
                        id SERIAL PRIMARY KEY
                    );
                ''',
                '''ALTER TABLE test1 ADD COLUMN fk INTEGER NOT NULL DEFAULT 1
                    REFERENCES test1''',

                # Sanity test the above is even possible
                'INSERT INTO test1  (id) VALUES(1);'
            ]:
                cur.execute(sql)

        with pytest.raises(RelationIntegrityError):
            PostgresqlSchema.create_from_conn(postgresql_conn)

import tempfile
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

        assert len(table1.incoming_fks) == 2
        assert len(table2.fks) == 2
        assert len(table2.fks_by_col) == 2
        assert table2.cols[1] in table2.fks_by_col
        assert table2.cols[2] in table2.fks_by_col
        fkc1 = table2.fks_by_col[table2.cols[1]]
        fkc2 = table2.fks_by_col[table2.cols[2]]
        assert fkc1 in table1.incoming_fks
        assert fkc2 in table1.incoming_fks
        assert fkc1.name is not None
        assert fkc2.name == 'test_constraint'
        assert str(fkc1) is not None
        assert str(fkc2) is not None

    def test_schema_primary_key_constraints(self, postgresql_conn):
        with postgresql_conn.cursor() as cur:
            cur.execute('''
                CREATE TABLE test1 (id1 serial PRIMARY KEY, name text);
                CREATE TABLE test2 (name text, id2 serial PRIMARY KEY);
                CREATE TABLE test3 (id3 serial);
            ''')
        cur.close()

        schema = PostgresqlSchema.create_from_conn(postgresql_conn)

        assert schema.tables[0].pk == schema.tables[0].cols[0]
        assert schema.tables[1].pk == schema.tables[1].cols[1]
        assert schema.tables[2].pk is None

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

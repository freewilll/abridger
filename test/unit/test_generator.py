import pytest
from pprint import pprint

from minime.schema.sqlite import SqliteSchema
from minime.extraction_model import ExtractionModel, Relation
from minime.rocket import Rocket
from minime.generator import Generator
from rocket_platform import TestRocketBase


class TestGenerator(TestRocketBase):
    # Two unrelated tables
    @pytest.fixture()
    def schema1(self):
        for sql in [
            '''CREATE TABLE test2 (
                id INTEGER PRIMARY KEY,
                name TEXT
            );''',
            '''CREATE TABLE test1 (
                id INTEGER PRIMARY KEY,
                name TEXT
            );''',
        ]:
            self.dbconn.execute(sql)

        return SqliteSchema.create_from_conn(self.dbconn.connection)

    @pytest.fixture()
    # A single nullable dependency
    def schema2(self):
        for sql in [
            '''CREATE TABLE test2 (
                id INTEGER PRIMARY KEY,
                name TEXT
            );''',
            '''CREATE TABLE test1 (
                id INTEGER PRIMARY KEY,
                name TEXT,
                test2_id INTEGER REFERENCES test2
            );''',
        ]:
            self.dbconn.execute(sql)

        return SqliteSchema.create_from_conn(self.dbconn.connection)

    @pytest.fixture()
    # A single not null dependency
    def schema3(self):
        for sql in [
            '''CREATE TABLE test2 (
                id INTEGER PRIMARY KEY,
                name TEXT
            );''',
            '''CREATE TABLE test1 (
                id INTEGER PRIMARY KEY,
                name TEXT,
                test2_id INTEGER NOT NULL REFERENCES test2
            );''',
        ]:
            self.dbconn.execute(sql)

        return SqliteSchema.create_from_conn(self.dbconn.connection)

    @pytest.fixture()
    # Two level dependency
    def schema4(self):
        for sql in [
            '''CREATE TABLE test3 (
                id INTEGER PRIMARY KEY,
                name TEXT
            );''',
            '''CREATE TABLE test2 (
                id INTEGER PRIMARY KEY,
                name TEXT,
                test3_id INTEGER NOT NULL REFERENCES test3
            );''',
            '''CREATE TABLE test1 (
                id INTEGER PRIMARY KEY,
                name TEXT,
                test2_id INTEGER NOT NULL REFERENCES test2
            );''',
        ]:
            self.dbconn.execute(sql)

        return SqliteSchema.create_from_conn(self.dbconn.connection)

    @pytest.fixture()
    # A cycle of not null keys
    def schema5(self):
        for sql in [
            '''CREATE TABLE test3 (
                id INTEGER PRIMARY KEY,
                name TEXT,
                test1_id INTEGER NOT NULL REFERENCES test1
            );''',
            '''CREATE TABLE test2 (
                id INTEGER PRIMARY KEY,
                name TEXT,
                test3_id INTEGER NOT NULL REFERENCES test3
            );''',
            '''CREATE TABLE test1 (
                id INTEGER PRIMARY KEY,
                name TEXT,
                test2_id INTEGER NOT NULL REFERENCES test2
            );''',
        ]:
            self.dbconn.execute(sql)

        return SqliteSchema.create_from_conn(self.dbconn.connection)

    @pytest.fixture()
    def data1(self, schema1):
        table1 = schema1.tables[0]
        rows = [
            (table1, (1, 'a')),
            (table1, (2, 'b')),
            (table1, (3, 'c')),
            (table1, (4, 'c'))]
        self.dbconn.insert_rows(rows)
        return rows

    def get_genesis_instance(self, schema,
                             not_null_columns=None):
        if not_null_columns is None:
            not_null_columns = []
        extraction_model_data = [
            {'subject': [{'tables': [{'table': 'test1'}]}]},
            {'relations': [{'defaults': Relation.DEFAULT_EVERYTHING}]},
            {'not-null-columns': not_null_columns},
        ]
        extraction_model = ExtractionModel.load(schema, extraction_model_data)
        rocket = Rocket(self.dbconn, extraction_model)
        return Generator(schema, rocket)

    def check_table_order(self, schema, expected_table_order,
                          not_null_columns=None):
        genesis = self.get_genesis_instance(
            schema, not_null_columns=not_null_columns)
        table_order = [t.name for t in genesis.table_order]
        if table_order != expected_table_order:
            print
            print 'Got table order:'
            pprint(table_order)
            print 'Expected table order:'
            pprint(expected_table_order)
        assert table_order == expected_table_order

    def test_genesis_table_order1(self, schema1):
        self.check_table_order(schema1, ['test1', 'test2'])

    def test_genesis_table_order2a(self, schema2):
        self.check_table_order(schema2, ['test1', 'test2'])

    def test_genesis_table_order2b(self, schema2):
        # The table order should be swapped because of the not-null-columns
        # rule.
        self.check_table_order(
            schema2, ['test2', 'test1'],
            not_null_columns=[{'table': 'test1', 'column': 'test2_id'}])

    def test_genesis_table_order3(self, schema3):
        self.check_table_order(schema3, ['test2', 'test1'])

    def test_genesis_table_order4(self, schema4):
        self.check_table_order(schema4, ['test3', 'test2', 'test1'])

    def test_genesis_table_order5(self, schema5):
        with pytest.raises(Exception) as e:
            self.check_table_order(schema5, ['test3', 'test2', 'test1'])
        assert 'A cyclic dependency exists amongst' in str(e)

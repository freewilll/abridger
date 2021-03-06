from pprint import pprint
import pytest

from abridger.exc import CyclicDependencyError
from abridger.extraction_model import ExtractionModel, Relation
from abridger.extractor import Extractor
from abridger.generator import Generator
from abridger.schema import SqliteSchema
from test.unit.extractor.base import TestExtractorBase


class TestGenerator(TestExtractorBase):
    # Two unrelated tables
    @pytest.fixture()
    def schema1(self):
        for stmt in [
            '''CREATE TABLE test1 (
                id INTEGER PRIMARY KEY,
                name TEXT
            );''',
            '''CREATE TABLE test2 (
                id INTEGER PRIMARY KEY,
                name TEXT
            );''',
        ]:
            self.database.execute(stmt)

        return SqliteSchema.create_from_conn(self.database.connection)

    @pytest.fixture()
    def data1(self, schema1):
        table1 = schema1.tables[0]
        rows = [
            (table1, (1, 'a')),
            (table1, (2, 'b')),
            (table1, (3, 'c')),
            (table1, (4, 'c'))]
        self.database.insert_rows(rows)
        return rows

    @pytest.fixture()
    # A single nullable dependency
    def schema2(self):
        for stmt in [
            '''CREATE TABLE test1 (
                id INTEGER PRIMARY KEY,
                name TEXT,
                test2_id INTEGER REFERENCES test2
            );''',
            '''CREATE TABLE test2 (
                id INTEGER PRIMARY KEY,
                name TEXT
            );''',
        ]:
            self.database.execute(stmt)

        return SqliteSchema.create_from_conn(self.database.connection)

    @pytest.fixture()
    # A single nullable dependency. Same as above, but with
    # the table names are reversed
    def schema2rev(self):
        for stmt in [
            '''CREATE TABLE test2 (
                id INTEGER PRIMARY KEY,
                name TEXT,
                test1_id INTEGER REFERENCES test1
            );''',
            '''CREATE TABLE test1 (
                id INTEGER PRIMARY KEY,
                name TEXT
            );''',
        ]:
            self.database.execute(stmt)

        return SqliteSchema.create_from_conn(self.database.connection)

    @pytest.fixture()
    # A single not null dependency
    def schema3(self):
        for stmt in [
            '''CREATE TABLE test1 (
                id INTEGER PRIMARY KEY,
                name TEXT,
                test2_id INTEGER NOT NULL REFERENCES test2
            );''',
            '''CREATE TABLE test2 (
                id INTEGER PRIMARY KEY,
                name TEXT
            );''',
        ]:
            self.database.execute(stmt)

        return SqliteSchema.create_from_conn(self.database.connection)

    @pytest.fixture()
    # Two level dependency
    def schema4(self):
        for stmt in [
            '''CREATE TABLE test1 (
                id INTEGER PRIMARY KEY,
                name TEXT,
                test2_id INTEGER NOT NULL REFERENCES test2
            );''',
            '''CREATE TABLE test2 (
                id INTEGER PRIMARY KEY,
                name TEXT,
                test3_id INTEGER NOT NULL REFERENCES test3
            );''',
            '''CREATE TABLE test3 (
                id INTEGER PRIMARY KEY,
                name TEXT
            );''',
        ]:
            self.database.execute(stmt)

        return SqliteSchema.create_from_conn(self.database.connection)

    @pytest.fixture()
    # A cycle of not null keys
    def schema5(self):
        for stmt in [
            '''CREATE TABLE test1 (
                id INTEGER PRIMARY KEY,
                name TEXT,
                test2_id INTEGER NOT NULL REFERENCES test2
            );''',
            '''CREATE TABLE test2 (
                id INTEGER PRIMARY KEY,
                name TEXT,
                test3_id INTEGER NOT NULL REFERENCES test3
            );''',
            '''CREATE TABLE test3 (
                id INTEGER PRIMARY KEY,
                name TEXT,
                test1_id INTEGER NOT NULL REFERENCES test1
            );''',
        ]:
            self.database.execute(stmt)

        return SqliteSchema.create_from_conn(self.database.connection)

    @pytest.fixture()
    # A cycle of not null keys, with one nullable
    def schema6(self):
        for stmt in [
            '''CREATE TABLE test1 (
                id INTEGER PRIMARY KEY,
                test2_id INTEGER NOT NULL REFERENCES test2
            );''',
            '''CREATE TABLE test2 (
                id INTEGER PRIMARY KEY,
                test1_id INTEGER REFERENCES test1,
                test3_id INTEGER NOT NULL REFERENCES test3
            );''',
            '''CREATE TABLE test3 (
                id INTEGER PRIMARY KEY,
                test1_id INTEGER REFERENCES test1
            );''',
            '''CREATE TABLE test4 (
                id INTEGER PRIMARY KEY,
                test4_id INTEGER REFERENCES test4
            );''',
        ]:
            self.database.execute(stmt)

        return SqliteSchema.create_from_conn(self.database.connection)

    # Tables without primary keys tests
    @pytest.fixture()
    def schema7(self):
        for stmt in [
            # No primary key, no unique indexes
            '''CREATE TABLE test1 (id INTEGER);''',

            # No primary key, with a unique index
            '''CREATE TABLE test2 (id INTEGER UNIQUE);''',
        ]:
            self.database.execute(stmt)

        return SqliteSchema.create_from_conn(self.database.connection)

    @pytest.fixture()
    def data7(self, schema7):
        table1 = schema7.tables[0]
        table2 = schema7.tables[1]
        rows = [
            (table1, (1,)),
            (table1, (1,)),
            (table1, (2,)),
            (table2, (1,)),
            (table2, (2,)),
        ]
        self.database.insert_rows(rows)
        return rows

    def get_generator_instance(self, schema, not_null_cols=None,
                               table='test1'):
        if not_null_cols is None:
            not_null_cols = []
        extraction_model_data = [
            {'subject': [{'tables': [{'table': table}]}]},
            {'relations': [{'defaults': Relation.DEFAULT_EVERYTHING}]},
            {'not-null-columns': not_null_cols},
        ]
        extraction_model = ExtractionModel.load(schema, extraction_model_data)
        extractor = Extractor(self.database, extraction_model)
        return Generator(schema, extractor)

    def check_table_order(self, schema, expected_table_order,
                          not_null_cols=None,
                          expected_deferred_update_rules=None):
        generator = self.get_generator_instance(
            schema, not_null_cols=not_null_cols,
            table=expected_table_order[0])
        table_order = [t.name for t in generator.table_order]
        if table_order != expected_table_order:
            print()
            print('Got table order:')
            pprint(table_order)
            print('Expected table order:')
            pprint(expected_table_order)
        assert table_order == expected_table_order

        if (expected_deferred_update_rules is not None and
                generator.deferred_update_rules !=
                expected_deferred_update_rules):
            print()
            print('Got deferred update rules:')
            pprint(generator.deferred_update_rules)
            print('Expected deferred update rules:')
            pprint(expected_deferred_update_rules)
            assert generator.deferred_update_rules == \
                expected_deferred_update_rules

    def test_table_order1(self, schema1):
        self.check_table_order(schema1, ['test1', 'test2'])

    def test_table_order2a(self, schema2):
        expected_deferred_update_rules = {
            schema2.tables[0]: set([schema2.tables[0].cols[2]]),
            schema2.tables[1]: set(),
        }
        self.check_table_order(
            schema2, ['test1', 'test2'],
            expected_deferred_update_rules=expected_deferred_update_rules)

    def test_table_order2alt_a(self, schema2rev):
        # Compared to schema2, there are no deferred update rules,
        # since test1 comes before test2 in the table order,
        # so test2's insert can go ahead.
        expected_deferred_update_rules = {
            schema2rev.tables[1]: set(),
            schema2rev.tables[0]: set(),
        }
        self.check_table_order(
            schema2rev, ['test1', 'test2'],  # alphabetical
            expected_deferred_update_rules=expected_deferred_update_rules)

    def test_table_order2b(self, schema2):
        # The table order should be swapped because of the not-null-columns
        # rule.
        expected_deferred_update_rules = {
            schema2.tables[0]: set(),
            schema2.tables[1]: set(),
        }
        self.check_table_order(
            schema2, ['test2', 'test1'],
            not_null_cols=[{'table': 'test1', 'column': 'test2_id'}],
            expected_deferred_update_rules=expected_deferred_update_rules)

    def test_table_order3(self, schema3):
        self.check_table_order(schema3, ['test2', 'test1'])

    def test_table_order4(self, schema4):
        self.check_table_order(schema4, ['test3', 'test2', 'test1'])

    def test_table_order5(self, schema5):
        with pytest.raises(CyclicDependencyError) as e:
            self.check_table_order(schema5, ['test3', 'test2', 'test1'])
        assert 'test1, test2, test3' in str(e)

    def test_table_order6(self, schema6):
        self.check_table_order(schema6, ['test3', 'test4', 'test2', 'test1'])

    def test_deferred_updates(self, schema6):
        generator = self.get_generator_instance(schema6)
        assert generator.deferred_update_rules == {
            schema6.tables[0]: set(),                             # test1
            schema6.tables[1]: set([schema6.tables[1].cols[1]]),  # test2
            schema6.tables[2]: set([schema6.tables[2].cols[1]]),  # test3
            schema6.tables[3]: set([schema6.tables[3].cols[1]]),  # test4
        }

    def test_statements_self_ref(self, schema6):
        table4 = schema6.tables[3]
        rows = [(table4, (1, 1))]
        self.database.insert_rows(rows)
        generator = self.get_generator_instance(schema6, table='test4')
        generator.extractor.launch()
        generator.generate_statements()

        assert generator.insert_statements == [(table4, (1, None))]
        assert generator.update_statements == [(
            table4,
            (table4.cols[0],), (1,),
            (table4.cols[1],), (1,))]

    def check_statements(self, generator, expected_insert_statements,
                         expected_update_statments):
        if generator.insert_statements != expected_insert_statements:
            print()
            print('Insert statements mismatch')
            print('Got:')
            pprint(generator.insert_statements)
            print('Expected:')
            pprint(expected_insert_statements)

        if generator.update_statements != expected_update_statments:
            print()
            print('Update statements mismatch')
            print('Got:')
            pprint(generator.update_statements)
            print('Expected:')
            pprint(expected_update_statments)

        assert generator.insert_statements == expected_insert_statements
        assert generator.update_statements == expected_update_statments

    def test_statements_deferred_updates(self, schema6):
        table1 = schema6.tables[0]
        table2 = schema6.tables[1]
        table3 = schema6.tables[2]

        inserts = [
            (table3, (1, None)),
            (table2, (1, None, 1)),
            (table1, (1, 1)),
        ]
        self.database.insert_rows(inserts)

        updates = [
            (table3, (table3.cols[0],), (1,), (table3.cols[1],), (1,)),
        ]
        self.database.update_rows(updates)

        generator = self.get_generator_instance(schema6, table='test3')
        generator.extractor.launch()
        generator.generate_statements()
        self.check_statements(generator, inserts, updates)

    @pytest.mark.parametrize('table, start, end', [
        ('test1', 0, 3),
        ('test2', 3, 5),
    ])
    def test_statements_no_pk_no_index(self, schema7, data7, table, start,
                                       end):
        generator = self.get_generator_instance(schema7, table=table)
        generator.extractor.launch()
        generator.generate_statements()
        self.check_statements(generator, data7[start:end], [])

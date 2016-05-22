import pytest

from minime.schema import SqliteSchema
from minime.extraction_model import ExtractionModel
from minime.rocket import Rocket
from rocket_platform import TestRocketBase


class TestRocketBasics(TestRocketBase):
    @pytest.fixture()
    def schema1(self):
        self.dbconn.execute('''
            CREATE TABLE test1 (id INTEGER PRIMARY KEY, name TEXT);
        ''')
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

    @pytest.fixture()
    def schema2(self):
        for sql in [
            '''
                CREATE TABLE test1 (
                    id INTEGER PRIMARY KEY
                );
            ''', '''
                CREATE TABLE test2 (
                    id INTEGER PRIMARY KEY,
                    test1_id INTEGER REFERENCES test1
                );
            ''',
        ]:
            self.dbconn.execute(sql)
        return SqliteSchema.create_from_conn(self.dbconn.connection)

    @pytest.fixture()
    def data2(self, schema2):
        table1 = schema2.tables[0]
        table2 = schema2.tables[1]
        rows = [
            (table1, (1,)),
            (table1, (2,)),
            (table2, (1, 1)),
            (table2, (2, 1)),
            (table2, (3, 2)),
            (table2, (4, 2)),
            (table2, (5, None)),
        ]
        self.dbconn.insert_rows(rows)
        return rows

    @pytest.fixture()
    def schema3(self):
        for sql in [
            '''
                CREATE TABLE test1 (
                    id INTEGER PRIMARY KEY,
                    alt_id INTEGER UNIQUE
                );
            ''', '''
                CREATE TABLE test2 (
                    id INTEGER PRIMARY KEY,
                    alt_test1_id INTEGER NOT NULL REFERENCES test1(alt_id)
                );
            ''',
        ]:
            self.dbconn.execute(sql)
        return SqliteSchema.create_from_conn(self.dbconn.connection)

    @pytest.fixture()
    def data3(self, schema3):
        table1 = schema3.tables[0]
        table2 = schema3.tables[1]
        rows = [
            (table1, (1, 100)),
            (table1, (2, 200)),
            (table2, (1, 100)),
            (table2, (2, 200)),
        ]
        self.dbconn.insert_rows(rows)
        return rows

    def test_one_subject_one_table(self, schema1, data1):
        table = {'table': 'test1'}
        self.check_one_subject(schema1, [table], data1)

    @pytest.mark.parametrize('col, values, start, end', [
        ('id', 1, 0, 1),
        ('id', [1], 0, 1),
        ('id', [2], 1, 2),
        ('id', [1, 2], 0, 2),
        ('id', [100], 0, 0),
        ('name', 'a', 0, 1),
        ('name', ['a'], 0, 1),
        ('name', ['b'], 1, 2),
        ('name', ['a', 'b'], 0, 2),
        ('name', ['c'], 2, 4),
        ('name', ['unknown'], 0, 0),
    ])
    def test_one_subject_one_filtered_table(
            self, schema1, data1, values, col, start, end):
        table = {'table': 'test1', 'column': col, 'values': values}
        self.check_one_subject(schema1, [table], data1[start:end])

    def test_one_subject_two_tables(self,  schema1, data1):
        tables = [
            {'table': 'test1', 'column': 'id', 'values': 1},
            {'table': 'test1', 'column': 'name', 'values': 'b'},
        ]
        self.check_one_subject(schema1, tables, data1[0:2])

        tables = [
            {'table': 'test1', 'column': 'id', 'values': 1},
            {'table': 'test1', 'column': 'name', 'values': 'unknown'},
        ]
        self.check_one_subject(schema1, tables, data1[0:1])

    def test_one_subject_row_cache(self, schema1, data1):
        # This should only issue one query
        tables = [
            {'table': 'test1', 'column': 'id', 'values': 1},
            {'table': 'test1', 'column': 'id', 'values': 1},
        ]
        self.check_one_subject(schema1, tables, data1[0:1],
                               expected_fetch_count=1)

    def test_two_subjects_row_cache1(self, schema1, data1):
        # The primary key is cached, so this should only result in one query
        table1 = {'table': 'test1', 'column': 'id', 'values': 1}
        table2 = {'table': 'test1', 'column': 'id', 'values': 1}
        self.check_two_subjects(schema1, [table1, table2], data1[0:1],
                                expected_fetch_count=1)

    def test_two_subjects_row_cache2(self, schema1, data1):
        # Nothing can be cached for this one
        table1 = {'table': 'test1', 'column': 'id', 'values': 3}
        table2 = {'table': 'test1', 'column': 'id', 'values': 4}
        self.check_two_subjects(schema1, [table1, table2], data1[2:4],
                                expected_fetch_count=2)

    def test_two_subjects_row_cache3(self, schema1, data1):
        # Nothing can be cached for this one
        table1 = {'table': 'test1', 'column': 'id', 'values': 3}
        table2 = {'table': 'test1', 'column': 'name', 'values': 'c'}
        self.check_two_subjects(schema1, [table1, table2], data1[2:4],
                                expected_fetch_count=2)

    @pytest.mark.parametrize('values, rows', [
        ([], [0, 1, 2, 3, 4, 5, 6]),
        ([1, 2, 3, 4], [0, 1, 2, 3, 4, 5]),
        ([1, 2, 3, 4, 5], [0, 1, 2, 3, 4, 5, 6]),
        ([1], [0, 2]),
        ([2], [0, 3]),
        ([3], [1, 4]),
        ([4], [1, 5]),
        ([5], [6]),
        ([1, 2], [0, 2, 3]),
        ([3, 4], [1, 4, 5]),
        ([1, 3], [0, 1, 2, 4]),
        ([2, 4], [0, 1, 3, 5]),
    ])
    def test_one_subject_two_tables_with_fk(self, schema2, data2, values,
                                            rows):
        table = {'table': 'test2'}
        if values:
            table = {'table': 'test2', 'column': 'id', 'values': values}
        expected_data = [data2[r] for r in rows]
        self.check_one_subject(schema2, [table], expected_data)

    @pytest.mark.parametrize('values, rows', [
        ([], [0, 1, 2, 3]),
        ([1, 2], [0, 1, 2, 3]),
        ([1], [0, 2]),
        ([2], [1, 3]),
        ([1, 2], [0, 1, 2, 3]),
    ])
    def test_one_subject_two_tables_with_alt_fk(self, schema3, data3, values,
                                                rows):
        table = {'table': 'test2'}
        if values:
            table = {'table': 'test2', 'column': 'id', 'values': values}
        expected_data = [data3[r] for r in rows]
        self.check_one_subject(schema3, [table], expected_data)

    @pytest.mark.parametrize(
        'values, rows, with_relation', [
            ([], [0, 1], False),
            ([], [0, 1, 2, 3, 4, 5], True),
            ([1, 2], [0, 1, 2, 3, 4, 5], True),
            ([1], [0, 2, 3], True),
            ([2], [1, 4, 5], True),
        ])
    def test_two_tables_relations(self, schema2, data2, values, rows,
                                  with_relation):
        table = {'table': 'test1'}
        if values:
            table = {'table': 'test1', 'column': 'id', 'values': values}
        if with_relation:
            relations = [{'table': 'test2', 'column': 'test1_id'}]
        else:
            relations = []
        expected_data = [data2[r] for r in rows]
        self.check_one_subject(schema2, [table], expected_data,
                               relations=relations)

    def test_two_tables_global_relations(self, schema2, data2):
        table = {'table': 'test1'}
        global_relations = [{'table': 'test2', 'column': 'test1_id'}]
        self.check_one_subject(schema2, [table], data2[0:6],
                               global_relations=global_relations)

    def test_two_tables_double_overlapping_subject(self, schema2, data2):
        extraction_model_data = [
            {'relations': [{'table': 'test2', 'column': 'test1_id'}]},
            {'subject': [{'tables': [{'table': 'test1'}]}]},
            {'subject': [{'tables': [{'table': 'test2'}]}]},
        ]
        extraction_model = ExtractionModel.load(schema2, extraction_model_data)
        rocket = Rocket(self.dbconn, extraction_model).launch()
        assert rocket.flat_results() == data2

    def test_results_row_str_and_repr(self, schema1, data1):
        extraction_model_data = [
            {'subject': [{'tables': [{'table': 'test1'}]}]},
        ]
        extraction_model = ExtractionModel.load(schema1, extraction_model_data)
        rocket = Rocket(self.dbconn, extraction_model).launch()
        table1 = schema1.tables[0]
        result_rows = rocket.results[table1][table1.primary_key]
        for result_row in result_rows.values():
            assert repr(result_row) is not None

    @pytest.mark.xfail  # TODO relations without a column on a subject
    def test_relation_without_a_column(self, schema2, data2):
        extraction_model_data = [
            {'relations': [{'table': 'test1'}]},
            {'subject': [{'tables': [{'table': 'test2',
                                      'column': 'id', 'values': 1}]}]},
        ]
        extraction_model = ExtractionModel.load(schema2, extraction_model_data)
        rocket = Rocket(self.dbconn, extraction_model).launch()
        # Everything in test 1 and one row in test2
        assert rocket.flat_results() == data2[0:2] + data2[2:3]

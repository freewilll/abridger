import pytest

from abridger.schema import SqliteSchema
from test.unit.extractor.base import TestExtractorBase


class TestExtractorBasics(TestExtractorBase):
    @pytest.fixture()
    def schema1(self):
        for stmt in [
            '''
                CREATE TABLE test1 (
                    id1 INTEGER,
                    id2 INTEGER,
                    PRIMARY KEY(id1, id2)

                );
            ''', '''
                CREATE TABLE test2 (
                    id INTEGER PRIMARY KEY,
                    test1_id INTEGER,
                    test2_id INTEGER,
                    FOREIGN KEY(test1_id, test2_id) REFERENCES test1
                );
            ''',
        ]:
            self.database.execute(stmt)
        return SqliteSchema.create_from_conn(self.database.connection)

    @pytest.fixture()
    def data1(self, schema1):
        table1 = schema1.tables[0]
        table2 = schema1.tables[1]
        rows = [
            (table1, (1, 1)),
            (table1, (2, 2)),
            (table2, (1, 1, 1)),
            (table2, (2, 1, 1)),
            (table2, (3, 2, 2)),
            (table2, (4, 2, 2)),
            (table2, (5, None, None)),
        ]
        self.database.insert_rows(rows)
        return rows

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
    def test_one_subject_two_tables_with_fk(self, schema1, data1, values,
                                            rows):
        table = {'table': 'test2'}
        if values:
            table = {'table': 'test2', 'column': 'id', 'values': values}
        expected_data = [data1[r] for r in rows]
        self.check_one_subject(schema1, [table], expected_data)

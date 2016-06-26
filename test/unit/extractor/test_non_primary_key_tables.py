import pytest

from abridger.extraction_model import Relation
from abridger.schema import SqliteSchema
from test.unit.extractor.base import TestExtractorBase


class TestExtractorNonPrimaryKeyTables(TestExtractorBase):
    REL_EVERYTHING = [{'defaults': Relation.DEFAULT_EVERYTHING}]

    schema1 = [
        '''CREATE TABLE test1 (
                id INTEGER PRIMARY KEY
           );''',
        '''CREATE TABLE test2 (
                test1_id INTEGER REFERENCES test1
           );''',
    ]

    # No index, no duplicates
    s1d1 = [
        ('test1', (1,)),
        ('test1', (2,)),
        ('test2', (1,)),
        ('test2', (2,)),
    ]

    # No index, with duplicates
    s1d2 = [
        ('test1', (1,)),
        ('test1', (2,)),
        ('test2', (1,)), ('test2', (1,)),
        ('test2', (2,)), ('test2', (2,)),
    ]

    schema2 = [
        '''CREATE TABLE test1 (
                id INTEGER PRIMARY KEY
           );''',
        '''CREATE TABLE test2 (
                test1_id INTEGER REFERENCES test1 UNIQUE
           );''',
    ]

    # With index, no duplicates are allowed
    s2d1 = [
        ('test1', (1,)),
        ('test1', (2,)),
        ('test2', (1,)),
        ('test2', (2,)),
    ]

    schema3 = [
        '''CREATE TABLE test1 (
                id INTEGER PRIMARY KEY
           );''',
        '''CREATE TABLE test2 (
                test1_id INTEGER REFERENCES test1,
                id2 INTEGER,
                UNIQUE(test1_id, id2)
           );''',
    ]

    # With index on two columns
    s3d1 = [
        ('test1', (1,)),
        ('test1', (2,)),
        ('test2', (1, 1)),
        ('test2', (1, 2)),
        ('test2', (2, 1)),
        ('test2', (2, 2)),
    ]

    schema4 = [
        '''CREATE TABLE test1 (
                id INTEGER PRIMARY KEY
           );''',
        '''CREATE TABLE test2 (
                test1_id INTEGER REFERENCES test1,
                id2 INTEGER
           );''',
    ]

    # No index, two columns, no duplicates
    s4d1 = [
        ('test1', (1,)),
        ('test1', (2,)),
        ('test2', (1, 1)),
        ('test2', (1, 2)),
        ('test2', (2, 1)),
        ('test2', (2, 2)),
    ]

    # No index, two columns, withduplicates
    s4d2 = [
        ('test1', (1,)),
        ('test1', (2,)),
        ('test2', (1, 1)), ('test2', (1, 1)),
        ('test2', (1, 2)), ('test2', (1, 2)),
        ('test2', (2, 1)), ('test2', (2, 1)),
        ('test2', (2, 2)), ('test2', (2, 2)),
    ]

    test_cases = [
        (schema1, s1d1),
        (schema1, s1d2),
        (schema2, s2d1),
        (schema3, s3d1),
        (schema4, s4d1),
        (schema4, s4d2),
    ]

    @pytest.mark.parametrize('schema, data', test_cases)
    def test_non_primary_key_table(self, schema, data):
        for stmt in schema:
            self.database.execute(stmt)
        schema = SqliteSchema.create_from_conn(self.database.connection)
        for i, datum in enumerate(data):
            (table_name, row_values) = datum
            table = schema.tables_by_name[table_name]
            data[i] = (table, row_values)
        self.database.insert_rows(data)
        self.check_one_subject(schema, [{'table': 'test1'}], data,
                               global_relations=self.REL_EVERYTHING)

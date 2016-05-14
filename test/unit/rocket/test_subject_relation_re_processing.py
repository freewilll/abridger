import pytest

from minime.schema.sqlite import SqliteSchema
from rocket_platform import TestRocketBase


class TestRocketSubjectRelationReProcessing(TestRocketBase):
    @pytest.fixture()
    def schema1(self):
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
            ''', '''
                CREATE TABLE test3 (
                    id INTEGER PRIMARY KEY,
                    test2_id INTEGER REFERENCES test2
                );
            ''', '''
                CREATE TABLE test4 (
                    id INTEGER PRIMARY KEY,
                    test1_id INTEGER REFERENCES test1,
                    test2_id INTEGER REFERENCES test2
                );
            ''',
        ]:
            self.dbconn.execute(sql)
        return SqliteSchema.create_from_conn(self.dbconn.connection)

    @pytest.fixture()
    def data1(self, schema1):
        table1 = schema1.tables[0]
        table2 = schema1.tables[1]
        table3 = schema1.tables[2]
        table4 = schema1.tables[3]
        rows = [
            (table1, (1,)),
            (table2, (1, 1)),
            (table3, (1, 1)),
            (table4, (1, 1, 1)),
        ]
        self.dbconn.insert_rows(rows)
        return rows

    def test_subject_relation_re_processing(self, schema1, data1):
        # 1 <-  2 <-  3
        # ^     ^
        #  \   /
        #    4

        # The rocket algorithm goes breadth first. In this example,
        # the test2 table is hit twice. However the first time it is hit,
        # it has less relationships, so it won't pull in test3.
        # The second subject includes test3 and test4. test2 will only get
        # processed when test2 has already been seen by subject 1.
        # This test ensures that test2 is re-processed due to subject 2
        # having more relationships.

        rel21 = {'table': 'test2', 'column': 'test1_id'}
        rel32 = {'table': 'test3', 'column': 'test2_id'}
        rel41 = {'table': 'test4', 'column': 'test1_id'}

        extraction_model_data = [
            # This subject won't include test3, only test 2
            {
                'subject': [
                    {'tables': [{'table': 'test1'}]},
                    {'relations': [rel21]},
                ]
            },

            # This subject will include test3 via test4
            {
                'subject': [
                    {'tables': [{'table': 'test1'}]},
                    {'relations': [rel41, rel32]},
                ]
            }
        ]
        self.check_launch(schema1, extraction_model_data, data1)

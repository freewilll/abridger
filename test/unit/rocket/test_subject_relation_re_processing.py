import pytest

from minime.extraction_model import Relation
from minime.schema import SqliteSchema
from test.unit.rocket.rocket_platform import TestRocketBase


class TestRocketSubjectRelationReProcessingIncoming(TestRocketBase):
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


class TestRocketTwoSubjectTwoColumnNulling(TestRocketBase):
    TEST_CASES = []

    for i in (True, False):
        for j in (True, False):
            for k in (True, False):
                for l in (True, False):
                    TEST_CASES.append([i, j, k, l])

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
                    test1_id INTEGER REFERENCES test1,
                    test3_id INTEGER REFERENCES test3,
                    test5_id INTEGER REFERENCES test5
                );
            ''', '''
                CREATE TABLE test3 (
                    id INTEGER PRIMARY KEY
                );
            ''', '''
                CREATE TABLE test5 (
                    id INTEGER PRIMARY KEY
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
        table5 = schema1.tables[4]

        rows = [
            (table1, (1,)),
            (table3, (1,)),
            (table5, (1,)),
            (table2, (1, 1, 1, 1)),
            (table4, (1, 1, 1)),
        ]
        self.dbconn.insert_rows(rows)
        return rows

    @pytest.mark.parametrize('i, j, k, l', TEST_CASES)
    def test_nulling(self, schema1, data1, i, j, k, l):
        #           5
        #         ^
        #        /
        # 1 <-  2 ->  3
        # ^     ^
        #  \   /
        #    4

        # The rocket algorithm goes breadth first. By testing with two
        # subjects, things can be rigged so that the test2 table is processed
        # twice, with different relationships.
        #
        # This tests checks that two outgoing relations on the test2 table
        # are processed correctly. If a row in test3 or test5 is not needed,
        # then the column on test2 should be made null.
        #
        # The 16 combinations are:
        #    relationship from 2-> 3 enabled/disabled for subject 1  -- i
        #    relationship from 2-> 3 enabled/disabled for subject 1  -- j
        #    relationship from 2-> 5 enabled/disabled for subject 2  -- k
        #    relationship from 2-> 5 enabled/disabled for subject 2  -- l

        table2 = schema1.tables[1]

        rel21 = {'table': 'test2', 'column': 'test1_id'}
        rel41 = {'table': 'test4', 'column': 'test1_id'}

        # Outgoing relations are enabled by default.
        rel23d = {'table': 'test2', 'column': 'test3_id', 'disabled': True,
                  'type': Relation.TYPE_OUTGOING}
        rel25d = {'table': 'test2', 'column': 'test5_id', 'disabled': True,
                  'type': Relation.TYPE_OUTGOING}

        # Incoming relations
        relations = [[rel21], [rel41]]

        # Disable outgoing relations
        if not i:
            relations[0].append(rel23d)
        if not j:
            relations[0].append(rel25d)
        if not k:
            relations[1].append(rel23d)
        if not l:
            relations[1].append(rel25d)

        expect3 = 1 if i or k else None     # Expect a non-None in test3_id
        expect5 = 1 if j or l else None     # Expect a non-None in test5_id

        expected_data = data1[0:1] + data1[4:5]
        expected_data.append((table2, (1, 1, expect3, expect5)))

        if expect3:
            expected_data += data1[1:2]  # Expect a row in test3
        if expect5:
            expected_data += data1[2:3]  # Expect a row in test5

        extraction_model_data = [
            {'subject': [
                {'tables': [{'table': 'test1'}]},
                {'relations': relations[0]}]},
            {'subject': [
                {'tables': [{'table': 'test1'}]},
                {'relations': relations[1]}]}
        ]

        self.check_launch(schema1, extraction_model_data, expected_data)

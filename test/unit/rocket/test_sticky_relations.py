import pytest

from minime.schema import SqliteSchema
from rocket_platform import TestRocketBase
from minime.extraction_model import Relation


class TestRocketStickyRelations(TestRocketBase):
    @pytest.fixture()
    def schema_out(self):
        '''
            test1 -> sticky         -> test3 <- test2
                  -> non_sticky     -> test3 <- test2
        '''
        for sql in [
            '''
                CREATE TABLE non_sticky (
                    id INTEGER PRIMARY KEY,
                    test3_id INTEGER REFERENCES test3
                );
            ''', '''
                CREATE TABLE sticky (
                    id INTEGER PRIMARY KEY,
                    test3_id INTEGER REFERENCES test3
                );
            ''', '''
                CREATE TABLE test1 (
                    id INTEGER PRIMARY KEY,
                    sticky INTEGER REFERENCES sticky,
                    non_sticky INTEGER REFERENCES non_sticky
                );
            ''', '''
                CREATE TABLE test2 (
                    id INTEGER PRIMARY KEY,
                    test3_id INTEGER REFERENCES test3
                );
            ''', '''
                CREATE TABLE test3 (
                    id INTEGER PRIMARY KEY
                );
            ''',
        ]:
            self.dbconn.execute(sql)
        return SqliteSchema.create_from_conn(self.dbconn.connection)

    @pytest.fixture()
    def data_out(self, schema_out):
        non_sticky = schema_out.tables[0]
        sticky = schema_out.tables[1]
        table1 = schema_out.tables[2]
        table2 = schema_out.tables[3]
        table3 = schema_out.tables[4]

        rows = [
            (table3, (1,)),
            (table3, (2,)),
            (table2, (1, 1)),
            (table2, (2, 2)),
            (sticky, (1, 1)),
            (non_sticky, (1, 2)),
            (table1, (1, 1, None)),
            (table1, (2, None, 1)),
        ]
        self.dbconn.insert_rows(rows)

        self.data_everything_except_table2 = (
            rows[0:2] +  # table 3
            rows[4:6] +  # sticky and non_sticky
            rows[6:8]    # table 1
        )

        self.data_everything_except_table2_non_sticky_row = (
            rows[0:2] +  # table 3
            rows[4:6] +  # sticky and non_sticky
            rows[6:8] +  # table 1
            rows[2:3]    # table 2, row 1
        )

        return rows

    def test_sticky_relations1(self, schema_out, data_out):
        # Check fetch without any relations, which won't grab any rows in
        # table 2
        table = {'table': 'test1'}

        self.check_one_subject(schema_out, [table],
                               self.data_everything_except_table2)

    def test_sticky_relations2(self, schema_out, data_out):
        # Check fetch without sticky relations, which grabs everything
        table = {'table': 'test1'}
        relation = {'table': 'test2', 'column': 'test3_id'}
        self.check_one_subject(schema_out, [table], data_out,
                               relations=[relation])

    def test_sticky_relations3(self, schema_out, data_out):
        # Check fetch without sticky relations, but flag test2 as sticky
        # this should not fetch anything in test2 since there is no sticky
        # trail
        table = {'table': 'test1'}

        def outgoing_sticky_rel(table, column):
            return {'table': table, 'column': column, 'sticky': True,
                    'type': Relation.TYPE_OUTGOING}

        relations = [
            outgoing_sticky_rel('test1', 'sticky'),
            outgoing_sticky_rel('sticky', 'test3_id'),
            outgoing_sticky_rel('test2', 'test3_id'),
            {'table': 'test2', 'column': 'test3_id', 'sticky': True},
        ]

        self.check_one_subject(
            schema_out, [table],
            self.data_everything_except_table2_non_sticky_row,
            relations=relations)

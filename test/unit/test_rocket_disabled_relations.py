import pytest

from minime.schema.sqlite import SqliteSchema
from rocket_platform import TestRocketBase
from minime.extraction_model import Relation, ExtractionModel


class TestRocketDisabledRelations(TestRocketBase):
    REL_NOTNULL = [{'defaults': Relation.DEFAULT_OUTGOING_NOTNULL}]
    REL_EVERYTHING = [{'defaults': Relation.DEFAULT_EVERYTHING}]

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
                    test1_nn_id INTEGER NOT NULL REFERENCES test1,
                    test1_id INTEGER REFERENCES test1
                );
            ''',
        ]:
            self.dbconn.execute(sql)
        return SqliteSchema.create_from_conn(self.dbconn.connection)

    @pytest.fixture()
    def data1(self, schema1):
        table1 = schema1.tables[0]
        table2 = schema1.tables[1]
        rows = [
            (table1, (1,)),
            (table1, (2,)),
            (table2, (1, 1, None)),
            (table2, (2, 1, 2)),
        ]
        self.dbconn.insert_rows(rows)
        return rows

    @pytest.mark.parametrize('table', ['test1', 'test2'])
    def test_everything(self, schema1, data1, table):
        # All relations, we should get all data in both cases
        self.check_one_subject(schema1, [{'table': table}], data1,
                               global_relations=self.REL_EVERYTHING)

    def expected_data_no_nullable_rel(self, schema1, data1):
        table2 = schema1.tables[1]
        return (
            data1[0:1] +                # Both rows in table 1
            [(table2, (1, 1, None))] +  # First row in table 2
            [(table2, (2, 1, None))]    # Second row in table 2, but with
                                        # the nullable FK value nulled
        )

    def test_not_null_table1(self, schema1, data1):
        # This should not extract any rows from table test2
        table = {'table': 'test1'}
        self.check_one_subject(schema1, [table], data1[0:2],
                               global_relations=self.REL_NOTNULL)

    def test_not_null_table2(self, schema1, data1):
        # Test that a nullable foreign key with a value is set to null
        # if the outgoing relationship isn't present
        table = {'table': 'test2'}
        expected_data = self.expected_data_no_nullable_rel(schema1, data1)
        self.check_one_subject(schema1, [table], expected_data,
                               global_relations=self.REL_NOTNULL)

    def test_not_null_global_with_subject_nullable_rel(self, schema1, data1):
        # Test case of global default being only outgoing not null,
        # but with a subject default adding the relationship.
        # All data should be returned.
        table = {'table': 'test2'}
        relations = [{'table': 'test2', 'column': 'test1_id',
                      'type': Relation.TYPE_OUTGOING}]
        self.check_one_subject(schema1, [table], data1,
                               global_relations=self.REL_NOTNULL,
                               relations=relations)

    def test_disabled_outgoing(self, schema1, data1):
        # Test disabling the nullable relationship from test2 -> test1
        relations = [{'table': 'test2', 'column': 'test1_id',
                      'type': Relation.TYPE_OUTGOING, 'disabled': True}]
        expected_data = self.expected_data_no_nullable_rel(schema1, data1)
        self.check_one_subject(schema1, [{'table': 'test2'}], expected_data,
                               global_relations=self.REL_EVERYTHING,
                               relations=relations)

    def test_disabled_incoming(self, schema1, data1):
        rel = {'table': 'test2', 'column': 'test1_id', 'disabled': True}
        rel_nn = {'table': 'test2', 'column': 'test1_nn_id', 'disabled': True}

        # Just the nullable relation disabled: the not-null key will fetch
        # all the rows in test2
        relations = [rel]
        self.check_one_subject(schema1, [{'table': 'test1'}],
                               data1,
                               global_relations=self.REL_EVERYTHING,
                               relations=relations)

        # Just the not-null relation disabled: the row with the
        # nullable key in test2 is present
        relations = [rel_nn]
        self.check_one_subject(schema1, [{'table': 'test1'}],
                               data1[0:2] + data1[3:4],
                               global_relations=self.REL_EVERYTHING,
                               relations=relations)

        # Both disabled: no rows in test2 should be present
        relations = [rel, rel_nn]
        self.check_one_subject(schema1, [{'table': 'test1'}], data1[0:2],
                               global_relations=self.REL_EVERYTHING,
                               relations=relations)

    def test_illegal_disabled_not_null_outgoing(self, schema1, data1):
        relation = {'table': 'test2', 'column': 'test1_nn_id',
                    'type': Relation.TYPE_OUTGOING, 'disabled': True}
        data = [{'relations': [relation]}]
        with pytest.raises(Exception) as e:
            ExtractionModel.load(schema1, data)
        assert 'Cannot disable outgoing not null foreign keys' in str(e)

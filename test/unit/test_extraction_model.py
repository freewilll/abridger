from collections import defaultdict
import pytest
from jsonschema.exceptions import ValidationError
from minime.extraction_model import ExtractionModel, Relation, merge_relations


class TestExtractionModelBase(object):
    @pytest.fixture(autouse=True)
    def self_schema1_sl(self, schema1_sl):
        self.schema1_sl = schema1_sl
        self.relations = schema1_sl.relations()

    @pytest.fixture
    def relation0(self):
        relation = dict(self.relations[0])
        data = [{'relations': [relation]}]
        model = ExtractionModel.load(self.schema1_sl, data)
        return model.relations[0]


class TestExtractionModel(TestExtractionModelBase):
    def test_unknown_directive(self):
        with pytest.raises(ValidationError):
            data = [{'foo': []}]
            ExtractionModel.load(self.schema1_sl, data)

    def test_relation_errors(self):
        with pytest.raises(ValidationError):
            data = 'foo'
            ExtractionModel.load(self.schema1_sl, data)

        with pytest.raises(ValidationError):
            data = ['foo']
            ExtractionModel.load(self.schema1_sl, data)

        with pytest.raises(ValidationError):
            data = [{'foo': [], 'bar': []}]
            ExtractionModel.load(self.schema1_sl, data)

        with pytest.raises(ValidationError):
            data = [{'foo': 'bar'}]
            ExtractionModel.load(self.schema1_sl, data)

    def test_schema1_sl_relations(self):
        data = [{'relations': self.relations}]
        model = ExtractionModel.load(self.schema1_sl, data)
        assert model.relations[0].table.name == self.relations[0]['table']
        assert model.relations[0].column.name == self.relations[0]['column']
        assert model.relations[0].name == self.relations[0]['name']
        assert repr(model.relations[0]) is not None

    def test_non_existent_table_data(self):
        # Check unknown table
        data = [{'subject': [{'tables': [{'table': 'foo'}]}]}]
        with pytest.raises(Exception) as e:
            ExtractionModel.load(self.schema1_sl, data)
        assert 'Unknown table' in str(e)

        # Check unknown column
        table_name = self.schema1_sl.tables[0].name
        table = {'table': table_name, 'column': 'unknown', 'values': [1]}
        subject = [{'tables': [table]}]
        data = [{'subject': subject}]
        with pytest.raises(Exception) as e:
            ExtractionModel.load(self.schema1_sl, data)
        assert 'Unknown column' in str(e)

    def test_non_existent_relation_table_data(self):
        # Check unknown table
        relation = dict(self.relations[0])
        relation['table'] = 'foo'
        data = [{'relations': [relation]}]
        with pytest.raises(Exception) as e:
            ExtractionModel.load(self.schema1_sl, data)
        assert 'Unknown table' in str(e)

        # Check unknown column
        relation = dict(self.relations[0])
        relation['column'] = 'unknown'
        data = [{'relations': [relation]}]
        with pytest.raises(Exception) as e:
            ExtractionModel.load(self.schema1_sl, data)
        assert 'Unknown column' in str(e)

    def test_relation_keys(self):
        relation = dict(self.relations[0])

        # A missing name is ok
        relation = dict(self.relations[0])
        del relation['name']
        data = [{'relations': [relation]}]
        ExtractionModel.load(self.schema1_sl, data)

        # A missing column is ok
        relation = dict(self.relations[0])
        del relation['column']
        data = [{'relations': [relation]}]
        ExtractionModel.load(self.schema1_sl, data)

        # A null name is ok
        relation = dict(self.relations[0])
        relation['name'] = None
        data = [{'relations': [relation]}]
        ExtractionModel.load(self.schema1_sl, data)

        # Unknown key
        relation = dict(self.relations[0])
        relation['foo'] = 'bar'
        data = [{'relations': [relation]}]
        with pytest.raises(ValidationError):
            ExtractionModel.load(self.schema1_sl, data)

        def check_bool(key, default_value, relation_key=None):
            if relation_key is None:
                relation_key = key

            # Test bool values
            for value in (True, False):
                relation = dict(self.relations[0])
                relation[key] = value
                data = [{'relations': [relation]}]
                model = ExtractionModel.load(self.schema1_sl, data)
                assert getattr(model.relations[0], relation_key) is value

            # Test bad value exception
            relation[key] = 'foo'
            with pytest.raises(ValidationError):
                ExtractionModel.load(self.schema1_sl, data)

            # Check default value
            relation = dict(self.relations[0])
            data = [{'relations': [relation]}]
            model = ExtractionModel.load(self.schema1_sl, data)
            assert getattr(model.relations[0], relation_key) is default_value

        check_bool('disabled', False)
        check_bool('sticky', False, relation_key='propagate_sticky')

    def test_relation_defaults_keys(self):
        # Test both None
        relation = dict(self.relations[0])
        table = relation.pop('table')
        data = [{'relations': [relation]}]
        with pytest.raises(Exception) as e:
            ExtractionModel.load(self.schema1_sl, data)
        assert 'Either defaults or table must be set' in str(e)

        # Test both set
        relation['table'] = table
        relation['defaults'] = Relation.DEFAULT_EVERYTHING
        with pytest.raises(Exception) as e:
            ExtractionModel.load(self.schema1_sl, data)
        assert 'Either defaults or table must be set' in str(e)

    @pytest.mark.parametrize(
        'relation_defaults, expect_nullable, expect_incoming', [
            ([], True, False),  # The default is DEFAULT_OUTGOING_NULLABLE
            ([Relation.DEFAULT_OUTGOING_NULLABLE], True, False),
            ([Relation.DEFAULT_OUTGOING_NOTNULL], False, False),
            ([Relation.DEFAULT_INCOMING], False, True),
            ([Relation.DEFAULT_EVERYTHING], True, True),
            ([Relation.DEFAULT_EVERYTHING,
              Relation.DEFAULT_EVERYTHING], True, True),
            ([Relation.DEFAULT_OUTGOING_NULLABLE,
              Relation.DEFAULT_INCOMING], True, True),
        ])
    def test_relation_defaults(self, relation_defaults, expect_nullable,
                               expect_incoming):
        relations = []
        for relation_default in relation_defaults:
            relations.append({'defaults': relation_default})
        data = [{'relations': relations}]
        model = ExtractionModel.load(self.schema1_sl, data)

        got_outgoing_not_null = False
        got_outgoing_nullable = False
        got_incoming = False
        got_outgoing = False

        relation_counts = defaultdict(int)
        for relation in model.relations:
            relation_counts[relation] += 1
            if relation_counts[relation] > 1:
                pytest.fail('Got duplicate relation "%s"' % relation)

            if relation.type == Relation.TYPE_OUTGOING:
                got_outgoing = True
                if relation.column.notnull:
                    got_outgoing_not_null = True
                else:
                    got_outgoing_nullable = True
            else:
                got_incoming = True

        assert got_outgoing is True
        assert got_outgoing_not_null is True
        assert expect_nullable == got_outgoing_nullable
        assert expect_incoming == got_incoming

    def test_relation_type(self):
        # Check type
        relation = dict(self.relations[0])
        relation['type'] = 'bar'
        data = [{'relations': [relation]}]
        with pytest.raises(ValidationError):
            ExtractionModel.load(self.schema1_sl, data)

        type_tests = [(Relation.TYPE_INCOMING, Relation.TYPE_INCOMING),
                      (Relation.TYPE_OUTGOING, Relation.TYPE_OUTGOING),
                      (None, Relation.TYPE_INCOMING)]
        for (value, expected_value) in type_tests:
            relation = dict(self.relations[0])
            data = [{'relations': [relation]}]
            if value is not None:
                relation['type'] = value
            model = ExtractionModel.load(self.schema1_sl, data)
            assert model.relations[0].type == expected_value

    def test_subject_must_have_at_least_one_table(self):
        data = [{'subject': [{'relations': self.relations}]}]
        with pytest.raises(Exception) as e:
            ExtractionModel.load(self.schema1_sl, data)
        assert 'A subject must have at least one table' in str(e)

    def model_of_table0(self):
        table = {'table': self.schema1_sl.tables[0].name}

        subject = [
            {'relations': self.relations},
            {'tables': [table]}
        ]

        data = [{'subject': subject}]
        return ExtractionModel.load(self.schema1_sl, data)

    def test_subject_relation(self):
        model = self.model_of_table0()

        # Test relation
        assert len(model.subjects) == 1
        assert model.subjects[0].relations[0].table.name == \
            self.relations[0]['table']

    def test_relation_without_a_column(self):
        relation = dict(self.relations[0])
        del relation['column']
        table = {'table': self.schema1_sl.tables[0].name}
        subject = [
            {'relations': [relation]},
            {'tables': [table]}
        ]
        data = [{'subject': subject}]
        model = ExtractionModel.load(self.schema1_sl, data)
        assert len(model.subjects) == 1
        assert model.subjects[0].relations[0].column is None

    def test_subject_repr(self):
        model = self.model_of_table0()
        assert repr(model.subjects[0]) is not None

    def test_subject_table_with_just_a_table_key(self):
        table = {'table': self.schema1_sl.tables[0].name}
        subject = [{'relations': self.relations}, {'tables': [table]}]
        data = [{'subject': subject}]

        model = ExtractionModel.load(self.schema1_sl, data)
        assert len(model.subjects[0].tables) == 1
        assert model.subjects[0].tables[0].table.name == table['table']

    def test_subject_table_column_and_values_keys_both_set(self):
        table = {'table': self.schema1_sl.tables[0].name}
        column = table['column'] = self.schema1_sl.tables[0].cols[0].name
        for key in ['column', 'values']:
            table = {'table': self.schema1_sl.tables[0].name}
            if key == 'column':
                table['column'] = column
            if key == 'values':
                table['values'] = 1
            subject = [{'relations': self.relations}, {'tables': [table]}]
            data = [{'subject': subject}]
            with pytest.raises(Exception) as e:
                ExtractionModel.load(self.schema1_sl, data)
            if key == 'column':
                assert 'A table with a column must have values' in str(e)
            if key == 'values':
                assert 'A table with values must have a column' in str(e)

    def test_subject_table_values_types(self):
        table = {'table': self.schema1_sl.tables[0].name}
        column = table['column'] = self.schema1_sl.tables[0].cols[0].name
        for values in [1, '1', [1, 2], ['1', '2']]:
            table = {'table': self.schema1_sl.tables[0].name, 'column': column,
                     'values': values}
            subject = [{'relations': self.relations}, {'tables': [table]}]
            data = [{'subject': subject}]
            model = ExtractionModel.load(self.schema1_sl, data)
            assert model.subjects[0].tables[0].values == values

    def test_not_null_columns_must_be_toplevel(self):
        not_null_columns = [{'not-null-columns': []}]
        data = [{'relations': [not_null_columns]}]
        with pytest.raises(Exception) as e:
            ExtractionModel.load(self.schema1_sl, data)
        assert 'is not valid under any of the given schemas' in str(e)

        data = [{'tables': [not_null_columns]}]
        with pytest.raises(Exception) as e:
            ExtractionModel.load(self.schema1_sl, data)
        assert 'is not valid under any of the given schemas' in str(e)

    def test_not_null_columns(self):
        relation = dict(self.relations[0])
        table = relation['table']
        column = relation['column']

        data = [{'not-null-columns': []}]
        ExtractionModel.load(self.schema1_sl, data)

        data = [{'not-null-columns': []}]
        ExtractionModel.load(self.schema1_sl, data)

        # It must have table and column keys
        with pytest.raises(Exception) as e:
            data = [{'not-null-columns': [{}]}]
            ExtractionModel.load(self.schema1_sl, data)
        assert 'is not valid under any of the given schemas' in str(e)

        with pytest.raises(Exception) as e:
            data = [{'not-null-columns': [{'table': table}]}]
            ExtractionModel.load(self.schema1_sl, data)
        assert 'is not valid under any of the given schemas' in str(e)

        with pytest.raises(Exception) as e:
            data = [{'not-null-columns': [{'column': column}]}]
            ExtractionModel.load(self.schema1_sl, data)
        assert 'is not valid under any of the given schemas' in str(e)

        data = [
            {'not-null-columns': [{'table': table, 'column': column}]}]
        model = ExtractionModel.load(self.schema1_sl, data)
        assert len(model.not_null_cols) == 1
        afc = model.not_null_cols[0]
        assert afc.table.name == table
        assert afc.column.name == column
        assert afc.foreign_key.src_cols[0].table.name == table
        assert column in [c.name for c in afc.foreign_key.src_cols]

    def test_non_existent_not_null_columns_data(self):
        relation = dict(self.relations[0])
        table = relation['table']
        column = relation['column']

        data = [
            {'not-null-columns': [{'table': 'foo', 'column': column}]}]
        with pytest.raises(Exception) as e:
            ExtractionModel.load(self.schema1_sl, data)
        assert 'Unknown table' in str(e)

        data = [
            {'not-null-columns': [{'table': table, 'column': 'unknown'}]}]
        with pytest.raises(Exception) as e:
            ExtractionModel.load(self.schema1_sl, data)
        assert 'Unknown column' in str(e)

    def test_not_null_columns_must_be_a_foreign_key(self):
        # The assumption there is that the first table doesn't have any
        # foreign keys.
        non_fk_column = self.schema1_sl.tables[0].cols[0]
        fk_column = None

        # Let's check that assumption, just to be paranoid that the starting
        # conditions of this test are valid.
        for table in self.schema1_sl.tables:
            for fk in table.foreign_keys:
                for fk_column in fk.src_cols:
                    assert non_fk_column != fk_column
                    if fk_column is None:
                        fk_column = fk_column

        assert fk_column is not None

        # Test for a valid foreign key
        data = [
            {'not-null-columns': [{'table': fk_column.table.name,
                                   'column': fk_column.name}]}]
        model = ExtractionModel.load(self.schema1_sl, data)
        assert len(model.not_null_cols) == 1

        # Test for a non foreign key
        data = [
            {'not-null-columns': [{'table': non_fk_column.table.name,
                                   'column': non_fk_column.name}]}]
        with pytest.raises(Exception) as e:
            ExtractionModel.load(self.schema1_sl, data)
        assert 'not-null-columns can only be used on foreign keys' in str(e)

    def test_clone_relation(self, relation0):
        r = relation0.clone()
        assert r.table == relation0.table
        assert r.column == relation0.column
        assert r.disabled == relation0.disabled
        assert r.type == relation0.type
        assert r.propagate_sticky == relation0.propagate_sticky
        assert r.only_if_sticky == relation0.only_if_sticky

    def test_relation_equality(self, relation0):
        relation1 = relation0.clone()
        relation1.disabled = True
        assert relation0 == relation0
        assert relation1 == relation1
        assert relation0 != relation1

        relation1 = relation0.clone()
        relation1.propagate_sticky = True
        assert relation0 == relation0
        assert relation1 == relation1
        assert relation0 != relation1

        relation1 = relation0.clone()
        relation1.only_if_sticky = True
        assert relation0 == relation0
        assert relation1 == relation1
        assert relation0 != relation1


class TestExtractionModelMergeRelations(TestExtractionModelBase):
    @pytest.fixture(autouse=True)
    def setup_fixtures(self, relation0):
        self.rel1 = relation0.clone()
        self.rel2 = relation0.clone()
        self.rel3 = relation0.clone()

    def test_merge_relations_disabled(self, relation0):
        self.rel2.disabled = True

        # Check merging identical relations has no effect
        assert len(merge_relations([self.rel1, self.rel1])) == 1

        # Check that the disabled relation by itself has no effect
        assert len(merge_relations([self.rel2])) == 0
        assert len(merge_relations([self.rel2, self.rel2])) == 0

        # Check the disabled relation removes the first
        assert len(merge_relations([self.rel1, self.rel2])) == 0
        assert len(merge_relations([self.rel1, self.rel2, self.rel2])) == 0

    @pytest.mark.parametrize('attr', ['propagate_sticky', 'only_if_sticky'])
    def test_merge_relations_sticky(self, attr):
        setattr(self.rel1, attr, True)
        setattr(self.rel2, attr, False)

        # Check merging identical relations has the same effect
        merge = merge_relations([self.rel1, self.rel1])
        assert len(merge) == 1
        assert getattr(merge[0], attr) is True

        merge = merge_relations([self.rel1, self.rel2])
        assert len(merge) == 1
        assert getattr(merge[0], attr) is True

        merge = merge_relations([self.rel2, self.rel2])
        assert len(merge) == 1
        assert getattr(merge[0], attr) is False

    def test_merge_relations_disabled_and_sticky1(self):
        self.rel1.propagate_sticky = True
        self.rel2.propagate_sticky = False
        self.rel3.propagate_sticky = False
        self.rel1.disabled = False
        self.rel2.disabled = False
        self.rel3.disabled = True
        assert len(merge_relations([self.rel2, self.rel2, self.rel3])) == 0

    def test_merge_relations_disabled_and_sticky2(self):
        self.rel1.only_if_sticky = True
        self.rel2.only_if_sticky = False
        self.rel3.only_if_sticky = False
        self.rel1.disabled = False
        self.rel2.disabled = False
        self.rel3.disabled = True
        assert len(merge_relations([self.rel2, self.rel2, self.rel3])) == 0

    def test_relation_repr(self):
        self.rel1.only_if_sticky = True
        assert repr(self.rel1) is not None

        self.rel1.only_if_sticky = False
        self.rel1.propagate_sticky = True
        assert repr(self.rel1) is not None

    def test_illegal_sticky_disabled_combination(self):
        for sticky in [True, False]:
            relation = dict(self.relations[0])
            relation['sticky'] = sticky
            relation['disabled'] = True
            data = [{'relations': [relation]}]
            with pytest.raises(Exception) as e:
                ExtractionModel.load(self.schema1_sl, data)
            assert 'The sticky flag is meaningless' in str(e)

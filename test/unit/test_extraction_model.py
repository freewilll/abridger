from collections import defaultdict
from jsonschema.exceptions import ValidationError
import pytest

from abridger.exc import (UnknownTableError, UnknownColumnError,
                          InvalidConfigError, RelationIntegrityError)
from abridger.extraction_model import (ExtractionModel, Relation,
                                       merge_relations)


class TestExtractionModelBase(object):
    @pytest.fixture(autouse=True)
    def self_schema_sl(self, schema_sl):
        self.schema_sl = schema_sl
        self.relations = schema_sl.relations()

    @pytest.fixture
    def relation0(self):
        relation = dict(self.relations[0])
        data = [{'relations': [relation]}]
        model = ExtractionModel.load(self.schema_sl, data)
        return model.relations[0]


class TestExtractionModel(TestExtractionModelBase):
    def test_toplevel(self):
        with pytest.raises(ValidationError) as e:
            data = 'foo'
            ExtractionModel.load(self.schema_sl, data)
        assert 'is not of type' in str(e)

        with pytest.raises(ValidationError) as e:
            data = ['foo']
            ExtractionModel.load(self.schema_sl, data)
        assert 'is not of type' in str(e)

        with pytest.raises(ValidationError) as e:
            data = [{'foo': [], 'bar': []}]
            ExtractionModel.load(self.schema_sl, data)
        assert 'Additional properties are not allowed' in str(e)

        with pytest.raises(ValidationError) as e:
            data = [{'foo': 'bar'}]
            ExtractionModel.load(self.schema_sl, data)
        assert 'Additional properties are not allowed' in str(e)

    def test_toplevel_key_value(self):
        # Check bad None value for top level keys
        for key in ('subject', 'relations', 'not-null-columns'):
            with pytest.raises(ValidationError) as e:
                data = [{key: None}]
                ExtractionModel.load(self.schema_sl, data)
            assert 'is not of type' in str(e)

    def test_toplevel_keys(self):
        # Test good cases
        not_null_cols = {'not-null-columns': []}
        relations = {'relations': self.relations}
        table = {'table': self.schema_sl.tables[0].name}
        subject = {'subject': [
            {'relations': self.relations},
            {'tables': [table]}
        ]}

        ExtractionModel.load(self.schema_sl, [not_null_cols])
        ExtractionModel.load(self.schema_sl, [relations])
        ExtractionModel.load(self.schema_sl, [subject])

        # Test exactly one of the keys has to be set
        with pytest.raises(InvalidConfigError) as e:
            ExtractionModel.load(self.schema_sl, [{}])
        assert 'Expected one key, got' in str(e)

        with pytest.raises(InvalidConfigError) as e:
            data = dict(not_null_cols)
            data.update(relations)
            ExtractionModel.load(self.schema_sl, [data])
        assert 'Expected one key, got' in str(e)

    def test_subject_keys(self):
        table = {'table': self.schema_sl.tables[0].name}
        subject = [
            {'relations': self.relations},
            {'tables': [table]}
        ]
        ExtractionModel.load(self.schema_sl, [{'subject': subject}])

        # Test zero keys
        with pytest.raises(InvalidConfigError) as e:
            subject.append({})
            ExtractionModel.load(self.schema_sl, [{'subject': subject}])
        assert 'Expected one key, got' in str(e)

        # Test two keys
        with pytest.raises(InvalidConfigError) as e:
            subject[2] = dict(subject[0])
            subject[2].update(subject[1])
            ExtractionModel.load(self.schema_sl, [{'subject': subject}])
        assert 'Expected one key, got' in str(e)

    def test_schema_sl_relations(self):
        data = [{'relations': self.relations}]
        model = ExtractionModel.load(self.schema_sl, data)
        assert model.relations[0].table.name == self.relations[0]['table']
        fk_col = model.relations[0].foreign_key.src_cols[0]
        assert fk_col.name == self.relations[0]['column']
        assert model.relations[0].name == self.relations[0]['name']
        assert repr(model.relations[0]) is not None

    def test_non_existent_table_data(self):
        # Check unknown table
        data = [{'subject': [{'tables': [{'table': 'foo'}]}]}]
        with pytest.raises(UnknownTableError):
            ExtractionModel.load(self.schema_sl, data)

        # Check unknown column
        table_name = self.schema_sl.tables[0].name
        table = {'table': table_name, 'column': 'unknown', 'values': [1]}
        subject = [{'tables': [table]}]
        data = [{'subject': subject}]
        with pytest.raises(UnknownColumnError):
            ExtractionModel.load(self.schema_sl, data)

    def test_non_existent_relation_table_data(self):
        # Check unknown table
        relation = dict(self.relations[0])
        relation['table'] = 'foo'
        data = [{'relations': [relation]}]
        with pytest.raises(UnknownTableError):
            ExtractionModel.load(self.schema_sl, data)

        # Check unknown column
        relation = dict(self.relations[0])
        relation['column'] = 'unknown'
        data = [{'relations': [relation]}]
        with pytest.raises(UnknownColumnError):
            ExtractionModel.load(self.schema_sl, data)

        # Check known column, but it's not a foreign key
        known_relations = set()
        for rel in self.relations:
            known_relations.add((rel['table'], rel['column']))
        found_test = None
        for table in self.schema_sl.tables:
            for col in table.cols:
                if (table.name, col.name) not in known_relations:
                    found_test = (table.name, col.name)
                    break
        if found_test is None:
            raise Exception('Unable to find a test table/column')
        (table, col) = found_test

        relation = {'table': table, 'column': col}
        data = [{'relations': [relation]}]
        with pytest.raises(RelationIntegrityError):
            ExtractionModel.load(self.schema_sl, data)

    def test_relation_keys(self):
        relation = dict(self.relations[0])

        # A missing name is ok
        relation = dict(self.relations[0])
        del relation['name']
        data = [{'relations': [relation]}]
        ExtractionModel.load(self.schema_sl, data)

        # A missing column is not ok
        relation = dict(self.relations[0])
        del relation['column']
        data = [{'relations': [relation]}]
        with pytest.raises(RelationIntegrityError) as e:
            ExtractionModel.load(self.schema_sl, data)
        assert 'Non default relations must have a column on table' in str(e)

        # A null name is ok
        relation = dict(self.relations[0])
        relation['name'] = None
        data = [{'relations': [relation]}]
        ExtractionModel.load(self.schema_sl, data)

        # Unknown key
        relation = dict(self.relations[0])
        relation['foo'] = 'bar'
        data = [{'relations': [relation]}]
        with pytest.raises(ValidationError):
            ExtractionModel.load(self.schema_sl, data)

        def check_bool(key, default_value, relation_key=None):
            if relation_key is None:
                relation_key = key

            # Test bool values
            for value in (True, False):
                relation = dict(self.relations[0])
                relation[key] = value
                data = [{'relations': [relation]}]
                model = ExtractionModel.load(self.schema_sl, data)
                assert getattr(model.relations[0], relation_key) is value

            # Test bad value exception
            relation[key] = 'foo'
            with pytest.raises(ValidationError):
                ExtractionModel.load(self.schema_sl, data)

            # Check default value
            relation = dict(self.relations[0])
            data = [{'relations': [relation]}]
            model = ExtractionModel.load(self.schema_sl, data)
            assert getattr(model.relations[0], relation_key) is default_value

        check_bool('disabled', False)
        check_bool('sticky', False, relation_key='propagate_sticky')

    def test_relation_defaults_keys(self):
        # Test both None
        relation = dict(self.relations[0])
        table = relation.pop('table')
        data = [{'relations': [relation]}]
        with pytest.raises(InvalidConfigError) as e:
            ExtractionModel.load(self.schema_sl, data)
        assert 'Either defaults or table must be set' in str(e)

        # Test both set
        relation['table'] = table
        relation['defaults'] = Relation.DEFAULT_EVERYTHING
        with pytest.raises(InvalidConfigError) as e:
            ExtractionModel.load(self.schema_sl, data)
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
        model = ExtractionModel.load(self.schema_sl, data)

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
                if relation.foreign_key.notnull:
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
            ExtractionModel.load(self.schema_sl, data)

        type_tests = [(Relation.TYPE_INCOMING, Relation.TYPE_INCOMING),
                      (Relation.TYPE_OUTGOING, Relation.TYPE_OUTGOING),
                      (None, Relation.TYPE_INCOMING)]
        for (value, expected_value) in type_tests:
            relation = dict(self.relations[0])
            data = [{'relations': [relation]}]
            if value is not None:
                relation['type'] = value
            model = ExtractionModel.load(self.schema_sl, data)
            assert model.relations[0].type == expected_value

    def test_subject_must_have_at_least_one_table(self):
        data = [{'subject': [{'relations': self.relations}]}]
        with pytest.raises(InvalidConfigError) as e:
            ExtractionModel.load(self.schema_sl, data)
        assert 'A subject must have at least one table' in str(e)

    def model_of_table0(self):
        table = {'table': self.schema_sl.tables[0].name}

        subject = [
            {'relations': self.relations},
            {'tables': [table]}
        ]

        data = [{'subject': subject}]
        return ExtractionModel.load(self.schema_sl, data)

    def test_subject_relation(self):
        model = self.model_of_table0()

        # Test relation
        assert len(model.subjects) == 1
        assert model.subjects[0].relations[0].table.name == \
            self.relations[0]['table']

    def test_subject_repr(self):
        model = self.model_of_table0()
        assert repr(model.subjects[0]) is not None

    def test_subject_table_with_just_a_table_key(self):
        table = {'table': self.schema_sl.tables[0].name}
        subject = [{'relations': self.relations}, {'tables': [table]}]
        data = [{'subject': subject}]

        model = ExtractionModel.load(self.schema_sl, data)
        assert len(model.subjects[0].tables) == 1
        assert model.subjects[0].tables[0].table.name == table['table']

    def test_subject_table_column_and_values_keys_both_set(self):
        table = {'table': self.schema_sl.tables[0].name}
        col = table['column'] = self.schema_sl.tables[0].cols[0].name
        for key in ['column', 'values']:
            table = {'table': self.schema_sl.tables[0].name}
            if key == 'column':
                table['column'] = col
            if key == 'values':
                table['values'] = 1
            subject = [{'relations': self.relations}, {'tables': [table]}]
            data = [{'subject': subject}]
            with pytest.raises(InvalidConfigError) as e:
                ExtractionModel.load(self.schema_sl, data)
            if key == 'column':
                assert 'A table with a column must have values' in str(e)
            if key == 'values':
                assert 'A table with values must have a column' in str(e)

    def test_subject_table_values_types(self):
        table = {'table': self.schema_sl.tables[0].name}
        col = table['column'] = self.schema_sl.tables[0].cols[0].name

        def test(values):
            table = {'table': self.schema_sl.tables[0].name, 'column': col,
                     'values': values}
            subject = [{'relations': self.relations}, {'tables': [table]}]
            data = [{'subject': subject}]
            model = ExtractionModel.load(self.schema_sl, data)
            assert model.subjects[0].tables[0].values == values

        # Good values
        for values in [1, '1', [1, 2], ['1', '2'], [1, '2']]:
            test(values)

        # Bad values
        for values in [None, True, {}, [None], [True], [{}]]:
            with pytest.raises(ValidationError) as e:
                test(values)
            assert 'is not valid under any of the given schemas' in str(e)

    def test_not_null_cols_must_be_toplevel(self):
        not_null_cols = [{'not-null-columns': []}]
        data = [{'relations': not_null_cols}]
        with pytest.raises(ValidationError) as e:
            ExtractionModel.load(self.schema_sl, data)
        assert 'Additional properties are not allowed' in str(e)

        data = [{'tables': not_null_cols}]
        with pytest.raises(ValidationError) as e:
            ExtractionModel.load(self.schema_sl, data)
        assert 'Additional properties are not allowed' in str(e)

    def test_not_null_cols(self):
        relation = dict(self.relations[0])
        table = relation['table']
        col = relation['column']

        data = [{'not-null-columns': []}]
        ExtractionModel.load(self.schema_sl, data)

        data = [{'not-null-columns': []}]
        ExtractionModel.load(self.schema_sl, data)

        # It must have table and column keys
        with pytest.raises(ValidationError) as e:
            data = [{'not-null-columns': [{}]}]
            ExtractionModel.load(self.schema_sl, data)
        assert "is a required property" in str(e)

        with pytest.raises(ValidationError) as e:
            data = [{'not-null-columns': [{'table': table}]}]
            ExtractionModel.load(self.schema_sl, data)
        assert "'column' is a required property" in str(e)

        with pytest.raises(ValidationError) as e:
            data = [{'not-null-columns': [{'column': col}]}]
            ExtractionModel.load(self.schema_sl, data)
        assert "'table' is a required property" in str(e)

        data = [
            {'not-null-columns': [{'table': table, 'column': col}]}]
        model = ExtractionModel.load(self.schema_sl, data)
        assert len(model.not_null_cols) == 1
        afc = model.not_null_cols[0]
        assert afc.table.name == table
        assert afc.col.name == col
        assert afc.foreign_key.src_cols[0].table.name == table
        assert col in [c.name for c in afc.foreign_key.src_cols]

    def test_non_existent_not_null_cols_data(self):
        relation = dict(self.relations[0])
        table = relation['table']
        col = relation['column']

        data = [
            {'not-null-columns': [{'table': 'foo', 'column': col}]}]
        with pytest.raises(UnknownTableError):
            ExtractionModel.load(self.schema_sl, data)

        data = [
            {'not-null-columns': [{'table': table, 'column': 'unknown'}]}]
        with pytest.raises(UnknownColumnError):
            ExtractionModel.load(self.schema_sl, data)

    def test_not_null_cols_must_be_a_foreign_key(self):
        # The assumption there is that the first table doesn't have any
        # foreign keys.
        non_fk_col = self.schema_sl.tables[0].cols[0]
        fk_col = None

        # Let's check that assumption, just to be paranoid that the starting
        # conditions of this test are valid.
        for table in self.schema_sl.tables:
            for fk in table.foreign_keys:
                for fk_col in fk.src_cols:
                    assert non_fk_col != fk_col
                    if fk_col is None:
                        fk_col = fk_col

        assert fk_col is not None

        # Test for a valid foreign key
        data = [
            {'not-null-columns': [{'table': fk_col.table.name,
                                   'column': fk_col.name}]}]
        model = ExtractionModel.load(self.schema_sl, data)
        assert len(model.not_null_cols) == 1

        # Test for a non foreign key
        data = [
            {'not-null-columns': [{'table': non_fk_col.table.name,
                                   'column': non_fk_col.name}]}]
        with pytest.raises(RelationIntegrityError) as e:
            ExtractionModel.load(self.schema_sl, data)
        assert 'not-null-columns can only be used on foreign keys' in str(e)

    def test_clone_relation(self, relation0):
        r = relation0.clone()
        assert r.table == relation0.table
        assert r.foreign_key == relation0.foreign_key
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
            with pytest.raises(InvalidConfigError) as e:
                ExtractionModel.load(self.schema_sl, data)
            assert 'The sticky flag is meaningless' in str(e)

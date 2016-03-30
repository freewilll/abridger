import pytest
from jsonschema.exceptions import ValidationError
from minime.extraction_model import ExtractionModel


class TestExtractionModel(object):
    @pytest.fixture(autouse=True)
    def self_schema1(self, schema1):
        self.schema1 = schema1
        self.relations = schema1.relations()

    def test_unknown_directive(self):
        with pytest.raises(ValidationError):
            data = [{'foo': []}]
            ExtractionModel.load(self.schema1, data)

    def test_relation_errors(self):
        with pytest.raises(ValidationError):
            data = 'foo'
            ExtractionModel.load(self.schema1, data)

        with pytest.raises(ValidationError):
            data = ['foo']
            ExtractionModel.load(self.schema1, data)

        with pytest.raises(ValidationError):
            data = [{'foo': [], 'bar': []}]
            ExtractionModel.load(self.schema1, data)

        with pytest.raises(ValidationError):
            data = [{'foo': 'bar'}]
            ExtractionModel.load(self.schema1, data)

    def test_schema1_relations(self):
        data = [{'relations': self.relations}]
        model = ExtractionModel.load(self.schema1, data)
        assert model.relations[0].table == self.relations[0]['table']
        assert model.relations[0].column == self.relations[0]['column']
        assert model.relations[0].name == self.relations[0]['name']

    def test_relation_keys(self):
        relation = dict(self.relations[0])

        # A missing name is ok
        relation = dict(self.relations[0])
        del relation['name']
        data = [{'relations': [relation]}]
        ExtractionModel.load(self.schema1, data)

        # A missing table or column key is not OK
        for key in ['table', 'column']:
            relation = dict(self.relations[0])
            del relation[key]
            data = [{'relations': [relation]}]
            with pytest.raises(ValidationError):
                ExtractionModel.load(self.schema1, data)

        # Unknown key
        relation = dict(self.relations[0])
        relation['foo'] = 'bar'
        data = [{'relations': [relation]}]
        with pytest.raises(ValidationError):
            ExtractionModel.load(self.schema1, data)

        # A relation can be disabled
        relation = dict(self.relations[0])
        relation['disabled'] = True
        data = [{'relations': [relation]}]
        model = ExtractionModel.load(self.schema1, data)
        assert model.relations[0].disabled is True
        relation['disabled'] = False
        model = ExtractionModel.load(self.schema1, data)
        assert model.relations[0].disabled is False
        relation['disabled'] = 'foo'
        with pytest.raises(ValidationError):
            ExtractionModel.load(self.schema1, data)

    def test_subject_must_have_at_least_one_table(self):
        data = [{'subjects': [[{'relations': self.relations}]]}]
        with pytest.raises(Exception) as e:
            ExtractionModel.load(self.schema1, data)
        assert 'A subject must have at least one table' in str(e)

    def test_subject_relation(self):
        table = {'table': self.schema1.tables[0].name}

        subject = [
            {'relations': self.relations},
            {'tables': [table]}
        ]

        data = [{'subjects': [subject]}]
        model = ExtractionModel.load(self.schema1, data)

        # Test relation
        assert len(model.subjects) == 1
        assert model.subjects[0].relations[0].table == \
            self.relations[0]['table']

    def test_subject_table_with_just_a_table_key(self):
        table = {'table': self.schema1.tables[0].name}
        subject = [{'relations': self.relations}, {'tables': [table]}]
        data = [{'subjects': [subject]}]
        model = ExtractionModel.load(self.schema1, data)
        assert len(model.subjects[0].tables) == 1
        assert model.subjects[0].tables[0].table == table['table']

    def test_subject_table_column_and_values_keys_both_set(self):
        table = {'table': self.schema1.tables[0].name}
        column = table['column'] = self.schema1.tables[0].cols[0].name
        for key in ['column', 'values']:
            table = {'table': self.schema1.tables[0].name}
            if key == 'column':
                table['column'] = column
            if key == 'values':
                table['values'] = 1
            subject = [{'relations': self.relations}, {'tables': [table]}]
            data = [{'subjects': [subject]}]
            with pytest.raises(Exception) as e:
                ExtractionModel.load(self.schema1, data)
            if key == 'column':
                assert 'A table with a column must have values' in str(e)
            if key == 'values':
                assert 'A table with values must have a column' in str(e)

    def test_subject_table_values_types(self):
        table = {'table': self.schema1.tables[0].name}
        column = table['column'] = self.schema1.tables[0].cols[0].name
        for values in [1, '1', [1, 2], ['1', '2']]:
            table = {'table': self.schema1.tables[0].name, 'column': column,
                     'values': values}
            subject = [{'relations': self.relations}, {'tables': [table]}]
            data = [{'subjects': [subject]}]
            model = ExtractionModel.load(self.schema1, data)
            assert model.subjects[0].tables[0].values == values

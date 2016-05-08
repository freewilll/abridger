import pytest
from jsonschema.exceptions import ValidationError
from minime.extraction_model import ExtractionModel, Relation


class TestExtractionModel(object):
    @pytest.fixture(autouse=True)
    def self_schema1_sl(self, schema1_sl):
        self.schema1_sl = schema1_sl
        self.relations = schema1_sl.relations()

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

        def check_bool(key, default_value):
            # Test bool values
            for value in (True, False):
                relation = dict(self.relations[0])
                relation[key] = value
                data = [{'relations': [relation]}]
                model = ExtractionModel.load(self.schema1_sl, data)
                assert getattr(model.relations[0], key) is value

            # Test bad value exception
            relation[key] = 'foo'
            with pytest.raises(ValidationError):
                ExtractionModel.load(self.schema1_sl, data)

            # Check default value
            relation = dict(self.relations[0])
            data = [{'relations': [relation]}]
            model = ExtractionModel.load(self.schema1_sl, data)
            assert getattr(model.relations[0], key) is default_value

        check_bool('disabled', False)
        check_bool('sticky', False)

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

    def test_relation_defaults(self):
        tests = [
            ([], True, False),  # The default is DEFAULT_OUTGOING_NULLABLE
            ([Relation.DEFAULT_OUTGOING_NULLABLE], True, False),
            ([Relation.DEFAULT_OUTGOING_NOTNULL], False, False),
            ([Relation.DEFAULT_INCOMING], False, True),
            ([Relation.DEFAULT_EVERYTHING], True, True),
            ([Relation.DEFAULT_OUTGOING_NULLABLE,
              Relation.DEFAULT_INCOMING], True, True),
        ]

        for (relation_defaults, expect_nullable, expect_incoming) in tests:
            relations = []
            for relation_default in relation_defaults:
                relations.append({'defaults': relation_default})
            data = [{'relations': relations}]
            model = ExtractionModel.load(self.schema1_sl, data)

            got_outgoing_not_null = False
            got_outgoing_nullable = False
            got_incoming = False
            got_outgoing = False

            for relation in model.relations:
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

    def test_subject_relation(self):
        table = {'table': self.schema1_sl.tables[0].name}

        subject = [
            {'relations': self.relations},
            {'tables': [table]}
        ]

        data = [{'subject': subject}]
        model = ExtractionModel.load(self.schema1_sl, data)

        # Test relation
        assert len(model.subjects) == 1
        assert model.subjects[0].relations[0].table.name == \
            self.relations[0]['table']

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

    def test_always_follow_columns_must_be_toplevel(self):
        always_follow_columns = [{'always-follow-columns': []}]
        data = [{'relations': [always_follow_columns]}]
        with pytest.raises(Exception) as e:
            ExtractionModel.load(self.schema1_sl, data)
        assert 'is not valid under any of the given schemas' in str(e)

        data = [{'tables': [always_follow_columns]}]
        with pytest.raises(Exception) as e:
            ExtractionModel.load(self.schema1_sl, data)
        assert 'is not valid under any of the given schemas' in str(e)

    def test_always_follow_columns(self):
        relation = dict(self.relations[0])
        table = relation['table']
        column = relation['column']

        data = [{'always-follow-columns': []}]
        ExtractionModel.load(self.schema1_sl, data)

        data = [{'always-follow-columns': []}]
        ExtractionModel.load(self.schema1_sl, data)

        # It must have table and column keys
        with pytest.raises(Exception) as e:
            data = [{'always-follow-columns': [{}]}]
            ExtractionModel.load(self.schema1_sl, data)
        assert 'is not valid under any of the given schemas' in str(e)

        with pytest.raises(Exception) as e:
            data = [{'always-follow-columns': [{'table': table}]}]
            ExtractionModel.load(self.schema1_sl, data)
        assert 'is not valid under any of the given schemas' in str(e)

        with pytest.raises(Exception) as e:
            data = [{'always-follow-columns': [{'column': column}]}]
            ExtractionModel.load(self.schema1_sl, data)
        assert 'is not valid under any of the given schemas' in str(e)

        data = [
            {'always-follow-columns': [{'table': table, 'column': column}]}]
        model = ExtractionModel.load(self.schema1_sl, data)
        assert len(model.always_follow_cols) == 1
        afc = model.always_follow_cols[0]
        assert afc.table.name == table
        assert afc.column.name == column

    def test_non_existent_always_follow_columns_data(self):
        relation = dict(self.relations[0])
        table = relation['table']
        column = relation['column']

        data = [
            {'always-follow-columns': [{'table': 'foo', 'column': column}]}]
        with pytest.raises(Exception) as e:
            ExtractionModel.load(self.schema1_sl, data)
        assert 'Unknown table' in str(e)

        data = [
            {'always-follow-columns': [{'table': table, 'column': 'unknown'}]}]
        with pytest.raises(Exception) as e:
            ExtractionModel.load(self.schema1_sl, data)
        assert 'Unknown column' in str(e)

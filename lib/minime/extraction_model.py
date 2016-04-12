from jsonschema import Draft4Validator


class Subject(object):
    def __init__(self):
        self.relations = []
        self.tables = []


class Relation(object):
    def __init__(self, table, column, name, disabled):
        self.table = table
        self.column = column
        self.name = name
        self.disabled = disabled


class AlwaysFollowColumn(object):
    def __init__(self, table, column):
        self.table = table
        self.column = column


class Table(object):
    def __init__(self, table, column, values):
        self.table = table
        self.column = column
        self.values = values


class ExtractionModel(object):
    relation_definition = {
        'type': 'object',
        'required': ['table'],
        'properties': {
            'table': {'type': 'string'},
            'column': {'type': 'string'},
            'name': {'type': ['string', 'null']},
            'disabled': {'type': ['boolean']},
        },
        'additionalProperties': False,
    }

    table_definition = {
        'type': 'object',
        'required': ['table'],
        'properties': {
            'table': {'type': 'string'},
            'column': {'type': 'string'},
            'values': {
                'anyOf': [
                    {'type': 'string'},
                    {'type': 'number'},
                    {'type': 'array', 'items': {'type': ['string', 'number']}},
                ]
            },
        },
        'additionalProperties': False,
    }

    tables_arr = {'type': 'array', 'items': {'$ref': '#/definitions/table'}}
    rels_arr = {'type': 'array', 'items': {'$ref': '#/definitions/relation'}}

    subject_definition = {
        'type': 'array',
        'items': {
            'anyOf': [
                {'type': 'object',
                 'properties': {'tables': tables_arr},
                 'additionalProperties': False},
                {'type': 'object',
                 'properties': {'relations': rels_arr},
                 'additionalProperties': False},
            ],
        }
    }

    subjects_arr = {'type': 'array',
                    'items': {'$ref': '#/definitions/subject'}}

    always_follow_cols_definition = {
        'type': 'object',
        'required': ['table', 'column'],
        'properties': {
            'table': {'type': 'string'},
            'column': {'type': 'string'},
        },
        'additionalProperties': False}

    always_foll_cols_arr = {
        'type': 'array',
        'items': {'$ref': '#/definitions/always_follow_cols'}}

    root_definition = {
        'type': 'array',
        'items': {
            'anyOf': [
                {'type': 'object',
                 'properties': {'subjects': subjects_arr},
                 'additionalProperties': False},
                {'type': 'object',
                 'properties': {'relations': rels_arr},
                 'additionalProperties': False},
                {'type': 'object',
                 'properties': {'always-follow-columns': always_foll_cols_arr},
                 'additionalProperties': False},
            ],
        }
    }

    definitions = {
        'root': root_definition,
        'relation': relation_definition,
        'table': table_definition,
        'subject': subject_definition,
        'always_follow_cols': always_follow_cols_definition,
    }

    relation_schema = {'$ref': '#/definitions/relation',
                       'definitions': definitions}
    table_schema = {'$ref': '#/definitions/table',
                    'definitions': definitions}
    subject_schema = {'$ref': '#/definitions/subject',
                      'definitions': definitions}
    root_schema = {'$ref': '#/definitions/root',
                   'definitions': definitions}

    table_validator = Draft4Validator(table_schema)
    subject_validator = Draft4Validator(subject_schema)
    relation_validator = Draft4Validator(relation_schema)
    root_validator = Draft4Validator(root_schema)

    def __init__(self):
        self.relations = []
        self.subjects = []
        self.always_follow_cols = []

    @staticmethod
    def get_single_key_dict(data):
        assert isinstance(data, dict)
        assert len(data.keys()) == 1
        key = data.keys()[0]
        list_data = data[key]
        assert isinstance(list_data, list)
        return (key, list_data)

    @staticmethod
    def load(schema, data):
        model = ExtractionModel()
        ExtractionModel.root_validator.validate(data)

        for top_level_element in data:
            (key, list_data) = ExtractionModel.get_single_key_dict(
                top_level_element)

            if key == 'relations':
                model._add_relations(model.relations, list_data)
            elif key == 'subjects':
                model._add_subjects(model.relations, list_data)
            elif key == 'always-follow-columns':
                model._add_always_follow_cols(list_data)

        return model

    def _add_relations(self, target, data):
        for relation_data in data:
            self.relation_validator.validate(relation_data)
            target.append(Relation(
                table=relation_data['table'],
                column=relation_data.get('column'),
                name=relation_data.get('name'),
                disabled=relation_data.get('disabled', False)))

    def _add_tables(self, target, data):
        for table_data in data:
            self.table_validator.validate(table_data)

            if 'column' in table_data and 'values' not in table_data:
                raise Exception('A table with a column must have values')
            if 'values' in table_data and 'column' not in table_data:
                raise Exception('A table with values must have a column')

            target.append(Table(
                table=table_data['table'],
                column=table_data.get('column'),
                values=table_data.get('values')))

    def _add_subjects(self, target, data):
        for subject_data in data:
            self.subject_validator.validate(subject_data)

            subject = Subject()
            self.subjects.append(subject)

            for subject_data_row in subject_data:
                (key, list_data) = ExtractionModel.get_single_key_dict(
                    subject_data_row)
                if key == 'relations':
                    self._add_relations(subject.relations, list_data)
                elif key == 'tables':
                    self._add_tables(subject.tables, list_data)

            if len(subject.tables) == 0:
                raise Exception('A subject must have at least one table')

    def _add_always_follow_cols(self, data):
        for row in data:
            self.always_follow_cols.append(AlwaysFollowColumn(
                row['table'],
                row['column']))

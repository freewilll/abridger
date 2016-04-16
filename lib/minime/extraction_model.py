from jsonschema import Draft4Validator


class Subject(object):
    def __init__(self):
        self.relations = []
        self.tables = []


class Relation(object):
    def __init__(self, table, column, name, disabled, sticky):
        self.table = table
        self.column = column
        self.name = name
        self.disabled = disabled
        self.sticky = sticky


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
            'sticky': {'type': ['boolean']},
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
                model._add_relations(schema, model.relations, list_data)
            elif key == 'subjects':
                model._add_subjects(schema, model.relations, list_data)
            elif key == 'always-follow-columns':
                model._add_always_follow_cols(schema, list_data)

        return model

    def _add_relations(self, schema, target, data):
        for relation_data in data:
            self.relation_validator.validate(relation_data)

            table_name = relation_data['table']
            column_name = relation_data.get('column')
            (table, column) = self._check_table_and_column(schema, table_name,
                                                           column_name)

            target.append(Relation(
                table=table,
                column=column,
                name=relation_data.get('name'),
                disabled=relation_data.get('disabled', False),
                sticky=relation_data.get('sticky', False)))

    def _check_table_and_column(self, schema, table_name, column_name):
        '''Ensure the table exists and if not-None, column exists on the
           table'''
        table = schema.tables_by_name.get(table_name)
        if table is None:
            raise Exception('Unknown table: "%s"' % table_name)

        if column_name is not None:
            column = table.cols_by_name.get(column_name)
            if column is None:
                raise Exception('Unknown column: "%s" on table "%s"' % (
                    column_name, table_name))
        else:
            column = None

        return (table, column)

    def _add_tables(self, schema, target, data):
        for table_data in data:
            self.table_validator.validate(table_data)

            if 'column' in table_data and 'values' not in table_data:
                raise Exception('A table with a column must have values')
            if 'values' in table_data and 'column' not in table_data:
                raise Exception('A table with values must have a column')

            table_name = table_data['table']
            column_name = table_data.get('column')
            (table, column) = self._check_table_and_column(schema, table_name,
                                                           column_name)

            target.append(Table(
                table=table,
                column=column,
                values=table_data.get('values')))

    def _add_subjects(self, schema, target, data):
        for subject_data in data:
            self.subject_validator.validate(subject_data)

            subject = Subject()
            self.subjects.append(subject)

            for subject_data_row in subject_data:
                (key, list_data) = ExtractionModel.get_single_key_dict(
                    subject_data_row)
                if key == 'relations':
                    self._add_relations(schema, subject.relations, list_data)
                elif key == 'tables':
                    self._add_tables(schema, subject.tables, list_data)

            if len(subject.tables) == 0:
                raise Exception('A subject must have at least one table')

    def _add_always_follow_cols(self, schema, data):
        for row in data:
            table_name = row['table']
            column_name = row['column']
            (table, column) = self._check_table_and_column(schema, table_name,
                                                           column_name)

            self.always_follow_cols.append(AlwaysFollowColumn(
                table, column))

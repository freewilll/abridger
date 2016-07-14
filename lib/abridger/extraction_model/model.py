from jsonschema import Draft4Validator

from .not_null_column import NotNullColumn  # noqa
from .relation import Relation, dedupe_relations, merge_relations  # noqa
from .subject import Subject  # noqa
from .table import Table  # noqa
from abridger.exc import (UnknownTableError, UnknownColumnError,
                          InvalidConfigError, RelationIntegrityError)


class ExtractionModel(object):
    relation_definition = {
        'type': 'object',
        'properties': {
            'defaults': {'enum': Relation.DEFAULTS},
            'table': {'type': 'string'},
            'column': {'type': 'string'},
            'name': {'type': ['string', 'null']},
            'disabled': {'type': ['boolean']},
            'sticky': {'type': ['boolean']},
            'type': {'enum': Relation.TYPES},
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
            'type': 'object',
            'properties': {
                'tables': tables_arr,
                'relations': rels_arr
            },
            'additionalProperties': False,
        }
    }

    not_null_cols_definition = {
        'type': 'object',
        'required': ['table', 'column'],
        'properties': {
            'table': {'type': 'string'},
            'column': {'type': 'string'},
        },
        'additionalProperties': False}

    not_null_cols_arr = {
        'type': 'array',
        'items': {'$ref': '#/definitions/not_null_cols'}}

    root_definition = {
        'type': 'array',
        'items': {
            'type': 'object',
            'properties': {
                'subject': subject_definition,
                'relations': rels_arr,
                'not-null-columns': not_null_cols_arr,
            },
            'additionalProperties': False,
        }
    }

    definitions = {
        'root': root_definition,
        'relation': relation_definition,
        'table': table_definition,
        'subject': subject_definition,
        'not_null_cols': not_null_cols_definition,
    }

    relation_schema = {'$ref': '#/definitions/relation',
                       'definitions': definitions}
    table_schema = {'$ref': '#/definitions/table',
                    'definitions': definitions}
    subject_schema = {'$ref': '#/definitions/subject',
                      'definitions': definitions}
    root_schema = {'$ref': '#/definitions/root',
                   'definitions': definitions}
    root_validator = Draft4Validator(root_schema)

    def __init__(self, schema):
        self.schema = schema
        self.relations = []
        self.subjects = []
        self.not_null_cols = []
        self._got_relation_defaults = False

    @staticmethod
    def _get_single_key_dict(data):
        assert isinstance(data, dict)
        if len(list(data.keys())) != 1:
            raise InvalidConfigError('Expected one key, got %s' %
                                     sorted(data.keys()))
        key = list(data.keys())[0]
        list_data = data[key]
        assert isinstance(list_data, list)
        return (key, list_data)

    @staticmethod
    def load(schema, data):
        model = ExtractionModel(schema)
        ExtractionModel.root_validator.validate(data)

        for top_level_element in data:
            (key, list_data) = ExtractionModel._get_single_key_dict(
                top_level_element)

            if key == 'relations':
                model._add_relations(model.relations, list_data)
            elif key == 'subject':
                model._add_subject(model.relations, list_data)
            elif key == 'not-null-columns':
                model._add_not_null_cols(list_data)

        model._finalize_default_relations()
        return model

    def _check_table_and_column(self, table_name, column_name):
        '''Ensure the table exists and if not-None, column exists on the
           table'''
        table = self.schema.tables_by_name.get(table_name)
        if table is None:
            raise UnknownTableError('Unknown table: "%s"' % table_name)

        if column_name is not None:
            col = table.cols_by_name.get(column_name)
            if col is None:
                raise UnknownColumnError(
                    'Unknown column: "%s" on table "%s"' % (
                        column_name, table_name))
        else:
            col = None

        return (table, col)

    def _add_relation(self, target, table=None, foreign_key=None, type=None,
                      name=None, disabled=False, sticky=False):
        target.append(Relation(
            table=table,
            foreign_key=foreign_key,
            name=name,
            disabled=disabled,
            sticky=sticky,
            type=type))

    def _add_table_relation(self, target, relation_data):
        table_name = relation_data['table']
        col_name = relation_data.get('column')

        if col_name is None:
            raise RelationIntegrityError(
                "Non default relations must have a column on table '%s'" %
                table_name)

        (table, col) = self._check_table_and_column(table_name, col_name)

        foreign_key = None
        if col is not None:
            for fk in table.foreign_keys:
                if col in fk.src_cols:
                    foreign_key = fk
                    break
            if foreign_key is None:
                raise RelationIntegrityError(
                    "Relations can only be used on foreign keys."
                    "Column %s on table %s isn't a foreign key." % (
                        col.name, table.name))

        # Note: validation will ensure the type is valid
        type = relation_data.get('type', Relation.TYPE_INCOMING)
        disabled = relation_data.get('disabled', False)
        sticky = relation_data.get('sticky', False)

        if (disabled and foreign_key is not None and
                type == Relation.TYPE_OUTGOING and foreign_key.notnull):
            raise RelationIntegrityError(
                'Cannot disable outgoing not null foreign keys on column '
                '%s as this would lead to an integrity error' % col)

        if disabled and 'sticky' in relation_data:
            raise InvalidConfigError(
                'The sticky flag is meaningless on disabled relations')

        self._add_relation(
            target,
            table=table,
            foreign_key=foreign_key,
            name=relation_data.get('name'),
            disabled=disabled,
            sticky=sticky,
            type=type)

    def _finalize_default_relations(self):
        # If no relations are specified, the default is to have:
        # - outgoing nullable
        # - outgoing not null
        if not self._got_relation_defaults:
                self._add_default_relations(
                    self.relations, Relation.DEFAULT_OUTGOING_NULLABLE)

        self._add_default_relations(
            self.relations, Relation.DEFAULT_OUTGOING_NOTNULL)

        self.relations = dedupe_relations(self.relations)

    def _add_default_relations(self, target, defaults):
        # Determine what we need based on the defaults setting and what
        # has already been previously added

        want_outgoing_nullables = defaults in [
            Relation.DEFAULT_OUTGOING_NULLABLE,
            Relation.DEFAULT_EVERYTHING]

        want_incoming = defaults in [
            Relation.DEFAULT_INCOMING,
            Relation.DEFAULT_EVERYTHING]

        def _add_rel(table, fk, type):
            self._add_relation(target, table=table, foreign_key=fk, type=type)

        for table in self.schema.tables:
            for fk in table.foreign_keys:
                if (fk.notnull or
                        (not fk.notnull and want_outgoing_nullables)):
                    _add_rel(table, fk, type=Relation.TYPE_OUTGOING)
                if want_incoming:
                    _add_rel(table, fk, type=Relation.TYPE_INCOMING)

        self._got_relation_defaults = True

    def _add_relations(self, target, data):
        for relation_data in data:
            defaults = relation_data.get('defaults')
            table_name = relation_data.get('table')

            if (defaults is None) == (table_name is None):
                raise InvalidConfigError(
                    'Either defaults or table must be set')

            if table_name is not None:
                self._add_table_relation(target, relation_data)
            else:
                defaults = relation_data.get('defaults')
                self._add_default_relations(target, defaults)

    def _add_tables(self, target, data):
        for table_data in data:
            if 'column' in table_data and 'values' not in table_data:
                raise InvalidConfigError(
                    'A table with a column must have values')
            if 'values' in table_data and 'column' not in table_data:
                raise InvalidConfigError(
                    'A table with values must have a column')

            table_name = table_data['table']
            col_name = table_data.get('column')
            (table, col) = self._check_table_and_column(table_name, col_name)

            target.append(Table(
                table=table,
                col=col,
                values=table_data.get('values')))

    def _add_subject(self, target, subject_data):
        subject = Subject()
        self.subjects.append(subject)

        for subject_data_row in subject_data:
            (key, list_data) = ExtractionModel._get_single_key_dict(
                subject_data_row)
            if key == 'relations':
                self._add_relations(subject.relations, list_data)
            elif key == 'tables':
                self._add_tables(subject.tables, list_data)

        if len(subject.tables) == 0:
            raise InvalidConfigError('A subject must have at least one table')

    def _add_not_null_cols(self, data):
        for row in data:
            table_name = row['table']
            col_name = row['column']
            (table, col) = self._check_table_and_column(table_name, col_name)

            # Check it's a foreign key
            found_fk = False
            for foreign_key in table.foreign_keys:
                for fk_col in foreign_key.src_cols:
                    if col == fk_col:
                        found_fk = foreign_key
            if not found_fk:
                raise RelationIntegrityError(
                    "not-null-columns can only be used on foreign keys."
                    "Column %s on table %s isn't a foreign key." % (
                        col.name, table.name))

            self.not_null_cols.append(NotNullColumn(
                table, col, found_fk))

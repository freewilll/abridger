from collections import defaultdict
from jsonschema import Draft4Validator
from abridger.exc import (UnknownTableError, UnknownColumnError,
                          InvalidConfigError, RelationIntegrityError)


class Subject(object):
    def __init__(self):
        self.relations = []
        self.tables = []

    def __repr__(self):
        return '<Subject %s>' % id(self)


class Relation(object):
    TYPE_INCOMING = 'incoming'
    TYPE_OUTGOING = 'outgoing'
    TYPES = [TYPE_INCOMING, TYPE_OUTGOING]

    DEFAULT_OUTGOING_NOTNULL = 'all-outgoing-not-null'
    DEFAULT_OUTGOING_NULLABLE = 'all-outgoing-nullable'
    DEFAULT_INCOMING = 'all-incoming'
    DEFAULT_EVERYTHING = 'everything'
    DEFAULTS = [DEFAULT_OUTGOING_NOTNULL, DEFAULT_OUTGOING_NULLABLE,
                DEFAULT_INCOMING, DEFAULT_EVERYTHING]

    def __init__(self, table, column, name, disabled, sticky, type):
        self.table = table
        self.column = column
        self.name = name
        self.disabled = disabled
        self.propagate_sticky = sticky
        self.only_if_sticky = \
            (type == self.TYPE_OUTGOING and sticky and not column.notnull) or \
            (type == self.TYPE_INCOMING and sticky)
        self.type = type

    def __str__(self):
        flags = ','.join([s for s in [
            'propagate_sticky' if self.propagate_sticky else None,
            'only_if_sticky' if self.only_if_sticky else None,
            'disabled' if self.disabled else None
        ] if s is not None])
        if flags:
            flags = ' ' + flags
        return '%s:%s name=%s type=%s%s' % (
            self.table.name,
            self.column.name if self.column is not None else 'None',
            self.name,
            self.type,
            flags)

    def __repr__(self):
        return '<Relation %s>' % str(self)

    def __eq__(self, other):
        return hash(self) == hash(other)

    def _base_list(self):
        return [
            self.table.name,
            self.column.name if self.column is not None else '-',
            self.name or '-',
            self.type]

    def _base_hash(self):
        '''Returns a hash of everything except sticky and disabled'''
        return hash('.'.join(self._base_list()))

    def __hash__(self):
        return hash('.'.join(self._base_list() + [
            str(self.disabled),
            str(self.propagate_sticky),
            str(self.only_if_sticky)]))

    def clone(self):
        relation = Relation(self.table, self.column, self.name, self.disabled,
                            False, self.type)
        relation.propagate_sticky = self.propagate_sticky
        relation.only_if_sticky = self.only_if_sticky
        return relation


def dedupe_relations(relations):
    # Dedupe relations, preserving ordering
    new_relations = []
    seen_relations = set()
    for relation in relations:
        if relation not in seen_relations:
            new_relations.append(relation)
            seen_relations.add(relation)
    return new_relations


def merge_relations(relations):
    same_relations = defaultdict(list)
    for relation in dedupe_relations(relations):
        same_relations[relation._base_hash()].append(relation)

    results = []
    for related_relations in list(same_relations.values()):
        disabled = False
        propagate_sticky = False
        only_if_sticky = False
        for relation in related_relations:
            if relation.disabled:
                disabled = True
            if relation.propagate_sticky:
                propagate_sticky = True
            if relation.only_if_sticky:
                only_if_sticky = True

        if not disabled:
            relation = related_relations[0]
            relation.disabled = False
            relation.propagate_sticky = propagate_sticky
            relation.only_if_sticky = only_if_sticky
            results.append(relation)

    return results


class NotNullColumn(object):
    def __init__(self, table, column, foreign_key):
        self.table = table
        self.column = column
        self.foreign_key = foreign_key


class Table(object):
    def __init__(self, table, column, values):
        self.table = table
        self.column = column
        self.values = values


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
            'anyOf': [
                {'type': 'object',
                 'properties': {'subject': subject_definition},
                 'additionalProperties': False},
                {'type': 'object',
                 'properties': {'relations': rels_arr},
                 'additionalProperties': False},
                {'type': 'object',
                 'properties': {'not-null-columns': not_null_cols_arr},
                 'additionalProperties': False},
            ],
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

    table_validator = Draft4Validator(table_schema)
    subject_validator = Draft4Validator(subject_schema)
    relation_validator = Draft4Validator(relation_schema)
    root_validator = Draft4Validator(root_schema)

    def __init__(self, schema):
        self.schema = schema
        self.relations = []
        self.subjects = []
        self.not_null_cols = []
        self._got_relation_defaults = False

    @staticmethod
    def get_single_key_dict(data):
        assert isinstance(data, dict)
        assert len(list(data.keys())) == 1
        key = list(data.keys())[0]
        list_data = data[key]
        assert isinstance(list_data, list)
        return (key, list_data)

    @staticmethod
    def load(schema, data):
        model = ExtractionModel(schema)
        ExtractionModel.root_validator.validate(data)

        for top_level_element in data:
            (key, list_data) = ExtractionModel.get_single_key_dict(
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
            column = table.cols_by_name.get(column_name)
            if column is None:
                raise UnknownColumnError(
                    'Unknown column: "%s" on table "%s"' % (
                        column_name, table_name))
        else:
            column = None

        return (table, column)

    def _add_relation(self, target, table=None, column=None, type=None,
                      name=None, disabled=False, sticky=False):
        target.append(Relation(
            table=table,
            column=column,
            name=name,
            disabled=disabled,
            sticky=sticky,
            type=type))

    def _add_table_relation(self, target, relation_data):
        table_name = relation_data.get('table')
        column_name = relation_data.get('column')
        (table, column) = self._check_table_and_column(table_name,
                                                       column_name)

        # Note: validation will ensure the type is valid
        type = relation_data.get('type', Relation.TYPE_INCOMING)
        disabled = relation_data.get('disabled', False)
        sticky = relation_data.get('sticky', False)

        if (disabled and column is not None and
                type == Relation.TYPE_OUTGOING and column.notnull):
            raise RelationIntegrityError(
                'Cannot disable outgoing not null foreign keys on column '
                '%s as this would lead to an integrity error' % column)

        if disabled and 'sticky' in relation_data:
            raise InvalidConfigError(
                'The sticky flag is meaningless on disabled relations')

        self._add_relation(
            target,
            table=table,
            column=column,
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

        def _add_rel(table, column, type):
            self._add_relation(target, table=table, column=column, type=type)

        for table in self.schema.tables:
            for fk in table.foreign_keys:
                first_fk_col = fk.src_cols[0]
                if (fk.notnull or
                        (not fk.notnull and want_outgoing_nullables)):
                    _add_rel(table, first_fk_col, type=Relation.TYPE_OUTGOING)
                if want_incoming:
                    _add_rel(table, first_fk_col, type=Relation.TYPE_INCOMING)

        self._got_relation_defaults = True

    def _add_relations(self, target, data):
        for relation_data in data:
            self.relation_validator.validate(relation_data)

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
            self.table_validator.validate(table_data)

            if 'column' in table_data and 'values' not in table_data:
                raise InvalidConfigError(
                    'A table with a column must have values')
            if 'values' in table_data and 'column' not in table_data:
                raise InvalidConfigError(
                    'A table with values must have a column')

            table_name = table_data['table']
            column_name = table_data.get('column')
            (table, column) = self._check_table_and_column(table_name,
                                                           column_name)

            target.append(Table(
                table=table,
                column=column,
                values=table_data.get('values')))

    def _add_subject(self, target, subject_data):
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
            raise InvalidConfigError('A subject must have at least one table')

    def _add_not_null_cols(self, data):
        for row in data:
            table_name = row['table']
            column_name = row['column']
            (table, column) = self._check_table_and_column(table_name,
                                                           column_name)

            # Check it's a foreign key
            found_fk = False
            for foreign_key in table.foreign_keys:
                for fk_col in foreign_key.src_cols:
                    if column == fk_col:
                        found_fk = foreign_key
            if not found_fk:
                raise RelationIntegrityError(
                    "not-null-columns can only be used on foreign keys."
                    "Column %s on table %s isn't a foreign key." % (
                        column.name, table.name))

            self.not_null_cols.append(NotNullColumn(
                table, column, found_fk))

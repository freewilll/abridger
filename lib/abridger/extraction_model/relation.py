from collections import defaultdict


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

    def __init__(self, table, foreign_key, name, disabled, sticky, type):
        self.table = table
        self.foreign_key = foreign_key
        self.name = name
        self.disabled = disabled
        self.propagate_sticky = sticky
        self.only_if_sticky = \
            (type == self.TYPE_OUTGOING and sticky and
             not foreign_key.notnull) or \
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
            str(self.foreign_key) if self.foreign_key is not None else '-',
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
            str(self.foreign_key) if self.foreign_key is not None else '-',
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
        relation = Relation(self.table, self.foreign_key, self.name,
                            self.disabled, False, self.type)
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

import Queue
from collections import defaultdict
from minime.extraction_model import Relation


class WorkItem(object):
    def __init__(self, subject, table, cols, values):
        assert (cols is None) == (values is None)

        self.subject = subject
        self.table = table
        self.cols = cols
        self.values = values

    def prune(self, existing_results):
        '''Remove values that are in existing_results'''

        if self.cols is None:
            return

        if self.table not in existing_results:
            return

        table_results = existing_results[self.table]

        if self.cols not in table_results:
            return

        table_values = table_results[self.cols]

        new_values = []
        for value in self.values:
            if value not in table_values:
                new_values.append(value)

        self.values = new_values

    def fetch_rows(self, dbconn):
        return dbconn.fetch_rows(self.table, self.cols, self.values)


class Rocket(object):
    def __init__(self, dbconn, extraction_model):
        self.dbconn = dbconn
        self.extraction_model = extraction_model
        self.work_queue = Queue.Queue()
        self.results = defaultdict(lambda: defaultdict(
            lambda: defaultdict(list)))
        self.fetch_count = 0
        self.fetched_row_count = 0
        self.fetched_row_count_per_table = defaultdict(int)

        self.subject_table_relations = {}
        for subject in extraction_model.subjects:
            for table in subject.tables:
                if table.values is not None:
                    if not isinstance(table.values, list):
                        table.values = [table.values]
                    value_tuples = [(v,) for v in table.values]
                    cols = (table.column,)
                else:
                    value_tuples = None
                    cols = None
                self.work_queue.put(WorkItem(
                    subject, table.table, cols, value_tuples))
            self._make_subject_table_relations(subject)

    def _make_subject_table_relations(self, subject):
        table_relations = defaultdict(list)

        # Add subject and global relations
        for relation in self.extraction_model.relations + subject.relations:
            col = relation.column

            # TODO process disabled relations
            if col is None:
                continue

            found_fk = None
            for fk in col.table.foreign_keys:
                if len(fk.src_cols) == 1 and fk.src_cols[0] == col:
                    found_fk = fk
            assert found_fk is not None

            if relation.type == Relation.TYPE_INCOMING:
                table_relations[found_fk.dst_cols[0].table].append(
                    (found_fk.dst_cols, found_fk.src_cols))
            else:
                table_relations[found_fk.src_cols[0].table].append(
                    (found_fk.src_cols, found_fk.dst_cols))

        self.subject_table_relations[subject] = table_relations

    def _lookup_row_values(self, table, row, keys):
        cols = table.cols
        values = []
        for key_tuple in keys:
            value = []
            for key_column in key_tuple:
                value.append(row[cols.index(key_column)])
            values.append(tuple(value))
        return values

    def launch(self):
        while not self.work_queue.empty():
            work_item = self.work_queue.get()
            table = work_item.table

            work_item.prune(self.results)

            if work_item.values is not None and len(work_item.values) == 0:
                continue  # All wanted have already been fetched

            rows = list(work_item.fetch_rows(self.dbconn))
            self.fetch_count += 1

            if table.primary_key is None:
                # TODO tables without primary keys
                continue

            keys = set([table.primary_key])

            for row in rows:
                self.fetched_row_count += 1
                self.fetched_row_count_per_table[table] += 1
                values = self._lookup_row_values(table, row, keys)
                for key, values in zip(keys, values):
                    self.results[table][key][values] = row

            table_relations = self.subject_table_relations[work_item.subject]
            if table in table_relations:
                relations = table_relations[table]
                for (src_cols, dst_cols) in relations:
                    dst_table = dst_cols[0].table
                    src_col_indexes = [table.cols.index(c) for c in src_cols]

                    dst_values = []
                    for row in rows:
                        value_tuple = tuple([row[i] for i in src_col_indexes])
                        dst_values.append(value_tuple)

                    # FIXME don't process null foreign keys
                    self.work_queue.put(WorkItem(
                        work_item.subject, dst_table, dst_cols, dst_values))

        return self

    def flat_results(self):
        results = []
        for table in sorted(self.results, key=lambda r: r.name):
            primary_key = table.primary_key
            rows = self.results[table][primary_key]
            for values in sorted(rows.values()):
                results.append((table, values))
        return results

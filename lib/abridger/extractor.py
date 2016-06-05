from __future__ import print_function
from queue import Queue
from collections import defaultdict
import sys
from abridger.extraction_model import Relation, merge_relations


class ResultsRow(object):
    def __init__(self, table, row, subjects=None, sticky=False, count=1):
        if subjects is None:
            subjects = set()
        self.table = table
        self.row = row
        self.subjects = subjects
        self.sticky = sticky
        self.count = 1

    def __str__(self):
        return 'row=%s subjects=%s sticky=%s' % (
            self.row, self.subjects, self.sticky)

    def __repr__(self):
        return '<ResultsRow %s>' % str(self)

    def __lt__(self, other):
        return self.row < other.row

    def merge(self, other):
        '''
            Merge two results rows. not-null values take precedence over nulls
        '''

        changed_row = None
        for i, (self_val, other_val) in enumerate(zip(self.row, other.row)):
            if (self_val is None) and (other_val is not None):
                if not changed_row:
                    changed_row = list(self.row)
                changed_row[i] = other_val

        if changed_row is not None:
            self.row = tuple(changed_row)


class WorkItem(object):
    def __init__(self, subject, table, cols, values, sticky,
                 parent_work_item=None, parent_results_row=None):
        assert (cols is None) == (values is None)

        self.subject = subject
        self.table = table
        self.cols = cols
        self.values = values
        self.sticky = sticky

        self.set_history(parent_work_item, parent_results_row)

    def value_hash(self, value):
        return hash(tuple([self.subject, self.table, self.cols, value,
                          self.sticky]))

    def fetch_rows(self, database):
        fetched_rows = database.fetch_rows(self.table, self.cols, self.values)
        fetched_rows = [ResultsRow(self.table, fr) for fr in fetched_rows]
        return fetched_rows

    def make_work_item_history(self):
        if self.values is not None:
            cols_csv = ','.join([c.name for c in self.cols])
            values_csv = ','.join([str(v) for v in list(self.values[0])])
            if len(self.values[0]) > 1:
                values_csv = '(%s)' % values_csv
                cols_csv = '(%s)' % cols_csv
            return (self.table, cols_csv, values_csv, self.sticky)
        else:
            return (self.table, None, None, self.sticky)

    def make_results_row_history(self, results_row):
        epk = results_row.table.effective_primary_key
        col_indexes = [results_row.table.cols.index(c) for c in epk]

        cols_csv = ','.join([c.name for c in epk])
        values = [results_row.row[i] for i in col_indexes]
        values_csv = ','.join([str(v) for v in values])
        if len(values) > 1:
            values_csv = '(%s)' % values_csv
            cols_csv = '(%s)' % cols_csv
        return (results_row.table, cols_csv, values_csv, self.sticky)

    def set_history(self, work_item, results_row):
        if work_item is None:
            self.history = [self.make_work_item_history()]
            return

        self.history = list(work_item.history)
        work_item_history = self.make_work_item_history()

        # if results_row is not None:
        results_row_history = self.make_results_row_history(results_row)
        if self.history[-1] != results_row_history:
            self.history.append(results_row_history)

        if work_item_history != results_row_history:
            self.history.append(work_item_history)

    def print_history(self):
        if len(self.history) == 0:
            return

        first = True
        for (table, cols, values, sticky) in self.history:
            if not first:
                print(' -> ', end='')
            else:
                first = False

            if cols is not None:
                print('%s.%s=%s' % (table, cols, values), end='')
            else:
                print(table, end='')

            if sticky:
                sys.stdout.write('*')
        print()


class Extractor(object):
    def __init__(self, database, extraction_model, explain=False):
        self.database = database
        self.extraction_model = extraction_model

        self.explain = explain
        self.work_queue = Queue()
        self.results = defaultdict(lambda: defaultdict(dict))
        self.fetch_count = 0
        self.fetched_row_count = 0
        self.fetched_row_count_per_table = defaultdict(int)
        self.seen_work_items = set()

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
                    subject, table.table, cols, value_tuples, True))
            self._make_subject_table_relations(subject)

    def _make_subject_table_relations(self, subject):
        table_relations = defaultdict(list)

        relations = merge_relations(self.extraction_model.relations +
                                    subject.relations)

        # Add subject and global relations
        for relation in relations:
            col = relation.column

            if col is None:
                # TODO relations without a column on a subject
                # Bear in mind that stickiness has to be transmitted.
                continue

            found_fk = None
            for fk in col.table.foreign_keys:
                if len(fk.src_cols) == 1 and fk.src_cols[0] == col:
                    found_fk = fk
            assert found_fk is not None

            if relation.type == Relation.TYPE_INCOMING:
                table_relations[found_fk.dst_cols[0].table].append(
                    (found_fk.dst_cols, found_fk.src_cols,
                     relation.propagate_sticky, relation.only_if_sticky))
            else:
                table_relations[found_fk.src_cols[0].table].append(
                    (found_fk.src_cols, found_fk.dst_cols,
                     relation.propagate_sticky, relation.only_if_sticky))

        self.subject_table_relations[subject] = table_relations

    def _lookup_row_value(self, col_indexes, results_row, key_tuple):
        value = []
        for col in key_tuple:
            value.append(results_row.row[col_indexes[col]])
        return tuple(value)

    def _process_work_item_relations(self, work_item, results_rows,
                                     relations, processed_outgoing_fk_cols):
        table = work_item.table

        for (src_cols, dst_cols, propagate_sticky,
                only_if_sticky) in relations:
            if only_if_sticky and not work_item.sticky:
                continue

            sticky = work_item.sticky and propagate_sticky
            processed_outgoing_fk_cols |= set(src_cols)

            dst_table = dst_cols[0].table
            src_col_indexes = [table.cols.index(c) for c in src_cols]

            dst_values = []
            seen_dst_values = set()
            for results_row in results_rows:
                value_tuple = tuple(
                    [results_row.row[i] for i in src_col_indexes])

                if any(s is None for s in value_tuple):
                    # Don't process any foreign keys if any of the
                    # values is None.
                    continue

                if value_tuple not in seen_dst_values:
                    dst_values.append(value_tuple)
                seen_dst_values.add(value_tuple)

                if self.explain:
                    for dst_value in dst_values:
                        self.work_queue.put(WorkItem(
                            work_item.subject, dst_table, dst_cols,
                            [dst_value], sticky, parent_work_item=work_item,
                            parent_results_row=results_row))

            if not self.explain and len(dst_values) > 0:
                self.work_queue.put(WorkItem(
                    work_item.subject, dst_table, dst_cols,
                    dst_values, sticky))

    def _process_work_item_results_rows(self, work_item, results_rows,
                                        processed_outgoing_fk_cols):
        table = work_item.table
        (epk, count_identical_rows) = (table.effective_primary_key,
                                       table.can_have_duplicated_rows)

        all_fk_cols = set()
        for foreign_key in table.foreign_keys:
            all_fk_cols |= set(foreign_key.src_cols)

        cols_that_need_nulling = all_fk_cols - processed_outgoing_fk_cols
        if len(cols_that_need_nulling) > 0:
            indexes = [table.cols.index(c) for c in cols_that_need_nulling]
            for i, results_row in enumerate(results_rows):
                row_list = list(results_row.row)
                for j in indexes:
                    row_list[j] = None
                results_rows[i] = ResultsRow(table, tuple(row_list),
                                             results_row.subjects)

        end_results_counts = defaultdict(int)
        table_epk_results = self.results[table][epk]
        col_indexes = {col: table.cols.index(col) for col in table.cols}

        for results_row in results_rows:
            results_row.subjects.add(work_item.subject)
            self.fetched_row_count += 1
            self.fetched_row_count_per_table[table] += 1
            value = self._lookup_row_value(col_indexes, results_row, epk)
            if count_identical_rows:
                end_results_counts[value] += 1
            if value in table_epk_results:
                found_results_row = table_epk_results[value]
                if results_row.row != found_results_row.row:
                    results_row.merge(found_results_row)

            table_epk_results[value] = results_row

        if count_identical_rows:
            for value in end_results_counts:
                count = end_results_counts[value]
                table_epk_results[value].count = count

    def _process_work_item(self, work_item):
        if self.explain:
            work_item.print_history()

        table = work_item.table

        results_rows = work_item.fetch_rows(self.database)
        self.fetch_count += 1

        if len(results_rows) == 0:
            return

        table_relations = self.subject_table_relations[work_item.subject]
        processed_outgoing_fk_cols = set()

        self._process_work_item_relations(
            work_item, results_rows,
            table_relations.get(table, []), processed_outgoing_fk_cols)

        self._process_work_item_results_rows(work_item, results_rows,
                                             processed_outgoing_fk_cols)

    def launch(self):
        while not self.work_queue.empty():
            work_item = self.work_queue.get()

            # Filter values by what's already been processed
            if work_item.cols is None:
                self._process_work_item(work_item)
            else:
                new_values = []
                for value in work_item.values:
                    h = work_item.value_hash(value)
                    if h not in self.seen_work_items:
                        new_values.append(value)

                if len(new_values) > 0:
                    work_item.values = new_values
                    self._process_work_item(work_item)

                for value in work_item.values:
                    h = work_item.value_hash(value)
                    self.seen_work_items.add(h)

        return self

    def flat_results(self):
        results = []
        for table in sorted(self.results, key=lambda r: r.name):
            epk = table.effective_primary_key
            results_rows = self.results[table][epk]
            for results_row in sorted(results_rows.values()):
                for i in range(results_row.count):
                    results.append((table, results_row.row))
        return results

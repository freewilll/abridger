from __future__ import print_function
from collections import defaultdict
from queue import Queue
from time import time

from abridger.extraction_model import Relation, merge_relations
from .results_row import ResultsRow
from .work_item import WorkItem


class Extractor(object):
    def __init__(self, database, extraction_model, explain=False,
                 verbosity=0):
        self.database = database
        self.extraction_model = extraction_model
        self.explain = explain
        self.verbosity = verbosity
        self.work_queue = Queue()
        self.results = defaultdict(lambda: defaultdict(dict))
        self.fetch_count = 0
        self.fetched_row_count = 0
        self.fetched_row_count_per_table = defaultdict(int)
        self.max_depth = 0
        self.seen_work_items = set()

        self.subject_table_relations = {}
        for subject in extraction_model.subjects:
            for table in subject.tables:
                if table.values is not None:
                    if not isinstance(table.values, list):
                        table.values = [table.values]
                    value_tuples = [(v,) for v in table.values]
                    cols = (table.col,)
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
            fk = relation.foreign_key
            if relation.type == Relation.TYPE_INCOMING:
                table_relations[fk.dst_cols[0].table].append(
                    (relation.table, fk.dst_cols, fk.src_cols,
                     relation.propagate_sticky, relation.only_if_sticky))
            else:
                table_relations[fk.src_cols[0].table].append(
                    (relation.table, fk.src_cols, fk.dst_cols,
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

        for (relation_table, src_cols, dst_cols, propagate_sticky,
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
                            [dst_value], sticky,
                            parent_work_item=work_item,
                            parent_results_row=results_row))

            if not self.explain and len(dst_values) > 0:
                self.work_queue.put(WorkItem(
                    work_item.subject, dst_table, dst_cols,
                    dst_values, sticky, parent_work_item=work_item))

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
        if work_item.depth > self.max_depth:
            self.max_depth = work_item.depth

        if self.explain:
            work_item.print_history()

        if self.verbosity > 1:
            table_count = len(self.fetched_row_count_per_table.keys())
            print(
                'Processing pass=%-5d queued=%-5d depth=%-3d tables=%-4d '
                'rows=%-7d table %s' % (
                    self.fetch_count + 1,
                    self.work_queue.qsize(),
                    self.max_depth,
                    table_count,
                    self.fetched_row_count,
                    work_item.table.name))

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
        start_time = time()

        while not self.work_queue.empty():
            work_item = self.work_queue.get()

            # Filter values by what's already been processed
            if work_item.cols is None:
                h = work_item.non_value_hash()
                if h not in self.seen_work_items:
                    self._process_work_item(work_item)
                self.seen_work_items.add(h)
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

        elapsed_time = time() - start_time

        if self.verbosity > 0:
            table_count = len(self.fetched_row_count_per_table.keys())
            print(
                'Extraction completed: '
                'fetched rows=%d, '
                'tables=%d, '
                'queries=%d, '
                'depth=%d, '
                'duration=%0.1f seconds' % (
                    self.fetched_row_count,
                    table_count,
                    self.fetch_count,
                    self.max_depth,
                    elapsed_time))

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

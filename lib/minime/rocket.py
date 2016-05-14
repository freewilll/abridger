import Queue
from collections import defaultdict
from minime.extraction_model import Relation, merge_relations


class ResultsRow(object):
    def __init__(self, row, subjects=None, sticky=False, count=1):
        if subjects is None:
            subjects = set()
        self.row = row
        self.subjects = subjects
        self.sticky = sticky
        self.count = 1

    def __str__(self):
        return 'row=%s subjects=%s sticky=%s' % (
            self.row, self.subjects, self.sticky)

    def __repr__(self):
        return '<ResultsRow %s>' % str(self)

    def __cmp__(self, other):
        return cmp(self.row, other.row)


class WorkItem(object):
    def __init__(self, subject, table, cols, values, sticky):
        assert (cols is None) == (values is None)

        self.subject = subject
        self.table = table
        self.cols = cols
        self.values = values
        self.sticky = sticky

    def prune_cached_rows(self, existing_results):
        '''Remove values that are in existing_results'''

        if self.cols is None:
            return []

        if self.table not in existing_results:
            return []

        table_results = existing_results[self.table]
        table_rows = table_results.get(self.cols)
        if table_rows is None:
            return []

        new_values = []
        cached_rows = []
        for value in self.values:
            cached_row = table_rows.get(value)
            if cached_row is None:
                new_values.append(value)
            else:
                cached_rows.append(cached_row)

        self.values = new_values
        return cached_rows

    def fetch_rows(self, dbconn, existing_results):
        cached_rows = self.prune_cached_rows(existing_results)

        if self.values is None or len(self.values) > 0:
            fetched_rows = dbconn.fetch_rows(self.table, self.cols,
                                             self.values)
        else:
            fetched_rows = []

        fetched_rows = [ResultsRow(fr) for fr in fetched_rows]
        return cached_rows + fetched_rows


class Rocket(object):
    def __init__(self, dbconn, extraction_model):
        self.dbconn = dbconn
        self.extraction_model = extraction_model
        self.work_queue = Queue.Queue()
        self.results = defaultdict(lambda: defaultdict(dict))
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

    def _lookup_row_value(self, table, results_row, key_tuple):
        cols = table.cols
        value = []
        for key_column in key_tuple:
            value.append(results_row.row[cols.index(key_column)])
        return tuple(value)

    def _get_effective_pk(self, table):
        if table.primary_key is not None:
            key = table.primary_key
            count_identical_rows = False
        elif table.alternate_primary_key is not None:
            key = table.alternate_primary_key
            count_identical_rows = False
        else:
            key = tuple(table.cols)
            count_identical_rows = True
        return (key, count_identical_rows)

    def launch(self):
        while not self.work_queue.empty():
            work_item = self.work_queue.get()
            table = work_item.table

            if work_item.values is not None and len(work_item.values) == 0:
                continue  # All wanted have already been fetched

            results_rows = work_item.fetch_rows(self.dbconn, self.results)
            self.fetch_count += 1

            if len(results_rows) == 0:
                continue

            (epk, count_identical_rows) = self._get_effective_pk(table)

            table_relations = self.subject_table_relations[work_item.subject]
            processed_outgoing_fk_cols = set()

            if table in table_relations:
                relations = table_relations[table]

                for (src_cols, dst_cols, propagate_sticky,
                        only_if_sticky) in relations:
                    if only_if_sticky and not work_item.sticky:
                        continue

                    sticky = work_item.sticky and propagate_sticky

                    processed_outgoing_fk_cols |= set(src_cols)

                    dst_table = dst_cols[0].table
                    src_col_indexes = [table.cols.index(c) for c in src_cols]

                    table_results = self.results.get(dst_table, {})
                    table_rows = table_results.get(dst_cols, {})

                    dst_values = []
                    seen_dst_values = set()
                    for results_row in results_rows:
                        value_tuple = tuple(
                            [results_row.row[i] for i in src_col_indexes])
                        if any(s is None for s in value_tuple):
                            # Don't process any foreign keys if any of the
                            # values is None
                            continue

                        if value_tuple in seen_dst_values:
                            continue
                        seen_dst_values.add(value_tuple)

                        if value_tuple not in table_rows:
                            dst_values.append(value_tuple)
                        else:
                            results_row = table_rows[value_tuple]
                            if work_item.subject not in results_row.subjects:
                                dst_values.append(value_tuple)

                    if len(dst_values) > 0:
                        self.work_queue.put(WorkItem(
                            work_item.subject, dst_table, dst_cols,
                            dst_values, sticky))

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
                    results_rows[i] = ResultsRow(tuple(row_list),
                                                 results_row.subjects)

            end_results_counts = defaultdict(int)
            table_epk_results = self.results[table][epk]
            for results_row in results_rows:
                results_row.subjects.add(work_item.subject)
                self.fetched_row_count += 1
                self.fetched_row_count_per_table[table] += 1
                value = self._lookup_row_value(table, results_row, epk)
                end_results_counts[value] += 1
                table_epk_results[value] = results_row

            for value in end_results_counts:
                count = end_results_counts[value]
                table_epk_results[value].count = count

        return self

    def flat_results(self):
        results = []
        for table in sorted(self.results, key=lambda r: r.name):
            (epk, count_identical_rows) = self._get_effective_pk(table)
            results_rows = self.results[table][epk]
            for results_row in sorted(results_rows.values()):
                for i in range(results_row.count):
                    results.append((table, results_row.row))
        return results

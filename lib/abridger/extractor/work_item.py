from __future__ import print_function

from .results_row import ResultsRow


class WorkItem(object):
    def __init__(self, subject, table, cols, values, sticky,
                 parent_work_item=None, parent_results_row=None):
        assert (cols is None) == (values is None)

        self.subject = subject
        self.table = table
        self.cols = cols
        self.values = values
        self.sticky = sticky
        self.depth = 0

        if parent_work_item is not None:
            self.depth = parent_work_item.depth + 1

        self._set_history(parent_work_item, parent_results_row)

    def value_hash(self, value):
        return hash(tuple([self.subject, self.table, self.cols, value,
                          self.sticky]))

    def non_value_hash(self):
        return hash(tuple([self.subject, self.table, self.sticky]))

    def fetch_rows(self, database):
        fetched_rows = database.fetch_rows(self.table, self.cols, self.values)
        fetched_rows = [ResultsRow(self.table, fr) for fr in fetched_rows]
        return fetched_rows

    def _make_work_item_history(self):
        if self.values is not None:
            cols_csv = ','.join([c.name for c in self.cols])
            values_csv = ','.join([str(v) for v in list(self.values[0])])
            if len(self.values[0]) > 1:
                values_csv = '(%s)' % values_csv
                cols_csv = '(%s)' % cols_csv
            return (self.table, cols_csv, values_csv, self.sticky)
        else:
            return (self.table, None, None, self.sticky)

    def _make_results_row_history(self, results_row):
        epk = results_row.table.effective_primary_key
        col_indexes = [results_row.table.cols.index(c) for c in epk]

        cols_csv = ','.join([c.name for c in epk])
        values = [results_row.row[i] for i in col_indexes]
        values_csv = ','.join([str(v) for v in values])
        return (results_row.table, cols_csv, values_csv, self.sticky)

    def _set_history(self, work_item, results_row):
        if work_item is None:
            self.history = [self._make_work_item_history()]
            return

        self.history = list(work_item.history)
        work_item_history = self._make_work_item_history()

        if results_row is not None:
            results_row_history = self._make_results_row_history(results_row)
            if self.history[-1] != results_row_history:
                self.history.append(results_row_history)

            if work_item_history != results_row_history:
                self.history.append(work_item_history)

    def print_history(self):
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
                print('*', end='')
        print()

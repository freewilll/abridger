import Queue
from collections import defaultdict


class WorkItem(object):
    def __init__(self, table, column, values):
        assert (column is None) == (values is None)

        self.table = table
        self.column = column

        if values is not None and not isinstance(values, list):
            values = [values]
        self.values = values

    def prune(self, existing_results):
        '''Remove values that are in existing_results'''

        if self.column is None:
            return

        if self.table not in existing_results:
            return

        table_results = existing_results[self.table]

        if (self.column,) not in table_results:
            return

        table_values = table_results[(self.column,)]

        new_values = []
        for value in self.values:
            if (value,) not in table_values:
                new_values.append(value)

        self.values = new_values

    def fetch_rows(self, dbconn):
        return dbconn.fetch_rows(self.table, self.column, self.values)


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

        for subject in extraction_model.subjects:
            for table in subject.tables:
                self.work_queue.put(WorkItem(table.table, table.column,
                                             table.values))

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

            rows = work_item.fetch_rows(self.dbconn)
            self.fetch_count += 1

            keys = set([table.primary_key])

            for row in rows:
                self.fetched_row_count += 1
                self.fetched_row_count_per_table[table] += 1
                values = self._lookup_row_values(table, row, keys)
                for key, values in zip(keys, values):
                    self.results[table][key][values] = row

        return self

    def flat_results(self):
        results = []
        for table in sorted(self.results, key=lambda r: r.name):
            primary_key = table.primary_key
            rows = self.results[table][primary_key]
            for values in sorted(rows.values()):
                results.append((table, values))
        return results

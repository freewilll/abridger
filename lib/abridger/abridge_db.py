from collections import defaultdict
from signal import signal, SIGPIPE, SIG_DFL
from time import time
import argparse
import math
import os
import sys

from abridger.extraction_model import ExtractionModel
from abridger.extractor import Extractor
from abridger.generator import Generator
import abridger.config_file_loader
import abridger.database


class DbOutputter(object):
    def __init__(self, url, verbosity):
        self.verbosity = verbosity

        if verbosity > 0:
            print('Connecting to', url)
        self.database = abridger.database.load(url)
        self.connection = self.database.connection
        self.cursor = self.connection.cursor()

    def insert_row(self, row):
        self.database.insert_rows([row], cursor=self.cursor)

    def update_row(self, row):
        self.database.update_rows([row], cursor=self.cursor)

    def begin(self):
        pass

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def finish(self):
        self.database.disconnect()


class SqlOutputter(object):
    def __init__(self, src_database, path, verbosity):
        self.src_db = src_database
        self.verbosity = verbosity
        self.path = path
        self.cursor = self.src_db.connection.cursor()

        if path == '-':  # pragma: no cover
            # Coverage isn't measured since this the test is executed in a
            # subprocess
            self.file = os.fdopen(sys.stdout.fileno(), 'wb')
        else:
            self.file = open(path, 'wb')

    def insert_row(self, row):
        stmt = self.src_db.make_insert_stmt(self.cursor, row)
        self.file.write(stmt)
        self.file.write(b"\n")

    def update_row(self, row):
        stmt = self.src_db.make_update_stmt(self.cursor, row)
        self.file.write(stmt)
        self.file.write(b";\n")

    def begin(self):
        for stmt in self.src_db.make_begin_stmts():
            self.file.write(stmt)
            self.file.write(b"\n")

    def commit(self):
        for stmt in self.src_db.make_commit_stmts():
            self.file.write(stmt)
            self.file.write(b"\n")

    def rollback(self):
        pass

    def finish(self):
        self.file.close()


def main(args):
    parser = argparse.ArgumentParser(
        description='Minimize a database')
    parser.add_argument(dest='config_path', metavar='CONFIG_PATH',
                        help="path to extraction config file")
    parser.add_argument(dest='src_url', metavar='SRC_URL',
                        help="source database url")
    parser.add_argument('-u', dest='dst_url', metavar='URL',
                        help="destination database url")
    parser.add_argument('-f', dest='dst_file', metavar='FILE',
                        help="destination database file. Use - for stdout")
    parser.add_argument('-e', '--explain', dest='explain', action='store_true',
                        default=False,
                        help='explain where rows are coming from')
    parser.add_argument('-q', '--quiet', dest='quiet', action='store_true',
                        default=False,
                        help="Don't output anything")
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true',
                        default=False,
                        help="Verbose output")

    # Ignore SIG_PIPE and don't throw exceptions on it
    signal(SIGPIPE, SIG_DFL)

    args = parser.parse_args(args)

    verbosity = 1
    if args.quiet:
        verbosity = 0
    if args.verbose:
        verbosity = 2

    if (args.dst_url is None) == (args.dst_file is None):
        print('Either -u or -f must be passed')
        exit(1)

    if verbosity > 0:
        print('Connecting to', args.src_url)
    src_database = abridger.database.load(args.src_url)

    if args.dst_url is not None:
        outputter = DbOutputter(args.dst_url, verbosity)
        if not isinstance(src_database, type(outputter.database)):
            print('src and dst databases must be of the same type')
            exit(1)
    else:
        if not src_database.CAN_GENERATE_SQL_STATEMENTS:
            print("SQL generation isn't available for this type of database")
            exit(1)
        outputter = SqlOutputter(src_database, args.dst_file, verbosity)

    if verbosity > 0:
        print('Querying...')
    extraction_model_data = abridger.config_file_loader.load(args.config_path)
    extraction_model = ExtractionModel.load(src_database.schema,
                                            extraction_model_data)
    extractor = Extractor(src_database, extraction_model, explain=args.explain,
                          verbosity=verbosity)
    extractor.launch()

    if args.explain:
        exit(0)

    generator = Generator(src_database.schema, extractor)
    generator.generate_statements()

    if args.dst_url is not None:
        # The src database isn't needed any more
        src_database.disconnect()

    total_table_insert_counts = defaultdict(int)
    total_table_update_counts = defaultdict(int)
    table_insert_counts = defaultdict(int)
    table_update_counts = defaultdict(int)
    total_insert_count = len(generator.insert_statements)
    total_update_count = len(generator.update_statements)
    total_count = total_insert_count + total_update_count

    start_time = time()

    try:
        for insert_statement in generator.insert_statements:
            (table, values) = insert_statement
            total_table_insert_counts[table] += 1

        for update_statement in generator.update_statements:
            table = update_statement[0]
            total_table_update_counts[table] += 1

        if verbosity > 0:
            insert_tables = set(total_table_insert_counts.keys())
            update_tables = set(total_table_insert_counts.keys())
            tables = insert_tables | update_tables
            print('Performing %d inserts and %d updates to %d tables...' % (
                total_insert_count, total_update_count, len(tables)))

        insert_count = 0
        count = 0
        outputter.begin()
        for insert_statement in generator.insert_statements:
            (table, values) = insert_statement
            table_insert_counts[table] += 1
            insert_count += 1
            count += 1
            if verbosity > 1:
                percentage = math.floor(1000 * (count / total_count)) / 10
                print("%5.1f%% Inserting (%6d/%6d) row (%6d/%6d) in %s" % (
                    percentage,
                    insert_count, total_insert_count,
                    table_insert_counts[table],
                    total_table_insert_counts[table],
                    table))
            outputter
            outputter.insert_row(insert_statement)

        update_count = 0
        for update_statement in generator.update_statements:
            table = update_statement[0]
            table_update_counts[table] += 1
            update_count += 1
            count += 1
            if verbosity > 1:
                percentage = math.floor(1000 * (count / total_count)) / 10
                print("%5.1f%% Updating  (%6d/%6d) row (%6d/%6d) in %s" % (
                    percentage,
                    update_count, total_update_count,
                    total_table_update_counts[table],
                    table_update_counts[table],
                    table))
            outputter.update_row(update_statement)

        outputter.commit()
    finally:
        try:
            outputter.rollback()
        except:
            pass

        src_database.disconnect()
        outputter.finish()

    if verbosity > 0:
        elapsed_time = time() - start_time
        print('Data loading completed in %0.1f seconds' % elapsed_time)

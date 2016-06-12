from collections import defaultdict
from signal import signal, SIGPIPE, SIG_DFL
from time import time
import argparse
import math

import abridger.database
from abridger.extraction_model import ExtractionModel
from abridger.extractor import Extractor
from abridger.generator import Generator
import abridger.config_file_loader


def main(args):
    parser = argparse.ArgumentParser(
        description='Minimize a database')
    parser.add_argument(dest='config_path', metavar='CONFIG_PATH',
                        help="path to extraction config file")
    parser.add_argument(dest='src_url', metavar='SRC_URL',
                        help="source database url")
    parser.add_argument(dest='dst_url', metavar='DST_URL',
                        help="destination database url")
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

    if verbosity > 0:
        print('Connecting to', args.src_url)
    src_database = abridger.database.load(args.src_url)

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
    src_database.disconnect()

    if verbosity > 0:
        print('Connecting to', args.dst_url)
    dst_database = abridger.database.load(args.dst_url)

    connection = dst_database.connection
    cur = connection.cursor()

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
            dst_database.insert_rows([insert_statement], cursor=cur)

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
            dst_database.update_rows([update_statement], cursor=cur)

        connection.commit()
    finally:
        try:
            connection.rollback()
        except Exception as e:
            print("Something went wrong while trying a rollback: %s" % str(e))

        dst_database.disconnect()

    if verbosity > 0:
        elapsed_time = time() - start_time
        print('Data loading completed in %0.1f seconds' % elapsed_time)

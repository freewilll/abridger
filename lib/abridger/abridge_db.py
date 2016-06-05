import argparse
from signal import signal, SIGPIPE, SIG_DFL
from collections import defaultdict

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

    # Ignore SIG_PIPE and don't throw exceptions on it
    signal(SIGPIPE, SIG_DFL)

    args = parser.parse_args(args)

    print('Connecting to', args.src_url)
    src_database = abridger.database.load(args.src_url)

    print('Querying...')
    extraction_model_data = abridger.config_file_loader.load(args.config_path)
    extraction_model = ExtractionModel.load(src_database.schema,
                                            extraction_model_data)
    extractor = Extractor(src_database, extraction_model, explain=args.explain)
    extractor.launch()

    if args.explain:
        exit(0)

    generator = Generator(src_database.schema, extractor)
    generator.generate_statements()
    src_database.disconnect()

    print('Connecting to', args.dst_url)
    dst_database = abridger.database.load(args.dst_url)

    connection = dst_database.connection

    table_row_counts = defaultdict(int)
    table_update_counts = defaultdict(int)
    cur = connection.cursor()
    try:
        total_insert_count = len(generator.insert_statements)
        insert_count = 0
        for insert_statement in generator.insert_statements:
            (table, values) = insert_statement
            table_row_counts[table] += 1
            insert_count += 1
            print("Inserting (%5d/%5d) row %5d into table %s" % (
                insert_count, total_insert_count,
                table_row_counts[table], table))
            dst_database.insert_rows([insert_statement], cursor=cur)

        total_update_count = len(generator.update_statements)
        update_count = 0
        for update_statement in generator.update_statements:
            table = update_statement[0]
            table_update_counts[table] += 1
            update_count += 1
            print("Updating (%5d/%5d) update %d on table %s" % (
                update_count, total_update_count,
                table_update_counts[table], table))
            dst_database.update_rows([update_statement], cursor=cur)

        connection.commit()
    finally:
        try:
            connection.rollback()
        except Exception as e:
            print("Something went wrong while trying a rollback: %s" % str(e))

        dst_database.disconnect()

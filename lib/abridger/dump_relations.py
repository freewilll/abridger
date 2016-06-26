from signal import signal, SIGPIPE, SIG_DFL
import argparse
import sys

from abridger.database import load


def main(args):
    parser = argparse.ArgumentParser(
        description='Dump relations from a database')
    parser.add_argument(dest='url', help="database url")

    # Ignore SIG_PIPE and don't throw exceptions on it
    signal(SIGPIPE, SIG_DFL)

    args = parser.parse_args(args)
    database = load(args.url)
    database.schema.dump_relations(sys.stdout)
    database.disconnect()

import argparse
from signal import signal, SIGPIPE, SIG_DFL
import sys
from minime.db_conn import load


def main(args):
    parser = argparse.ArgumentParser(
        description='Dump relations from a database')
    parser.add_argument(dest='url', help="database url")

    # Ignore SIG_PIPE and don't throw exceptions on it
    signal(SIGPIPE, SIG_DFL)

    args = parser.parse_args(args)
    dbconn = load(args.url)
    dbconn.schema.dump_relations(sys.stdout)
    dbconn.disconnect()

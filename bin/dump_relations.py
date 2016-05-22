#!/usr/bin/env python

import argparse
from signal import signal, SIGPIPE, SIG_DFL
import sys
from minime.db_conn import PostgresqlDbConn
from minime.schema import PostgresqlSchema


def main(args):
    parser = argparse.ArgumentParser(
        description='Dump relations from a database')
    parser.add_argument(dest='db_config_path', help="database config path")

    args = parser.parse_args(args)
    dbconn = PostgresqlDbConn.load(args.db_config_path)
    conn = dbconn.connect()
    schema = PostgresqlSchema.create_from_conn(conn)
    conn.close()

    # Ignore SIG_PIPE and don't throw exceptions on it
    signal(SIGPIPE, SIG_DFL)
    schema.dump_relations(sys.stdout)


if __name__ == '__main__':
    main(sys.argv[1:])

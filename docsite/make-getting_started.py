#!/usr/bin/env python

from jinja2 import Template
import os

import subprocess
from subprocess import Popen, PIPE

from docsite_utils import file_path, read_file


INPUT_SQL_PATH = 'test-input.sql'
SQLITE_DB_PATH = 'test-db.sqlite3'
OUTPUT_SQL_PATH = 'test-output.sql'


def cleanup():
    pass
    for path in (INPUT_SQL_PATH, SQLITE_DB_PATH, OUTPUT_SQL_PATH):
        if os.path.exists(path):
            os.remove(path)


def main():
    cleanup()

    p = Popen(['sqlite3', SQLITE_DB_PATH], stdout=PIPE, stdin=PIPE,
              stderr=PIPE)
    schema = read_file('examples-schema-relations.sql')
    out, err = p.communicate(input=bytes(schema, 'utf-8'))

    config_path = 'getting-started-config.yaml'

    contents_args = [
        'sqlite3', '-header', '-column', SQLITE_DB_PATH,
        'SELECT e.*, d.name as department_name '
        'FROM employees e '
        'join departments d on (e.department_id=d.id) ORDER by id;']
    sqlite_db_contents = subprocess.check_output(
        contents_args,  universal_newlines=True).strip()

    contents_args[4] = "'%s'" % contents_args[4]
    contents_cmdline = ' '.join(contents_args)

    abridge_db_args = ['../bin/abridge-db', config_path,
                       'sqlite:///%s' % SQLITE_DB_PATH, '-f', OUTPUT_SQL_PATH]
    abridge_db_output = subprocess.check_output(
        abridge_db_args,  universal_newlines=True).strip()

    abridge_db_args[0] = 'abridge-db'
    abridge_db_cmdline = ' '.join(abridge_db_args)

    config = read_file(config_path)

    sql_output = read_file(OUTPUT_SQL_PATH)

    template = Template(open(file_path('getting_started.rst.j2')).read())
    with open(file_path('getting_started.rst'), 'wt') as f:
        f.write(template.render(
            input_sql_path=INPUT_SQL_PATH,
            sqlite_db_path=SQLITE_DB_PATH,
            output_sql_path=OUTPUT_SQL_PATH,
            schema=schema.split("\n"),
            contents_cmdline=contents_cmdline,
            sqlite_db_contents=sqlite_db_contents.split("\n"),
            config_path=config_path,
            config=config.split("\n"),
            abridge_db_cmdline=abridge_db_cmdline,
            abridge_db_output=abridge_db_output.split("\n"),
            sql_output=sql_output.split("\n")))

    cleanup()

if __name__ == '__main__':
    main()

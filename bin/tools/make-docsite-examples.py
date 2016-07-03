#!/usr/bin/env python

from io import StringIO
from jinja2 import Template
from pprint import pprint
import contextlib
import os
import re
import six
import sys
import yaml

from abridger.database.sqlite import SqliteDatabase
from abridger.extraction_model import ExtractionModel
from abridger.extractor import Extractor
from abridger.generator import Generator
from abridger.schema.sqlite import SqliteSchema


@contextlib.contextmanager
def stdout_redirect(where):
    sys.stdout = where
    try:
        yield where
    finally:
        sys.stdout = sys.__stdout__


def file_path(filename):
    return os.path.abspath(os.path.join(
        os.path.dirname(__file__),
        os.path.pardir,
        os.path.pardir, 'docsite', filename))


def read_file(filename):
    return open(file_path(filename)).read()


def complete_statement(stmt, values):
    '''Whatever you do, don't use this function to generate SQL you'll
       actually use. This is for documentation purposes only and should
       not be used. The escaping functions are NOT complete.'''
    stmt = re.sub('\?', '%s', stmt)

    formatted_values = []
    for value in values:
        if value is None:
            value = 'NULL'
        elif isinstance(value, six.string_types):
            value = "'%s'" % re.sub("'", "''", value)
        formatted_values.append(value)
    return (stmt + ';') % tuple(formatted_values)


def process_example(toplevel_example):
    database = SqliteDatabase(path=':memory:')
    conn = database.connection

    test_schema = read_file(toplevel_example['schema'])
    conn.executescript(test_schema)
    schema = SqliteSchema.create_from_conn(conn)

    demos = []
    for example in toplevel_example['examples']:
        (title, description, config, expected_statements) = (
            example['title'], example['description'], example['config'],
            example['expected_statements'])

        extraction_model = ExtractionModel.load(schema, config)

        with stdout_redirect(StringIO()) as new_stdout:
            extractor = Extractor(database, extraction_model,
                                  explain=True).launch()
        new_stdout.flush()
        new_stdout.seek(0)
        output = new_stdout.read()
        output = output.rstrip().split('\n')

        generator = Generator(schema, extractor)
        generator.generate_statements()

        statements = []
        for insert_statement in generator.insert_statements:
            (stmt, values) = list(database.make_insert_statement(
                insert_statement))
            statements.append(complete_statement(stmt, values))

        if expected_statements != statements:
            print('There is a mismatch in expected statements.')
            print('Schema:%s\n' % test_schema)
            print('Config:')
            pprint(config)
            print('Statements:')
            for stmt in statements:
                print(stmt)
            print('Expected statements:')
            for stmt in expected_statements:
                print(stmt)
            exit(1)

        demo = {
            'title': title,
            'description': description,
            'config': yaml.dump(config).split("\n"),
            'output': output,
            'statements': statements,
        }
        demos.append(demo)
    return {
        'title': toplevel_example['title'],
        'description': toplevel_example['description'],
        'schema': test_schema.split('\n'),
        'demos': demos,
    }


def main():
    examples = yaml.load(read_file('examples.yaml'))
    toplevel_examples = []
    for example in examples:
        toplevel_examples.append(process_example(example))

    template = Template(open(file_path('examples.rst.j2')).read())
    with open(file_path('examples.rst'), 'wt') as f:
        f.write(template.render(toplevel_examples=toplevel_examples))


if __name__ == '__main__':
    main()

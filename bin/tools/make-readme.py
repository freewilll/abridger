#!/usr/bin/env python

from io import StringIO
from jinja2 import Template
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


def read_file(filename):
    path = os.path.abspath(os.path.join(
        os.path.dirname(__file__),
        os.path.pardir,
        os.path.pardir, 'doc', filename))
    return open(path).read()


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


def main():
    database = SqliteDatabase(path=':memory:')
    conn = database.connection
    test_schema = read_file('README-schema.sql')
    conn.executescript(test_schema)
    schema = SqliteSchema.create_from_conn(conn)

    demos = []
    tests = yaml.load(read_file('README-tests.yaml'))
    for test in tests:
        (title, description, config, after_thought) = (
            test['title'], test['description'], test['config'],
            test.get('after-thought'))

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

        demo = {
            'title': title,
            'description': description,
            'config': yaml.dump(config),
            'output': output,
            'statements': statements,
            'after_thought': after_thought,
        }
        demos.append(demo)

    template_path = os.path.abspath(os.path.join(
        os.path.dirname(__file__),
        os.path.pardir,
        os.path.pardir, 'doc', 'README.md.j2'))
    template = Template(open(template_path).read())

    readme_path = os.path.abspath(os.path.join(
        os.path.dirname(__file__),
        os.path.pardir,
        os.path.pardir, 'README.md'))
    with open(readme_path, 'wt') as f:
        f.write(template.render(test_schema=test_schema, demos=demos))


if __name__ == '__main__':
    main()

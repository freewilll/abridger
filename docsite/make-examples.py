#!/usr/bin/env python

from io import StringIO
from jinja2 import Template
from graphviz import Digraph
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
from docsite_utils import file_path, read_file


@contextlib.contextmanager
def stdout_redirect(where):
    sys.stdout = where
    try:
        yield where
    finally:
        sys.stdout = sys.__stdout__


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


def make_graph(schema, svg_path):
    g = Digraph('G', filename=svg_path, format='svg')
    for table in schema.tables:
        g.node(table.name)
        for fk in table.foreign_keys:
            style = 'solid' if fk.notnull else 'dashed'
            g.edge(fk.src_cols[0].table.name, fk.dst_cols[0].table.name,
                   style=style)
    g.render(cleanup=True)


def process_toplevel_example(toplevel_example):
    database = SqliteDatabase(path=':memory:')
    conn = database.connection

    schema_filename = toplevel_example['schema']
    svg_filename = schema_filename.replace('.sql', '')
    svg_path = file_path(os.path.join('_static', svg_filename))
    schema_lines = read_file(schema_filename)
    conn.executescript(schema_lines)
    schema = SqliteSchema.create_from_conn(conn)
    make_graph(schema, svg_path)

    examples = []
    for example in toplevel_example['examples']:
        (title, ref, description, config, expected_statements,
         short_description) = (
            example['title'], example.get('ref'), example['description'],
            example['config'], example['expected_statements'],
            example['short_description'])

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

        for update_statement in generator.update_statements:
            (stmt, values) = list(database.make_update_statement(
                update_statement))
            statements.append(complete_statement(stmt, values))

        if expected_statements != statements:
            print('There is a mismatch in expected statements.')
            print('Schema:%s\n' % schema)
            print('Config:')
            pprint(config)
            print('Statements:')
            for stmt in statements:
                print(stmt)
            print('Expected statements:')
            for stmt in expected_statements:
                print(stmt)
            exit(1)

        examples.append({
            'title': title,
            'ref': ref,
            'description': description,
            'sdesc': short_description,
            'config': yaml.dump(config).split("\n"),
            'output': output,
            'statements': statements,
        })

    doc_filename = toplevel_example['doc_filename']

    max_ref_len = max([len(e['ref']) for e in examples])
    max_sdesc_len = max([len(e['sdesc']) for e in examples])

    data = {
        'title': toplevel_example['title'],
        'description': toplevel_example.get('description'),
        'schema_svg': os.path.join('_static', svg_filename),
        'schema': schema_lines.split('\n'),
        'examples': examples,
        'max_ref_len': max_ref_len,
        'max_sdesc_len': max_sdesc_len,
    }

    # Write main rst file
    template = Template(open(file_path('examples-example.rst.j2')).read())
    with open(file_path('%s.rst' % doc_filename), 'wt') as f:
        f.write(template.render(**data))

    # Write table rst file
    template = Template(open(file_path('examples-table.rst.j2')).read())
    with open(file_path('%s_table.rst' % doc_filename), 'wt') as f:
        f.write(template.render(**data))

    return doc_filename


def main():
    examples = yaml.load(read_file('examples.yaml'))
    filenames = []
    for example in examples:
        filenames.append(process_toplevel_example(example))

    template = Template(open(file_path('examples.rst.j2')).read())
    with open(file_path('examples.rst'), 'wt') as f:
        f.write(template.render(filenames=filenames))


if __name__ == '__main__':
    main()

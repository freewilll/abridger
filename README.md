Abridger
========
Abridger is a tool which allows for extraction of a subset of a relational database using a yaml configuration file. Sqlite and postgresql databases are supported. Please see the [documentation](http://abridger.readthedocs.io/en/latest/overview/).

Installation
------------
    $ sudo pip install abridger

Example Configuration
---------------------
    - relations:
      - { defaults: everything}
    - subject:
      - tables:
        - {table: departments, column: name, values: Research}

Examples
--------
Extract data from a postgresql database and add it to another

    abridge-db config.yaml postgresql://user@localhost/test -u postgresql://user@localhost/abridged_test

Extract data from a postgresql database and write an sql file

    abridge-db config.yaml postgresql://user@localhost/test -f test-postgresql.sql

Extract data from a sqlite3 database and output SQL to stdout

    abridge-db config.yaml sqlite:///test-db.sqlite3 -q -f -

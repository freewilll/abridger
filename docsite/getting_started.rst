Getting Started
===============

Installation
------------
The code is hosted on `GitHub <https://github.com/freewilll/abridger>`_. Abridger should be installed with python's pip installer.

If you don’t have pip installed, run:

::

    $ sudo easy_install pip

Root installation
++++++++++++++++++
Installation using pip
::
    $ sudo pip install abridger

Install from github
::

    $ git clone https://github.com/freewilll/abridger
    $ cd abridger
    $ sudo python setup.py install


Or alternatively, you can do it in one step:
::

    $ sudo pip install git+https://github.com/freewilll/abridger



If you wish to use postgresql, install the psycopg2 package:

::

    $ sudo pip install psycopg2

Non-root installation
+++++++++++++++++++++

If you would rather not install it as root, you can use ``virtualenv`` to install a local copy

::

    $ virtualenv venv
    $ source venv/bin/activate
    $ pip install abridger

    If you want to use postgresql
    $ pip install psycopg2

Quick start
-----------
In the following example, a test sqlite3 database will be created with some tables and some data. An extraction is shown using all relations as a default.

Create a test database
++++++++++++++++++++++
Create a file called ``test-input.sql`` and put the following in it:
::

  CREATE TABLE departments (
      id INTEGER PRIMARY KEY,
      name TEXT
  );
  
  CREATE TABLE employees (
      id INTEGER PRIMARY KEY,
      name TEXT,
      department_id INTEGER NOT NULL REFERENCES departments
  );
  
  INSERT INTO departments (id, name) VALUES
      (1, 'Research'),
      (2, 'Accounting');
  
  INSERT INTO employees (id, name, department_id) VALUES
      (1, 'John', 1),
      (2, 'Jane', 1),
      (3, 'Janet', 2);
  

Load ``test-input.sql`` into an sqlite3 database called ``test-db.sqlite3``
::

  $ sqlite3 test-db.sqlite3 < test-input.sql

The contents of the test database
+++++++++++++++++++++++++++++++++
::

  $ sqlite3 -header -column test-db.sqlite3 'SELECT e.*, d.name as department_name FROM employees e join departments d on (e.department_id=d.id) ORDER by id;'

  id          name        department_id  department_name
  ----------  ----------  -------------  ---------------
  1           John        1              Research       
  2           Jane        1              Research       
  3           Janet       2              Accounting

Create an extraction config file
++++++++++++++++++++++++++++++++
In this example, we'll fetch the ``Research`` department, which will also fetch all employees in it.
Create a file called ``getting-started-config.yaml`` and put the following in it:
::


    - relations:
      - { defaults: everything}
    - subject:
      - tables:
        - {table: departments, column: name, values: Research}
  

Run abridger
+++++++++++++
::

  $ abridge-db getting-started-config.yaml sqlite:///test-db.sqlite3 -f test-output.sql

  Connecting to sqlite:///test-db.sqlite3
  Querying...
  Extraction completed: fetched rows=4, tables=2, queries=3, depth=2, duration=0.0 seconds
  Writing SQL for 3 inserts and 0 updates in 2 tables...
  Done

Results
+++++++
::

  $ cat test-output.sql

  BEGIN;
  INSERT INTO departments (id, name) VALUES(1, 'Research');
  INSERT INTO employees (id, name, department_id) VALUES(1, 'John', 1);
  INSERT INTO employees (id, name, department_id) VALUES(2, 'Jane', 1);
  COMMIT;
  

Running abridger
----------------
Usage: ``abridge-db [-h] [-u URL] [-f FILE] [-e] [-q] [-v] CONFIG_PATH SRC_URL``

positional arguments:

===========  ==============================
CONFIG_PATH  path to extraction config file
SRC_URL      source database url
===========  ==============================

optional arguments:

  -h, --help            show this help message and exit
  -u URL, --url URL     destination database url
  -f FILE, --file FILE  destination database file. Use - for stdout
  -e, --explain         explain where rows are coming from
  -q, --quiet           don't output anything
  -v, --verbose         verbose output

Unless ``--explain`` is being used, exactly one of ``--file`` and ``--url`` must be specified.
Use ``--file -`` to output the SQL results to stdout.

Note that using ``--explain`` is very inefficient since the extractor will do one
query for each row.


Examples
++++++++

Extract data from a postgresql database and add it to another
::

  abridge-db config.yaml postgresql://user@localhost/test -u postgresql://user@localhost/abridged_test

Extract data from a postgresql database and write an sql file
::

  abridge-db config.yaml postgresql://user@localhost/test -f test-postgresql.sql

Extract data from a sqlite3 database and output SQL to stdout
::

  abridge-db config.yaml sqlite:///test-db.sqlite3 -q -f -
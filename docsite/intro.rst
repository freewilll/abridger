Introduction
============


Installation
------------
The code is hosted on `GitHub <https://github.com/willangenent/abridger>`_. No python package has released, so you will need to run python's pip installer.

If you don’t have pip installed in your version of Python, install pip:

::

    $ sudo easy_install pip

Root installation
++++++++++++++++++

Install from github
::

    $ git clone https://github.com/willangenent/abridger
    $ cd abridger
    $ sudo python setup.py install


Or alternatively, you can do it in one step:
::

    $ sudo pip install git+https://github.com/willangenent/abridger



If you wish to use postgresql, install the psycopg2 package:

::

    $ sudo pip install psycopg2

Non-root installation
+++++++++++++++++++++

If you would rather not install it as root, you can use virtualenv to install a local copy

::

    $ virtualenv venv
    $ source venv/bin/activate
    $ pip install git+https://github.com/willangenent/abridger

    If you want to use postgresql
    $ pip install psycopg2

Quick start
-----------
In the following example, a test sqlite3 database will be created with some tables and some data. An extraction is shown using all relations as a default.

Create a test database
++++++++++++++++++++++
::

  $ sqlite3 test.sqlite3
  SQLite version 3.12.2 2016-04-18 17:30:31
  Enter ".help" for usage hints.
  sqlite> CREATE TABLE departments (
     ...>     id INTEGER PRIMARY KEY,
     ...>     name TEXT
     ...> );
  sqlite> CREATE TABLE employees (
     ...>     id INTEGER PRIMARY KEY,
     ...>     name TEXT,
     ...>     department_id INTEGER NOT NULL REFERENCES departments
     ...> );
  sqlite> INSERT INTO departments (id, name) VALUES
     ...>     (1, 'Research'),
     ...>     (2, 'Accounting');
  sqlite> INSERT INTO employees (id, name, department_id) VALUES
     ...>     (1, 'John', 1),
     ...>     (2, 'Jane', 1),
     ...>     (3, 'Janet', 2);
  sqlite> .quit

The contents of the test database
+++++++++++++++++++++++++++++++++
::

  $ sqlite3 test.sqlite3
  SQLite version 3.12.2 2016-04-18 17:30:31
  Enter ".help" for usage hints.
  sqlite> .mode column
  sqlite> .headers on
  sqlite> SELECT e.*, d.name as department_name FROM employees e join departments d on (e.department_id=d.id) ORDER by id;
  id          name        department_id  department_name
  ----------  ----------  -------------  ---------------
  1           John        1              Research
  2           Jane        1              Research
  3           Janet       2              Accounting
  sqlite> .quit

Create a config file
++++++++++++++++++++
In this example, we'll fetch the ``Research`` department, which will also fetch all employees in it.

::

  $ cat << END > config.yaml
  > - relations:
  >   - { defaults: everything}
  > - subject:
  >   - tables:
  >     - {table: departments, column: name, values: Research}
  > END

Run abridger
+++++++++++++
::

  $ abridge-db config.yaml sqlite:///test.sqlite3 -f test-abridger.sql
  Connecting to sqlite:///test.sqlite3
  Querying...
  Extraction completed: rows=1, tables=1, queries=1, depth=0, duration=0.0 seconds

Results
+++++++
::

  $ cat test-abridger.sql
  BEGIN;
  INSERT INTO departments (id, name) VALUES(1, 'Research');
  INSERT INTO employees (id, name, department_id) VALUES(1, 'John', 1);
  INSERT INTO employees (id, name, department_id) VALUES(2, 'Jane', 1);
  COMMIT;

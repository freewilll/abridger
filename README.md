Abridger
========
Abridger is a tool which allows for extraction of a subset of database data using a yaml configuration file. It currently supports sqlite and postgresql.

This readme file is just temporary, pending writing of some more substantial docs.

# Synopsis

```
usage: abridge-db [-h] [-u URL] [-f FILE] [-e] [-q] [-v] CONFIG_PATH SRC_URL

Minimize a database

positional arguments:
  CONFIG_PATH    path to extraction config file
  SRC_URL        source database url

optional arguments:
  -h, --help     show this help message and exit
  -u URL         destination database url
  -f FILE        destination database file. Use - for stdout
  -e, --explain  explain where rows are coming from
  -q, --quiet    Don't output anything
  -v, --verbose  Verbose output
```

Features
========
- Multiple subjects
- Compound foreign keys
- Nested includes
- Various defaults for relations
- Whitelisting and blacklisting relations using the `disabled` flag on relations
- SQL generation can go to a file or stdout
- SQL generation for postgresql
- Running of generates statements on destination databases
- Relations can be either global or only apply to a specific subject

Supported Databases
-------------------
- Sqlite
- Postgresql

Configuration
-------------
- Configuration files can be included using the `include` directive.

Extraction
----------
- One or more subjects can be defined, for example
  - An entire table
  - A table with certain rows matched to a column, e.g. `departments` with `id=1`
- Several relationships can be defined, for example
  - An outgoing foreign key
  - An incoming foreign key
  - Sticky relations: a sticky flag can be added to a relation. This can be used to limit which rows are fetched in large database schemas. Using the sticky flag ensures that only rows are fetched when the path from the table to the subject table consists entirely of sticky relations. This doesn't apply to not-null outgoing relationships, which are always fetched.
  - Use the `--explain` flag to get detailed output of why things are fetched.

Default Relationships
---------------------
| Default | Meaning |
| ------- | ------- |
| all-outgoing-not-null | This builtin relationship is always present and cannot be disabled |
| all-outgoing-nullable | This relationship is the default one. It ensures that all columns of rows matching the extraction criteria are fetched, leading to complete row fetches. |
| all-incoming | Include all tables that reference tables fetched when going through the extraction criteria |
| everything | Fetch everything that matches the extraction criteria, doing bidirectional matches |

Examples
========
Some example extractions follow, using toy database.

Database
--------
This toy database is used:
```
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
```

The data looks like this:
```
SELECT d.*, e.* FROM employees e JOIN departments d ON (d.id = e.department_id) ORDER by e.id, d.id
 id |    name    | id | name  | department_id
----+------------+----+-------+---------------
  1 | Research   |  1 | John  |             1
  1 | Research   |  2 | Jane  |             1
  2 | Accounting |  3 | Janet |             2
(3 rows)
```

Extraction with the default relations
-------------------------------------
By default, complete rows are fetched. This means that if a rule is added to fetch the department with name "Research" then rows referencing that department aren't fetched. This results in just one department being fetched and nothing else.

Config
```
- subject:
  - tables:
    - {table: departments, column: name, values: Research}
```

Results
```
BEGIN;
\set ON_ERROR_STOP
INSERT INTO departments (id, name) VALUES(1, 'Research');
COMMIT;
```

Extraction with a relation
--------------------------
This does an extraction with a relation from `employees` to `departments`. This will include both employees in the research department.

Config
```
- subject:
  - tables:
    - {table: departments, column: name, values: Research}
  - relations:
    - {table: employees, column: department_id}
```

Results
```
BEGIN;
\set ON_ERROR_STOP
INSERT INTO departments (id, name) VALUES(1, 'Research');
INSERT INTO employees (id, name, department_id) VALUES(1, 'John', 1);
INSERT INTO employees (id, name, department_id) VALUES(2, 'Jane', 1);
COMMIT;
```

Extraction with a relation
--------------------------
This does an extraction with the above relation, but with both departments. This ends up fetching all employees.

Config
```
- subject:
  - tables:
    - {table: departments, column: name, values: [Research, Accounting]}
  - relations:
    - {table: employees, column: department_id}
```

Results
```
BEGIN;
\set ON_ERROR_STOP
INSERT INTO departments (id, name) VALUES(1, 'Research');
INSERT INTO departments (id, name) VALUES(2, 'Accounting');
INSERT INTO employees (id, name, department_id) VALUES(1, 'John', 1);
INSERT INTO employees (id, name, department_id) VALUES(2, 'Jane', 1);
INSERT INTO employees (id, name, department_id) VALUES(3, 'Janet', 2);
COMMIT;
```

Everything
----------
This includes all relations. This leads to all employees in the research department being fetched since:
- John belongs to the research department
- All employees in the research department are fetched, which pulls in Jane

Config
```
- relations:
    - { defaults: everything}
- subject:
  - tables:
    - {table: employees, column: name, values: John}
```

Results
```
BEGIN;
\set ON_ERROR_STOP
INSERT INTO departments (id, name) VALUES(1, 'Research');
INSERT INTO employees (id, name, department_id) VALUES(1, 'John', 1);
INSERT INTO employees (id, name, department_id) VALUES(2, 'Jane', 1);
COMMIT;
```

Everything with --explain
-------------------------
This runs the above extraction, but with the `--explain` option.

Config
```
- relations:
    - { defaults: everything}
- subject:
  - tables:
    - {table: employees, column: name, values: John}
```

Results
```
employees.name=John*
employees.name=John* -> employees.id=1 -> departments.id=1
employees.name=John* -> employees.id=1 -> departments.id=1 -> employees.department_id=1
Extraction completed: rows=4, tables=2, queries=3, depth=2, duration=0.0 seconds
```

The asterisks indicate stickyness. In this case stickiness isn't used, so only the subject has a *.
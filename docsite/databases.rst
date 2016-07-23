Databases
=========

Two databases are supported: sqlite and postgresql. The database URLs follow the django url convention. The following features are supported in both databases:

- Schema parsing of tables, columns, primary keys, foreign keys and unique indexes
- Compound primary and foreign keys
- SQL generation

Sqlite
++++++

Use the ``sqlite:///`` prefix in front of the path name.

For a relative path use e.g.

::

    sqlite:///test-db.sqlite3

For an absolute path use e.g.

::

    sqlite:////var/lib/databases/test-db.sqlite3

Postgresql
++++++++++

A full postgresql URL is something like:
::

    postgresql://user:password@host:port/dbname

``host`` and ``dbname`` are  required and ``password`` and ``port`` are optional. This is e.g. a valid url
::

    postgresql://test_user@localhost/test_database

The generated SQL always starts with a ``BEGIN``, ends with a ``COMMIT`` and has an extra ``\set ON_ERROR_STOP`` for convenience, so that a full SQL result looks something like:
::

    BEGIN;
    \set ON_ERROR_STOP
    INSERT INTO ...
    COMMIT;

SQL Generation
==============
SQL generation uses the fetched and processed rows from the extraction step and converts them into SQL ``INSERT`` and ``UPDATE`` statements. The insert statements are done in order so that not null foreign keys are respected.

.. _not_null_columns:

Not Null Columns
+++++++++++++++++
If an insert statement cannot be done without violating foreign key constraints due to nullable foreign keys, then it is split into an insert and update statement. If nullable foreign key cannot be made null due to for example a ``CHECK`` constraint, then a simple rule can be added which tells the SQL generator to treat that column as if it were not null.

Examples:

.. include:: examples_not_null_columns_table.rst

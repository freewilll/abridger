Overview
========
The objective is to create a new database with a referentially intact subset of data from another database. The schema of the database is identical, but the data is different. Creating a new database with the same schema as an old database is easy, however copying just some of the data can be tricky due to the database's relational nature.

It all comes down to defining rules on what to extract. If the rules are too strict, then not enough data is copied. If the rules are not strict enough, too much data is copied. Furthermore, for highly complex databases, it can become quite a task to define the rules and combine them in a sensible way.

The rules are defined in an :doc:`extraction_model` which is configured in one or more yaml files.

Concepts
++++++++

  Extraction model
    A collection of rules describing what to extract.
  Subject
    An extraction model has one or more subjects. A subject is a collection of tables and relations. See :ref:`subjects` for more information.
  Table
    A subject has one or more tables. A table can either be an extraction of all rows in the table, or filtered rows using a column and list of values. A table can also be set as top-level and be applied to all subjects. See :ref:`tables` for more information.
  Relation
    A relation is a reference to a database foreign key. A relation is either `outgoing` or `incoming` from the perspective of a subject. Relations can be `disabled` or made `sticky`. See :ref:`relations` for more information.
  Default relations
    By default, any row found in a table in the extraction model is fetched in its entirety. This will pull in rows required to satisfy any foreign key constraints on the row's table. Rows in other tables referencing the source table aren't fetched by default. These defaults can be overridden. See :ref:`defaults` for more information.
  Not null columns
    When populating the destination database or generating SQL, nullable columns can be treated as not null so that they are included in the insert statements. This is useful if check constraints are used. See :ref:`not_null_columns` for more information.


Extraction Model
================

.. _subjects:

Subjects
--------
An extraction model consists of one more more :ref:`subjects <subjects>`. Each subject has its own set of tables and :ref:`relations <relations>`. Relations can however also be top-level, which allows setting of defaults that can be overridden by subjects. By :ref:`default <defaults>`, any row found in a table in the extraction model is fetched in its entirety.

.. _tables:

Tables
--------
A table on a subject consists of the following:

  table
    The name of the table
  column
   The name of the column to extract values out of, when used together with ``values``
  values
    A single number of string *or* an array of number or strings

Examples:


.. include:: examples_subjects_table.rst

.. _relations:

Relations
---------
A relation enables or disables the processing of foreign keys in the database schema. A relationship is ``incoming`` or ``outgoing`` as seen from the perspective of a table in the extraction. All ``outgoing`` not null foreign keys *must* be processed since otherwise a corresponding row could not get inserted into the database.

A relation can be applied globally or to a subject. A global relation is always processed. Relations specified under a subject are only executed when a row is found when fetching the subject's data.

  defaults
      Add all relations from a couple of selected types. See :ref:`defaults <defaults>` for more details.
  table
      A foreign key constraint is identified by specifying a ``table`` and ``column`` in a relation. The first foreign key relationship to match the table and column is used.
  column
      Must be specified when using ``table`` to identify a foreign key.
  type
      One of ``incoming`` or ``outgoing``, with ``incoming`` the default. This identifies the direction of a relationship from the perspective of an encountered subject row.
  name
      Optional and purely for informational purposes.
  disabled
      Foreign key relations can be disabled. This is useful in the blacklisting approach where ``everything`` defaults are used and then individual relations disabled.
  sticky
      Sticky relations can be used to keep track of which rows are directly connected to the subject. See :ref:`sticky relations <sticky_relations>` for more details.

Compound keys are also supported, see e.g. :ref:`examples_compound_foreign_keys`

A relationship us uniquely identified by its ``table``, ``column``, ``type`` and ``name``. Identical relationships are processed in order and merged according to the following rules:

- If any of one or more identical relations is disabled, then the relationship is disabled.
- If any of one or more identical relations is sticky, then the relationship is sticky.

Examples:

.. include:: examples_relations_table.rst

.. _defaults:

Defaults
--------
Default relations can be set by using the ``relations`` ``default`` key. There are four default settings that can be combined in an additive way:

===================== =============== ===============================================
Setting               Default         Meaning
--------------------- --------------- -----------------------------------------------
all-outgoing-not-null yes             Always satisfy not null foreign key constraints
all-outgoing-nullable yes             Ensures that complete rows are fetched
all-incoming          no              Processing incoming foreign keys
everything            no              All of the above
===================== =============== ===============================================

If no defaults are specified, a single relation of type ``all-outgoing-nullable`` is used. The ``all-outgoing-not-null`` default is always present. The combination of these two ensures that whenever a row is encountered, all outgoing foreign keys are processed. This will cause those referenced table rows to be included and the value of the foreign key to be set in the original row.

This is the default setting:
::

    - relations:
      - {defaults: all-outgoing-not-null}
      - {defaults: all-outgoing-nullable}


To add all incoming relations to the default, use:
::

     - relations:
      - {defaults: everything}

Since ``all-outgoing-not-null`` is always included implicitly, the above is equivalent to:
::

    - relations:
      - {defaults: all-outgoing-nullable}
      - {defaults: all-incoming}

Use this to disable all relations except the minimal required ``all-outgoing-not-null``:
::

    - relations:
      - {defaults: all-outgoing-not-null}

Setting default relations are useful when using the blacklisting approach. See :ref:`example_relations_disabled_incoming` and :ref:`example_relations_disabled_outgoing`.


Examples:

.. include:: examples_defaults_table.rst

Includes
--------
Yaml files can be included in each other using the ``include`` directive. For example having this in a top level file:
::

    - include basic-tables.yaml
    - subject:
      - tables:
        - {table: departments}

and this in another file called ``basic-tables.yaml``

::

    - subject:
      - tables:
        - {table: building_types}
        - {table: something_essential}

will lead to the config effectively becoming:

::

    - subject:
      - tables:
        - {table: building_types}
        - {table: something_essential}
    - subject:
      - tables:
        - {table: departments}

.. _sticky_relations:

Sticky relations
----------------
What can quickly happen when doing an extraction in a complicated database schema is an explosion of data. In many of these cases, just enabling a foreign key relationship can pull in lots of unwanted data. An easy solution to prevent this is to make use of the ``sticky`` relations. When this flag is set on a relation, then the relation is *only* processed if there is a direct graph of sticky relations back to a subject. The rules of transmitting stickiness are:

- All rows in the initial subject's table fetch are sticky
- Non-sticky relations are always processed, however any potential stickiness is lost. This is the default behavior.
- A sticky relationship is only processed if the row is sticky
- Stickiness is only transmitted if a) the row is sticky and b) the relationship is sticky

This behavior can be summarized in a table:

================== =================== ========================= ====================
Fetched row sticky Relationship sticky Relationship is processed Processed row sticky
------------------ ------------------- ------------------------- --------------------
No                 No                  Yes                       No
Yes                No                  Yes                       No
No                 Yes                 No                        *-*
Yes                Yes                 Yes                       Yes
================== =================== ========================= ====================

See :ref:`examples_sticky_relations` for an example.

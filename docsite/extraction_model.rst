Extraction Model
================

An extraction model consists of one more more :ref:`subjects <subjects>`. Each subject has its own set of tables and :ref:`relations <relations>`. Relations can however also be top-level, which allows setting of defaults that can be overridden by subjects. By :ref:`default <defaults>`, any row found in a table in the extraction model is fetched in its entirety.

.. _subjects:

Subjects
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
A relation enables or disables the processing of a foreign key in the database schema. A relationship is ``incoming`` or ``outgoing`` as seen from the perspective of a table in the extraction. All ``outgoing`` not null foreign keys *must* be processed since otherwise a corresponding row could not get inserted into the database.

A relation can be applied globally or to a subject. A global relation is always processed. Relations specified under a subject are only executed when a row is found when fetching the subject's data.

Examples:

.. include:: examples_relations_table.rst

.. _defaults:

Defaults
--------
Defaults can be set for what relations should be processed. By default,

If no defaults are specified, a single relation of type ``all-outgoing-nullable`` is used. Otherwise only the set of specific relation defaults are used.

This is the default setting of just enabling ``all-outgoing-nullable`` relations:
::

    - relations:
      - {defaults: all-outgoing-nullable}


This is equivalent to the ``everything`` default:
::

    - relations:
      - {defaults: all-outgoing-nullable}
      - {defaults: all-incoming}

Use this to disable everything except the required ``all-outgoing-not-null`` relation:
::

    - relations:
      - {defaults: all-outgoing-not-null}


Setting default relations are useful when using the :ref:`blacklisting <example_relations_disabled1>` approach.

===================== ================== ==================================================================
Setting               Enabled by default Meaning
--------------------- ------------------ ------------------------------------------------------------------
all-outgoing-not-null yes                Always enabled
all-outgoing-nullable yes                This ensures that complete rows are fetched
all-incoming          no                 This enables processing of incoming foreign keys
everything            no                 All of the above
===================== ================== ==================================================================


Examples:

.. include:: examples_defaults_table.rst

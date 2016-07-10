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

======================================= =======================================================================
Example                                 Description
--------------------------------------- -----------------------------------------------------------------------
:ref:`example_subjects_all_departments` A table entry with just a table name will fetch all rows for that table
:ref:`example_subjects_one_department`  A table entry with a single column/value will fetch one row
:ref:`example_subjects_two_departments` A table entry with multiple column/value will fetch multiple rows
======================================= =======================================================================

.. _relations:

Relations
---------
A relation enables or disables the processing of a foreign key in the database schema. A relationship is ``incoming`` or ``outgoing`` as seen from the perspective of a table in the extraction. All ``outgoing`` not null foreign keys *must* be processed since otherwise a corresponding row could not get inserted into the database.

A relation can be applied globally or to a subject. A global relation is always processed. Relations specified under a subject are only executed when a row is found when fetching the subject's data.

Examples:

========================================= =======================================================================
Example                                   Description
----------------------------------------- -----------------------------------------------------------------------
:ref:`example_relations_for_a_department` Default relations for a department
:ref:`example_relations_two_departments1` Default relations for two departments
:ref:`example_relations_two_departments2` An alternative default relations for two departments
:ref:`example_relations_two_departments3` Another alternative default relations for two departments
:ref:`example_relations_an_employee`      All relations
:ref:`example_relations_disabled1`        Blacklisting approach using disabled relations
========================================= =======================================================================

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

- :ref:`example_relations_all_outgoing_not_null`
- :ref:`example_relations_all_outgoing_nullable`
- :ref:`example_relations_all_incoming`
- :ref:`example_relations_all_incoming_and_all_outgoing_nullable`

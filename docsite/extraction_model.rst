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
A relation enables or disables the processing of foreign keys in the database schema. A relationship is ``incoming`` or ``outgoing`` as seen from the perspective of a table in the extraction. All ``outgoing`` not null foreign keys *must* be processed since otherwise a corresponding row could not get inserted into the database.

A relation can be applied globally or to a subject. A global relation is always processed. Relations specified under a subject are only executed when a row is found when fetching the subject's data.




  defaults
      Add all relations from a couple of selected types. See :ref:`defaults <defaults>` for more details.
  table
      A foreign key constraint is identified by specifing a ``table`` and ``column`` in a relation. The first foreign key relationship to match the table and column is used.
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


Setting default relations are useful when using the blacklisting approach. See :ref:`example_relations_disabled_incoming` and :ref:`example_relations_disabled_outgoing`.

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

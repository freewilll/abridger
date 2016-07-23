Extraction
==========

Extraction is done by keeping track of a queue of `work items` for each subject. The work items queue starts with the subjects and grows as rows in new tables are added by processing relations. The procedure is complete as soon as the queue is empty.

The procedure
-------------
The procedure is as follows

#. Add all subject tables/columns/values to the work item queue
#. Fetch an item from the queue
#. Skip the item if the table, column, subject and values have already been processed
#. Query for the table/column/values
#. For each row, process the subject's relationships
#. For each row, null any nullable foreign keys that didn't have their relationship processed
#. If a row has been seen in a previous iteration, merge in any not null values for columns that may have been made null. This ensures that if a row is seen twice and the second time is processed with more relationships, then the final row contains foreign key values for the new relationships.
#. Repeat step 2 if the queue isn't empty

Identical rows
--------------
Identical rows for a table are processed by using an `effective primary key`. This is:

#. The table's primary key, if available
#. Otherwise, if available, the first discovered most restrictive unique index
#. Otherwise, the entire row is treated as unique, but duplicate rows are allowed. Duplicates are counted and the row will be inserted the correct amount of times.

Using explain
-------------
When running from the command line, use the ``--explain`` option to get a detailed view of the extraction procedure. The output of the script will have details about each query and processed relationships.

When running with explain, a query is done for each individual row instead of batching them using SQL ``IN`` statements. This makes the procedure much slower, but this is needed to be able to identify exactly where a row is coming from. The :doc:`examples` all contain the output of ``--explain``.

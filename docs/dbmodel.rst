==============
Database model
==============

When on-the-fly Relion processing is performed at eBIC, results are written to the ISPyB database 
as they become available. The tables in ISPyB designed to hold SPA information have various 
relationships between them, which requires that certain records be inserted before others. In an 
environment where database operations have been separated from the data collection and the 
Relion project itself in order to improve the stability and recoverability of the pipeline the order 
in which these insertions are made is not guaranteed. This means the service performing the database 
operations will not be able to determine the necessary relationships between entries. In order to 
coordinate with the database we employ a model of the database table structure within the code that 
follows the Relion project and therefore, in principle, has all the required information about what 
the entry relationships should be. 

An ``sqlalchemy`` ISPyB ORM is available as part of the ``ispyb`` `API package <https://github.com/DiamondLightSource/ispyb-api>`_ 
and we make use of this to keep up to date with the available ISPyB columns.

------
Tables
------

The ``relion.dbmodel.modeltables.Table`` class allows rows to be added to a table with defined 
columns while maintaining the uniqueness of entries under set conditions in order to avoid duplication. 
On initialisation of a ``Table`` instance a single primary key must be specified . When a row is added 
(with the ``add_row`` method) if the value of the primary key is not specified for the new row (i.e. 
it is left as ``None``) then the primary key is auto-incremented. On initialisation there is also the 
option to specify "unique" columns. The table will not insert a new row if a row already exists with the 
same entries for the "unique" columns (even if the value of the primary key is not provided); instead it 
will perform an update of the existing row. 

An example of a ``Table`` definition for the ``ParticleClassification`` table is:

.. code-block:: python 

    class ParticleClassificationTable(Table):
        def __init__(self):
            columns, prim_key = parse_sqlalchemy_table(sqlalchemy.ParticleClassification)
            columns.append("job_string")
            super().__init__(columns, prim_key, unique=["job_string", "class_number"])

(The ISPyB column names are changed from camel to snake case here.) The ``"job_string"`` column which is 
added here, extraneously to the columns that exist in ISPyB, is useful for keeping track of which results 
belong to which Relion job. The ``unique`` key word argument is used to specify that only one row can exist 
for a given combination of ``job_string`` and ``class_number``, i.e. for each classification job there can 
only be one record for each class number.

In addition, a column can be set to auto-increment with the ``counters`` key word in ``Table`` initialisation. 
This is used, for example, in the ``MotionCorrection`` table which contains an ``image_number`` column which 
needs to be a unique integer for each micrograph. An appendable column may be specified with the ``append`` 
keyword. For these columns values are appended to a list rather than updated.

------
DBNode
------

A ``DBNode`` is a ``Node`` implemented with a specific call. Calling a ``DBNode`` will perform an insert into 
a ``Table`` associated with the node after doing a few checks. If ``"check_for`` is present in the node's 
``environment`` then the ``Table`` ``environment["foreign_table"]`` will be searched for 
``environment[environment["check_for"]]`` and the primary key associated with the found value will be inserted 
into the node's ``Table`` in the column ``environment["table_key"]``. This allows entries between different tables 
to be linked based on if they share some common value. For example, in the ISPyB database schema the ``CTF`` table 
contains a ``motionCorrectionId`` column which should point to the ``MotionCorrection`` entry for the matching 
micrograph. If both ``DBNode``s associated with the ``MotionCorrection`` and ``CTF`` tables have a ``micrograph_full_path`` 
in their ``environment``, then specifying ``environment["check_for"] = micrograph_full_path``, ``environment["table_key"] = motion_correction_id`` 
and ``environment["foreign_table"]`` is the ``MotionCorrection`` table in the ``CTF`` node will ensure that ``motion_correction_id`` 
is equal to the primary key of the ``MotionCorrection`` row that has a matching ``micrograph_full_path``.

The ``DBNode`` call will return a message designed to be sent to a Zocalo service that will do the insertions into the 
actual ISPyB database. A graph of ``DBNode``s can be set up which upon calling provides a series of messages to be sent 
to the Zocalo service with the correct connections made between the entries in various different related tables. 

-------
DBGraph
-------

A ``DBGraph`` adds some basic functionality to a ``Graph`` to keep track of the last time that tables were updated, 
and from which nodes that data came from. This allows checks on whether or not a data producing job has been run 
since the last table update. Calling a ``DBGraph`` will return a collapsed set of messages from all the ``DBNodes`` 
within the graph.

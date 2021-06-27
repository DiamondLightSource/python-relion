==============
Database model
==============

When on-the-fly Relion processing is performed at eBIC results are written to the ISPyB database 
as they become available. The tables in ISPyB designed to hold SPA information have various 
relationships between them which requires that certain records be inserted before others. In an 
environment in which database operations have been separated from the data collection from the 
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

An exaple of a ``Table`` definition for the ``ParticleClassification`` table is:

.. code_block:: python 

    class ParticleClassificationTable(Table):
        def __init__(self):
            columns = [
                to_snake_case(c)
                for c in ispyb.sqlalchemy.ParticleClassification.__table__.columns.keys()
            ]
            columns.append("job_string")
            prim_key = get_prim_key(ispyb.sqlalchemy.ParticleClassification)
            super().__init__(columns, prim_key, unique=["job_string", "class_number"])

The ``"job_string"`` column which is added here, extraneously to the columns that exist in ISPyB, 
is useful for keeping track of which results belong to which Relion job.
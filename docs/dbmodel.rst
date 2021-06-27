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
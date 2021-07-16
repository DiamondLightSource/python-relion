===============================
Following on-the-fly processing
===============================

Following Relion processing on-the-fly as data is being collected is useful during data taking sessions 
that span days. At `eBIC <https://www.diamond.ac.uk/Instruments/Biological-Cryo-Imaging/eBIC.html>`_ 
key results from the Relion processing pipeline are stored in the `ISPyB database <https://ispyb.github.io/ISPyB/>`_ 
as soon as they become available. Systems are then needed to not only collect data from a Relion project but to 
maintain records of what data has been sent to the database and the various kinds of relationships between 
the database tables and how they correspond to the structure of the Relion pipeline. 

Relion itself keeps track of individual job's inputs and outputs in the form of a graph, the nodes and edges of 
which are recorded in the ``default_pipeline.star`` file. It is therefore natural to mimic this structure when extracting 
results from a project.

The ``Project`` class can be given the keyword argument ``database`` on initialisation which defaults to ``"ISPyB"``. 
This selects a database model from ``relion.dbmodel`` (currently the ISPyB EM schema is the only implemented database 
model). These implementations take the form of a dictionary where the keys are names of the various job types in a 
Relion pipeline and the values are ``DBNode`` s that will receive the data collected from that job type. These nodes 
should be linked together where appropriate (i.e. where there are relationships between the tables).
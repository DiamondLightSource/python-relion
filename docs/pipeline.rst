===============================
Launching on-the-fly processing
===============================

Pre-prescribed on-the-fly processing pipelines are offered through the 
``relipy.run_pipeline`` command line utility. For RELION 3 this is a 
version of the ``relion_it.py`` script packaged with RELION 3.1 
modified to allow the use of `crYOLO <https://cryolo.readthedocs.io/en/stable/>`_ 
for particle picking and the running of various 
`IceBreaker <https://github.com/DiamondLightSource/python-icebreaker>`_ jobs for 
the estimation of ice thickness within micrographs. For RELION 4 the logic 
of this pipeline has been rewritten using the 
`CCP-EM pipeliner <https://ccpem-pipeliner.readthedocs.io/en/latest/>`_. Below 
are breif instructions for the condiguration and running of these pipelines.

--------
RELION 3
--------

An options file is required to run the pipeline. A default options file called 
``relion_it_options.py`` can be generated with the command 

``relipy.print_options``

The parameters in this file should be edited to suit both the data collection 
parameters and system setup. This file should be provided to the ``relipy.run_pipeline`` 
command to start processing:

``relipy.run_pipeline -f <path to relion_it_options.py> -m <path to data directory> -d <path to project directory>``

The data directory is the top level directory containing the movies or micrographs 
to be imported. The project directory is the RELION project directory. A symlink will 
be created from a ``Movies`` directory in the project directory to the specified data 
directory. 

Processing can be stopped by removing the ``*_RUNNING`` files written to the project directory.

--------
RELION 4
--------

The general setup is the same for the RELION 4 pipeline. The flag ``--version 4`` should be added 
to the run command. By default 2D classification is done using the EM algorithm but can be switched 
to VDAM by specifying ``do_class2d_vdam`` to be ``True`` in the ``relion_it_options.py`` file.

The Relion 4.0 pipeline will stop automatically after all movies have been processed and no new movies 
have appeared within a timeout period (this is set to two days be default and can be changed by 
specifying the timeout in seconds with the ``--timeout`` flag for ``relipy.run_pipeline``). Otherwise 
the pipeline can be stopped by creating a file called ``stop.stop`` int he project directory.

To restart a stopped processing pipeline (only available for RELION 4):

``relipy.run_pipeline --version 4 -f <path to relion_it_options.py> -m <path to data directory> -d <path to project directory> --continue``

It is difficult to guarantee that this will always work as intended.

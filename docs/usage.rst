=====
Usage
=====

To access a Relion project folder you first need to create a ``relion.Project`` object (c.f. :doc:`api` for more information):

.. code-block:: python

     import relion
     proj = relion.Project("/path/to/relion/project/directory")
     proj = relion.Project(pathlib.Path("/project/directory"))  # path objects are supported


The directory structure inside a Relion directory is built up of stages and jobs.
Each stage folder will contain one or more job folders.
The job folder(s) contain files related to the stage, including the ``*.star`` files from which values can be read::

    project_root
    │
    ├── MotionCorr
    │   └── job002
    │       └── corrected_micrographs.star
    │       └── ...
    ├── CTFFind
    │   └── job003
    │       └── micrographs_ctf.star
    │       └── ...
    ├── Class2D
    │   ├── job008
    │   │   └── run_it025_data.star
    │   │   └── run_it025_model.star
    │   │   └── ...
    │   └── job013
    │       └── run_it_025_data.star
    │       └── run_it_025_model.star
    │       └── ...
    └── Class3D
        └── job016
            └── run_it_025_data.star
            └── run_it_025_model.star
            └── ...

The desired EM values are extracted from \*.star files.
For example, a snippet from ``MotionCorr/job002/corrected_micrographs.star`` is shown below::

    ...
    loop_
    _rlnCtfPowerSpectrum #1
    _rlnMicrographName #2
    _rlnMicrographMetadata #3
    _rlnOpticsGroup #4
    _rlnAccumMotionTotal #5
    _rlnAccumMotionEarly #6
    _rlnAccumMotionLate #7
    MotionCorr/job002/Movies/20170629_00021_frameImage_PS.mrc MotionCorr/job002/Movies/20170629_00021_frameImage.mrc MotionCorr/job002/Movies/20170629_00021_frameImage.star            1    16.420495     2.506308    13.914187
    MotionCorr/job002/Movies/20170629_00022_frameImage_PS.mrc MotionCorr/job002/Movies/20170629_00022_frameImage.mrc MotionCorr/job002/Movies/20170629_00022_frameImage.star            1    19.551677     2.478968    17.072709
    MotionCorr/job002/Movies/20170629_00023_frameImage_PS.mrc MotionCorr/job002/Movies/20170629_00023_frameImage.mrc MotionCorr/job002/Movies/20170629_00023_frameImage.star            1    17.547827     1.941103    15.606724
    MotionCorr/job002/Movies/20170629_00024_frameImage_PS.mrc MotionCorr/job002/Movies/20170629_00024_frameImage.mrc MotionCorr/job002/Movies/20170629_00024_frameImage.star            1    18.100817     1.722567    16.378250
    ...

To access the ``_rlnAccumMotionTotal`` column in this file you can use:

.. code-block:: python

    >>> [micrograph.total_motion for micrograph in proj.motioncorrection["job002"]]
    ['16.420495', '19.551677', '17.547827', '18.100817', ...]

Stages are dictionary-like objects, so can discover the list of all known jobs by:

.. code-block:: python

    >>> list(proj.class2D)
    ['job008', 'job013']

and use the other standard dictionary accessors (``.values()``, ``.keys()``, ``.items()``), too.
You can also convert the stages into normal dictionaries:

.. code-block:: python

    >>> dict(p.ctffind)
    {'job003': [CTFMicrograph(...), ...]}

For a list of supported stages and a list of supported values per stage please have a look at the :doc:`api` page.

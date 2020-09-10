=====
Usage
=====

To access a Relion project folder you first need to create a :ref:`relion-project-object` ``relion.Project`` object::

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


The desired EM values are extracted from \*.star files. A snippet from MotionCorr/job002/corrected_micrographs.star is shown below::

    # version 30001

    data_micrographs

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


To view a specific value from a given Relion stage, use::

    proj.<stage title>.<job number>.<value name>

This will return a list of all values found for the given value, job and stage.
For example, from the Motion Correction data shown above::

    proj.motioncorrection."job002".total_motion

would return::

 [16.420495, 19.551677, 17.547827, 18.100817]

To extract a dictionary of all the Electron Microscopy (EM) data from a given Relion stage, use::

    dict(proj.<stage title>)
    eg. dict(proj.ctffind)


The current available stages and values are as follows::


    motioncorrection:
        micrograph_name
        total_motion
        early_motion
        late_motion
    ctffind:
        micrograph_name
        astigmatism
        defocus_u
        defocus_v
        defocus_angle
        max_resolution
        fig_of_merit
    class2d:
        particle_sum
        reference_image
        class_distribution
        accuracy_rotations
        accuracy_translations_angst
        estimated_resolution
        overall_fourier_completeness
    class3d:
        particle_sum
        reference_image
        class_distribution
        accuracy_rotations
        accuracy_translations_angst
        estimated_resolution
        overall_fourier_completeness



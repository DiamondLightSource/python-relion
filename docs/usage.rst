=====
Usage
=====

To access a Relion project folder you first need to create a relion.Project object::

     import relion
     proj = relion.Project("/path/to/relion/project/directory")
     proj = relion.Project(pathlib.Path("/project/directory"))  # path objects are supported


To extract a dictionary of all the Electron Microscopy (EM) data from a given Relion stage, use::

    dict(proj.<stage title>)
    eg. dict(proj.ctffind)

To view one specific value use::

    proj.<stage title>.<value name>
    eg. proj.motioncorrection.total_motion

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



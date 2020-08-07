=====
Usage
=====

To access a Relion project folder you first need to create a relion.Project object::

     import relion
     proj = relion.Project("/path/to/relion/project/directory")
     proj = relion.Project(pathlib.Path("/project/directory"))  # path objects are supported


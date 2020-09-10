===
API
===

Project object
--------------

.. autoclass:: relion.Project
    :members:

The individual stage accessors ``.ctffind``, ``.class2D``, etc. return a dictionary-like object that allows you to access individual Relion jobs within that particular stage.
The dictionary key names are the relion job names (usually ``jobXXX``), the dictionary value is a list of stage-specific named tuples, listed below.


Stage-specific information
--------------------------

.. autoclass:: relion._parser.ctffind.CTFMicrograph
    :members:


.. autoclass:: relion._parser.motioncorrection.MCMicrograph
    :members:


.. autoclass:: relion._parser.class2D.Class2DParticleClass
    :members:


.. autoclass:: relion._parser.class3D.Class3DParticleClass
    :members:



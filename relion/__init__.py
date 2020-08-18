"""
Relion Python API
https://github.com/DiamondLightSource/python-relion
"""

import functools
import pathlib
from relion._parser.ctffind import CTFFind
from relion._parser.motioncorrection import MotionCorr

__all__ = []
__author__ = "Diamond Light Source - Scientific Software"
__email__ = "scientificsoftware@diamond.ac.uk"
__version__ = "0.0.1"
__version_tuple__ = tuple(int(x) for x in __version__.split("."))


class Project:
    def __init__(self, path):
        self.basepath = pathlib.Path(path)
        if not self.basepath.is_dir():
            raise ValueError(f"path {self.basepath} is not a directory")

    def __eq__(self, other):
        if isinstance(other, Project):
            return self.basepath == other.basepath
        return False

    def __hash__(self):
        return hash(("relion.Project", self.basepath))

    def __repr__(self):
        return f"relion.Project({repr(str(self.basepath))})"

    def __str__(self):
        return f"<relion.Project at {self.basepath}>"

    @property
    @functools.lru_cache(maxsize=1)
    def ctffind(self):
        return CTFFind(self.basepath / "CtfFind")

    @property
    @functools.lru_cache(maxsize=1)
    def motioncorrection(self):
        return MotionCorr(self.basepath / "MotionCorr")

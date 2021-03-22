"""
Relion Python API
https://github.com/DiamondLightSource/python-relion
"""

import functools
import pathlib
from relion._parser.ctffind import CTFFind
from relion._parser.motioncorrection import MotionCorr
from relion._parser.class2D import Class2D
from relion._parser.class3D import Class3D
from relion._parser.relion_pipeline import RelionPipeline
from relion._parser.pipeline import ProcessNode

__all__ = []
__author__ = "Diamond Light Source - Scientific Software"
__email__ = "scientificsoftware@diamond.ac.uk"
__version__ = "0.2.0"
__version_tuple__ = tuple(int(x) for x in __version__.split("."))


class Project:
    """
    Reads information from a Relion project directory and makes it available in
    a structured, object-oriented, and pythonic fashion.
    """

    def __init__(self, path):
        """
        Create an object representing a Relion project.
        :param path: A string or file system path object pointing to the root
                     directory of an existing Relion project.
        """
        self.basepath = pathlib.Path(path)
        if not self.basepath.is_dir():
            raise ValueError(f"path {self.basepath} is not a directory")
        self.pipeline = RelionPipeline(
            self.basepath / "Movies", ProcessNode("Import/job001")
        )

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
        """access the CTFFind stage of the project.
        Returns a dictionary-like object with job names as keys,
        and lists of CTFMicrograph namedtuples as values."""
        return CTFFind(self.basepath / "CtfFind")

    @property
    @functools.lru_cache(maxsize=1)
    def motioncorrection(self):
        """access the motion correction stage of the project.
        Returns a dictionary-like object with job names as keys,
        and lists of MCMicrograph namedtuples as values."""
        return MotionCorr(self.basepath / "MotionCorr")

    @property
    @functools.lru_cache(maxsize=1)
    def class2D(self):
        """access the 2D classification stage of the project.
        Returns a dictionary-like object with job names as keys,
        and lists of Class2DParticleClass namedtuples as values."""
        return Class2D(self.basepath / "Class2D")

    @property
    @functools.lru_cache(maxsize=1)
    def class3D(self):
        """access the 3D classification stage of the project.
        Returns a dictionary-like object with job names as keys,
        and lists of Class3DParticleClass namedtuples as values."""
        return Class3D(self.basepath / "Class3D")

    def _load_pipeline(self):
        self.pipeline.load_nodes_from_star(self.basepath / "default_pipeline.star")
        self.pipeline.check_job_node_statuses(self.basepath)
        self.pipeline.collect_job_times(list(self.schedule_files))

    def show_job_nodes(self):
        self._load_pipeline()
        self.pipeline.show_job_nodes(self.basepath)

    @property
    def schedule_files(self):
        return self.basepath.glob("pipeline*.log")

    @property
    def current_job(self):
        self._load_pipeline()
        return self.pipeline.current_job()

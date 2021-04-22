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
import time
import copy

__all__ = []
__author__ = "Diamond Light Source - Scientific Software"
__email__ = "scientificsoftware@diamond.ac.uk"
__version__ = "0.4.1"
__version_tuple__ = tuple(int(x) for x in __version__.split("."))

pipeline_lock = ".relion_lock"


class Project(RelionPipeline):
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
        super().__init__(
            "Import/job001", locklist=[self.basepath / "default_pipeline.star"]
        )
        if not self.basepath.is_dir():
            raise ValueError(f"path {self.basepath} is not a directory")
        try:
            self.load()
        except (FileNotFoundError, RuntimeError):
            pass
            # raise RuntimeWarning(
            #    f"Relion Project was unable to load the relion pipeline from {self.basepath}/default_pipeline.star"
            # )
        self.res = RelionResults()

    @property
    def _plock(self):
        return PipelineLock(self.basepath / pipeline_lock)

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
    def _results_dict(self):
        resd = {
            "CtfFind": self.ctffind,
            "MotionCorr": self.motioncorrection,
            "Class2D": self.class2D,
            "Class3D": self.class3D,
        }
        return resd

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

    def origin_present(self):
        try:
            self.load_nodes_from_star(self.basepath / "default_pipeline.star")
        except (TypeError, FileNotFoundError, RuntimeError):
            return False
        if len(self._nodes) == 0:
            return False
        return (self.basepath / self.origin / "RELION_JOB_EXIT_SUCCESS").is_file()

    def load(self):
        self._jobs_collapsed = False
        self.load_nodes_from_star(self.basepath / "default_pipeline.star")
        self.check_job_node_statuses(self.basepath)
        self.collect_job_times(list(self.schedule_files))

    def show_job_nodes(self):
        self.load()
        super().show_job_nodes(self.basepath)

    @property
    def schedule_files(self):
        return self.basepath.glob("pipeline*.log")

    @property
    def results(self):
        self._clear_caches()
        res = []
        for jtnode in self:
            if jtnode.attributes["status"]:
                res_obj = self._results_dict.get(str(jtnode._path))
                if res_obj is not None:
                    res.append(
                        (
                            res_obj,
                            jtnode.attributes["job"],
                            jtnode.attributes["end_time_stamp"],
                        )
                    )
        self.res.consume(res)
        return self.res

    @property
    def current_jobs(self):
        self.load()
        currj = super().current_jobs
        if currj is None:
            return None
        else:
            for n in currj:
                n.change_name(self.basepath / n.name)
            return currj

    @staticmethod
    def _clear_caches():
        Project.motioncorrection.fget.cache_clear()
        Project.ctffind.fget.cache_clear()
        Project.class2D.fget.cache_clear()
        Project.class3D.fget.cache_clear()


class RelionResults:
    def __init__(self):
        self._results = []
        self._fresh_results = []
        self._seen_before = []
        self._fresh_called = False
        self._cache = {}

    def consume(self, results):
        self._results = [(r[0], r[1]) for r in results]
        if not self._fresh_called:
            self._fresh_results = self._results
            for r in results:
                if r[2] not in [p[1] for p in self._seen_before]:
                    self._cache[r[1]] = []
                    for single_res in r[0][r[1]]:
                        self._cache[r[1]].append(r[0].for_cache(single_res))
                if (r[1], r[2]) not in self._seen_before:
                    self._seen_before.append((r[1], r[2]))
        else:
            self._fresh_results = []
            results_copy = copy.deepcopy(results)
            for r in results_copy:
                if (r[1], r[2]) not in self._seen_before:
                    current_job_results = list(r[0][r[1]])
                    for single_res in current_job_results:
                        if self._cache.get(r[1]) is None:
                            self._cache[r[1]] = []
                        if r[0].for_cache(single_res) in self._cache[r[1]]:
                            r[0][r[1]].remove(single_res)
                        else:
                            self._cache[r[1]].append(r[0].for_cache(single_res))

                    self._fresh_results.append((r[0], r[1]))
                    self._seen_before.append((r[1], r[2]))

    def __iter__(self):
        return iter(self._results)

    @property
    def fresh(self):
        self._fresh_called = True
        return iter(self._fresh_results)


# helper class for dealing with the default_pipeline.star lock
class PipelineLock:
    def __init__(self, lockdir):
        self.lockdir = lockdir
        self.fail_count = 0
        self.obtained = False

    def __enter__(self):
        while self.fail_count < 20:
            try:
                self.lockdir.mkdir()
                self.obtained = True
                break
            except FileExistsError:
                time.sleep(0.1)
                self.fail_count += 1
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self.obtained:
            self.lockdir.rmdir()
        self.obtained = False

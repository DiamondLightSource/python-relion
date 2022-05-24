"""
Relion Python API
https://github.com/DiamondLightSource/python-relion
"""

from __future__ import annotations

import functools
import os
import pathlib
import time
from collections import namedtuple

from gemmi import cif

from relion._parser.autopick import AutoPick
from relion._parser.class2D import Class2D
from relion._parser.class3D import Class3D
from relion._parser.cryolo import Cryolo
from relion._parser.ctffind import CTFFind
from relion._parser.initialmodel import InitialModel
from relion._parser.motioncorrection import MotionCorr
from relion._parser.relativeicethickness import RelativeIceThickness
from relion._parser.relion_pipeline import RelionPipeline

try:
    from relion.cryolo_relion_it.cryolo_relion_it import RelionItOptions
except ModuleNotFoundError:
    pass
import logging

from relion.dbmodel import DBGraph, DBModel, DBNode
from relion.node.graph import Graph

logger = logging.getLogger("relion.Project")

__all__ = []
__author__ = "Diamond Light Source - Scientific Software"
__email__ = "scientificsoftware@diamond.ac.uk"
__version__ = "0.10.5"
__version_tuple__ = tuple(int(x) for x in __version__.split("."))

pipeline_lock = ".relion_lock"


RelionJobResult = namedtuple(
    "RelionJobResult",
    [
        "stage_object",
        "job_name",
        "end_time_stamp",
    ],
)

RelionJobInfo = namedtuple(
    "RelionJobInfo",
    [
        "job_name",
        "end_time_stamp",
    ],
)


class Project(RelionPipeline):
    """
    Reads information from a Relion project directory and makes it available in
    a structured, object-oriented, and pythonic fashion.
    """

    def __init__(
        self,
        path,
        database="ISPyB",
        run_options=None,
        message_constructors=None,
        cluster=False,
    ):
        """
        Create an object representing a Relion project.
        :param path: A string or file system path object pointing to the root
                     directory of an existing Relion project.
        """
        self.basepath = pathlib.Path(path)
        super().__init__(
            "Import/job001", locklist=[self.basepath / "default_pipeline.star"]
        )
        if message_constructors is not None:
            self.construct_messages = message_constructors
        else:
            self.construct_messages = {}
        if not self.basepath.is_dir():
            raise ValueError(f"path {self.basepath} is not a directory")
        self._data_pipeline = Graph("DataPipeline", [])
        self._db_model = DBModel(database)
        self._drift_cache = {}
        if run_options is None:
            self.run_options = RelionItOptions()
        else:
            self.run_options = run_options
        try:
            self.load(cluster=cluster)
        except (FileNotFoundError, OSError, RuntimeError):
            pass
            # raise RuntimeWarning(
            #    f"Relion Project was unable to load the relion pipeline from {self.basepath}/default_pipeline.star"
            # )
        # self.res = RelionResults()
        self._drift_cache = {}

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
            "AutoPick": self.autopick,
            "External/crYOLO_AutoPick/": self.cryolo,
            "Class2D": self.class2D,
            "InitialModel": self.initialmodel,
            "Class3D": self.class3D,
            "External/Icebreaker_5fig/": self.relativeicethickness,
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
        return MotionCorr(self.basepath / "MotionCorr", self._drift_cache)

    @property
    @functools.lru_cache(maxsize=1)
    def autopick(self):
        return AutoPick(self.basepath / "AutoPick")

    @property
    @functools.lru_cache(maxsize=1)
    def cryolo(self):
        return Cryolo(self.basepath / "External")

    @property
    @functools.lru_cache(maxsize=1)
    def class2D(self):
        """access the 2D classification stage of the project.
        Returns a dictionary-like object with job names as keys,
        and lists of Class2DParticleClass namedtuples as values."""
        return Class2D(self.basepath / "Class2D")

    @property
    @functools.lru_cache(maxsize=1)
    def initialmodel(self):
        return InitialModel(self.basepath / "InitialModel")

    @property
    @functools.lru_cache(maxsize=1)
    def class3D(self):
        """access the 3D classification stage of the project.
        Returns a dictionary-like object with job names as keys,
        and lists of Class3DParticleClass namedtuples as values."""
        return Class3D(self.basepath / "Class3D")

    @property
    @functools.lru_cache(maxsize=1)
    def relativeicethickness(self):
        return RelativeIceThickness(self.basepath / "External")

    def origin_present(self):
        try:
            self.load_nodes_from_star(self.basepath / "default_pipeline.star")
        except (TypeError, FileNotFoundError, OSError, RuntimeError):
            return False
        if len(self._nodes) == 0:
            return False
        return (self.basepath / self.origin / "RELION_JOB_EXIT_SUCCESS").is_file()

    def load(self, clear_cache=True, cluster=False):
        if clear_cache:
            self._clear_caches()
        self._data_pipeline = Graph("DataPipeline", [])
        # reset the in and out lists of database nodes
        # have to avoid removing the permanent connections from other database nodes
        for dbn in self._db_model.values():
            dbn._in = [
                i_node
                for i_node in dbn._in
                if isinstance(i_node, DBNode) or isinstance(i_node, DBGraph)
            ]
        self._jobs_collapsed = False
        self.load_nodes_from_star(self.basepath / "default_pipeline.star")
        self.check_job_node_statuses(self.basepath)
        self.collect_job_times(
            list(self.schedule_files), self.basepath / "pipeline_PREPROCESS.log"
        )
        if cluster:
            self.collect_cluster_info(self.basepath)
        for jobnode in self:
            if (
                self._results_dict.get(jobnode.name)
                or jobnode.environment.get("alias") in self._results_dict
            ):
                if jobnode.name == "InitialModel":
                    self._update_pipeline(
                        jobnode,
                        jobnode.name,
                        prop=("ini_model_job_string", "ini_model_job_string"),
                        in_db_model=False,
                    )
                elif jobnode.name == "AutoPick":
                    self._update_pipeline(
                        jobnode, jobnode.name, prop=("job_string", "parpick_job_string")
                    )
                elif "crYOLO" in jobnode.environment.get("alias"):
                    self._update_pipeline(
                        jobnode,
                        jobnode.environment.get("alias"),
                        prop=("job_string", "parpick_job_string"),
                    )
                elif jobnode.name == "External":
                    self._update_pipeline(
                        jobnode,
                        jobnode.environment.get("alias"),
                    )
                else:
                    self._update_pipeline(jobnode, jobnode.name)
            else:
                self._data_pipeline.add_node(jobnode)
                if jobnode.name == "Import":
                    self._data_pipeline.origins = [jobnode]

    def _update_pipeline(self, jobnode, label, prop=None, in_db_model=True):
        jobnode.environment["result"] = self._results_dict[label]
        if in_db_model:
            jobnode.environment["extra_options"] = self.run_options
            self._db_model[label].environment["extra_options"] = self.run_options
            self._db_model[label].environment[
                "message_constructors"
            ] = self.construct_messages
        if prop is not None:
            jobnode.propagate(prop)
        jobnode.link_to(
            self._db_model[label],
            result_as_traffic=True,
            share=[("end_time_stamp", "end_time")],
        )
        self._data_pipeline.add_node(jobnode)
        if in_db_model:
            self._data_pipeline.add_node(self._db_model[label])

    def show_job_nodes(self):
        self.load()
        super().show_job_nodes(self.basepath)

    @property
    def schedule_files(self):
        return self.basepath.glob("pipeline*.log")

    @property
    def messages(self):
        self._clear_caches()
        msgs = []
        results = self._data_pipeline()
        if results is None:
            return msgs
        for node in self._db_model.db_nodes:
            try:
                if results[node.nodeid] is not None:
                    d = {}
                    if isinstance(results[node.nodeid], dict):
                        for key, val in results[node.nodeid].items():
                            try:
                                d[key].append(val)
                            except KeyError:
                                d[key] = val
                    else:
                        for p in results[node.nodeid]:
                            for key, val in p.items():
                                try:
                                    d[key].extend(val)
                                except KeyError:
                                    d[key] = val
                    msgs.append(d)
            except KeyError:
                logger.debug(
                    f"No results found for {node.name}: probably the job has not completed yet"
                )
        return msgs

    @property
    def current_jobs(self):
        self.load(cluster=True)
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
        Project.autopick.fget.cache_clear()
        Project.cryolo.fget.cache_clear()
        Project.class2D.fget.cache_clear()
        Project.initialmodel.fget.cache_clear()
        Project.class3D.fget.cache_clear()
        Project.relativeicethickness.fget.cache_clear()

    def get_imported(self):
        try:
            import_job_path = self.basepath / self.origin
            gemmi_readable_path = os.fspath(import_job_path / "movies.star")
            star_doc = cif.read_file(gemmi_readable_path)
            for index, block in enumerate(star_doc):
                if list(block.find_loop("_rlnMicrographMovieName")):
                    block_index = index
                    break
            else:
                return []
            data_block = star_doc[block_index]
            values = list(data_block.find_loop("_rlnMicrographMovieName"))
            if not values:
                return []
            return values
        except (FileNotFoundError, OSError, RuntimeError, ValueError):
            return []


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

"""
Relion Python API
https://github.com/DiamondLightSource/python-relion
"""

import copy
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
from relion._parser.relion_pipeline import RelionPipeline

try:
    from relion.cryolo_relion_it.cryolo_relion_it import RelionItOptions
except ModuleNotFoundError:
    pass
import logging

from relion.dbmodel import DBModel
from relion.node.graph import Graph

logger = logging.getLogger("relion.Project")

__all__ = []
__author__ = "Diamond Light Source - Scientific Software"
__email__ = "scientificsoftware@diamond.ac.uk"
__version__ = "0.7.3"
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

<<<<<<< e53f5f7a98e64592b9453b5140689b8ab3c1d36a
    def __init__(
        self, path, database="ISPyB", run_options=None, message_constructors=None
    ):
=======
    def __init__(self, path, database="ISPyB", run_options=None):
>>>>>>> Connect ProcessNodes to DBNodes so that collected information ends up in the database model
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
            self.load()
        except (FileNotFoundError, RuntimeError):
            pass
            # raise RuntimeWarning(
            #    f"Relion Project was unable to load the relion pipeline from {self.basepath}/default_pipeline.star"
            # )
        self.res = RelionResults()
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
            "External:crYOLO": self.cryolo,
            "Class2D": self.class2D,
            "InitialModel": self.initialmodel,
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

    def origin_present(self):
        try:
            self.load_nodes_from_star(self.basepath / "default_pipeline.star")
        except (TypeError, FileNotFoundError, RuntimeError):
            return False
        if len(self._nodes) == 0:
            return False
        return (self.basepath / self.origin / "RELION_JOB_EXIT_SUCCESS").is_file()

    def load(self, clear_cache=True):
        if clear_cache:
            self._clear_caches()
        self._jobs_collapsed = False
        self.load_nodes_from_star(self.basepath / "default_pipeline.star")
        self.check_job_node_statuses(self.basepath)
        self.collect_job_times(
            list(self.schedule_files), self.basepath / "pipeline_PREPROCESS.log"
        )
        for jobnode in self:
<<<<<<< e53f5f7a98e64592b9453b5140689b8ab3c1d36a
            if self._results_dict.get(
                jobnode.name
            ) or "crYOLO" in jobnode.environment.get("alias"):
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
                        f"{jobnode._path}:crYOLO",
                        prop=("job_string", "parpick_job_string"),
                    )
                else:
                    self._update_pipeline(jobnode, jobnode.name)
=======
            if self._results_dict.get(jobnode.name) and jobnode.name != "InitialModel":
                jobnode.environment["result"] = self._results_dict[jobnode.name]
                jobnode.environment["extra_options"] = self.run_options
                self._db_model[jobnode.name].environment[
                    "extra_options"
                ] = self.run_options
                self._db_model[jobnode.name].environment[
                    "message_constructor"
                ] = construct_message
                jobnode.link_to(
                    self._db_model[jobnode.name],
                    result_as_traffic=True,
                    share=[("end_time", "end_time")],
                )
                self._data_pipeline.add_node(jobnode)
                self._data_pipeline.add_node(self._db_model[jobnode.name])
                if jobnode.name == "AutoPick":
                    jobnode.propagate(("job_string", "parpick_job_string"))
            elif jobnode.name == "InitialModel":
                jobnode.environment["result"] = self._results_dict[jobnode.name]
                jobnode.link_to(
                    self._db_model[jobnode.name],
                    result_as_traffic=True,
                    share=[("end_time", "end_time")],
                )
                self._data_pipeline.add_node(jobnode)
                jobnode.propagate(("ini_model_job_string", "ini_model_job_string"))
            elif "crYOLO" in jobnode.environment.get("alias"):
                jobnode.environment["result"] = self._results_dict[
                    f"{jobnode._path}:crYOLO"
                ]
                jobnode.environment["extra_options"] = self.run_options
                self._db_model[f"{jobnode._path}:crYOLO"].environment[
                    "extra_options"
                ] = self.run_options
                self._db_model[f"{jobnode._path}:crYOLO"].environment[
                    "message_constructor"
                ] = construct_message
                jobnode.propagate(("job_string", "parpick_job_string"))
                jobnode.link_to(
                    self._db_model[f"{jobnode._path}:crYOLO"],
                    result_as_traffic=True,
                    share=[("end_time", "end_time")],
                )
                self._data_pipeline.add_node(jobnode)
                self._data_pipeline.add_node(self._db_model[f"{jobnode._path}:crYOLO"])
>>>>>>> Connect ProcessNodes to DBNodes so that collected information ends up in the database model
            else:
                self._data_pipeline.add_node(jobnode)
                if jobnode.name == "Import":
                    self._data_pipeline.origins = [jobnode]
<<<<<<< e53f5f7a98e64592b9453b5140689b8ab3c1d36a

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
=======
>>>>>>> Connect ProcessNodes to DBNodes so that collected information ends up in the database model

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
<<<<<<< e53f5f7a98e64592b9453b5140689b8ab3c1d36a
            try:
                if results[node.nodeid] is not None:
                    d = {}
                    for p in results[node.nodeid]:
                        for key, val in p.items():
                            try:
                                d[key].extend(val)
                            except KeyError:
                                d[key] = val
                    msgs.append(d)
=======
            print(node)
            try:
                if results[node.name + "-" + node.nodeid] is not None:
                    msgs.append(results[node.name + "-" + node.nodeid])
>>>>>>> Connect ProcessNodes to DBNodes so that collected information ends up in the database model
            except KeyError:
                logger.debug(
                    f"No results found for {node.name}: probably the job has not completed yet"
                )
        return msgs

    @property
    def results(self):
        self._clear_caches()
        res = []
        for jtnode in self:
            if jtnode.environment["status"]:
                if "crYOLO" in jtnode.environment["alias"]:
                    res_obj = self._results_dict.get(f"{jtnode._path}:crYOLO")
                else:
                    res_obj = self._results_dict.get(str(jtnode._path))
                if res_obj is not None:
                    res.append(
                        RelionJobResult(
                            res_obj,
                            jtnode.environment["job"],
                            jtnode.environment["end_time_stamp"],
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
        Project.autopick.fget.cache_clear()
        Project.cryolo.fget.cache_clear()
        Project.class2D.fget.cache_clear()
        Project.initialmodel.fget.cache_clear()
        Project.class3D.fget.cache_clear()

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
        except (FileNotFoundError, RuntimeError, ValueError):
            return []


class RelionResults:
    def __init__(self):
        self._results = []
        self._fresh_results = []
        self._seen_before = []
        self._fresh_called = False
        self._cache = {}
        self._validation_cache = {}

    @staticmethod
    def _update_temp_validation_cache(
        stage, job, current_result, cache, results_for_validation
    ):
        validation_check = stage.for_validation(current_result)
        if validation_check:
            if not cache.get(stage):
                cache[stage] = {}
            if (stage, job) not in results_for_validation:
                results_for_validation.append((stage, job))
            cache[stage].update(validation_check)

    def consume(self, results):
        self._results = results  # [(r.stage_object, r.job_name) for r in results]
        curr_val_cache = {}
        results_for_validation = []
        if not self._fresh_called:
            self._fresh_results = self._results
            for r in results:
                end_time_not_seen_before = r.end_time_stamp not in [
                    p.end_time_stamp for p in self._seen_before
                ]
                if end_time_not_seen_before:
                    self._cache[r.job_name] = []
                for single_res in r.stage_object[r.job_name]:
                    self._update_temp_validation_cache(
                        r.stage_object,
                        r.job_name,
                        single_res,
                        curr_val_cache,
                        results_for_validation,
                    )
                    if end_time_not_seen_before:
                        self._cache[r.job_name].append(
                            r.stage_object.for_cache(single_res)
                        )
                if (r.job_name, r.end_time_stamp) not in self._seen_before:
                    self._seen_before.append(
                        RelionJobInfo(r.job_name, r.end_time_stamp)
                    )

        else:
            self._fresh_results = []
            results_copy = copy.deepcopy(results)
            for r in results_copy:
                current_job_results = list(r.stage_object[r.job_name])
                not_seen_before = (
                    r.job_name,
                    r.end_time_stamp,
                ) not in self._seen_before
                for single_res in current_job_results:
                    self._update_temp_validation_cache(
                        r.stage_object,
                        r.job_name,
                        single_res,
                        curr_val_cache,
                        results_for_validation,
                    )
                    if not_seen_before:
                        if self._cache.get(r.job_name) is None:
                            self._cache[r.job_name] = []
                        if (
                            r.stage_object.for_cache(single_res)
                            in self._cache[r.job_name]
                        ):
                            r.stage_object[r.job_name].remove(single_res)
                        else:
                            self._cache[r.job_name].append(
                                r.stage_object.for_cache(single_res)
                            )
                if not_seen_before:
                    self._fresh_results.append(r)
                    self._seen_before.append(
                        RelionJobInfo(r.job_name, r.end_time_stamp)
                    )
        self._validate(curr_val_cache, results_for_validation)

    # check if a validation dictionary is compatible with the previously stored validation dictionary
    # if not correct the offending results
    # the validation dictionary is a dictionary of dictionaries:
    # for a given stage there is a dictionary with relevant keys (such as micrograph names) and numeric values
    # these values must be conitguous integers to pass validation
    def _validate(self, new_val_cache, job_results):
        for stage, job in job_results:
            # print(stage, job)

            new_numbers = sorted(new_val_cache[stage].values())

            # ignore if there are no results being reported for validation
            if len(new_numbers) == 0:
                continue

            new_missing_numbers = sorted(
                set(range(new_numbers[0], new_numbers[-1] + 1)).difference(new_numbers)
            )
            # if the count of results is not contiguous something has gone wrong
            # wherever they were counted
            if len(new_missing_numbers) != 0:
                raise ValueError("Validation numbers were not contiguous")

            # if there is no pre-existing validation cache for this stage then set it and move on
            if self._validation_cache.get(stage) is None:
                self._validation_cache[stage] = new_val_cache[stage]
                continue

            numbers = sorted(self._validation_cache[stage].values())

            start_new_count = None

            # check if there is a mismatch between old and new caches or if there
            # are results in the old cache that are not in the new cache
            # if there is overlap between new cache and old cache fix new cache
            # to match old on the overlap
            for name, num in self._validation_cache[stage].items():
                if new_val_cache[stage].get(name) != num:
                    new_val_cache[stage][name] = num
                    if start_new_count is None:
                        start_new_count = len(numbers) + 1

            # if the validation has failed then correct the offending attributes
            if start_new_count:
                name_diffs = set(new_val_cache[stage].keys()).difference(
                    self._validation_cache[stage].keys()
                )
                nums_for_name_diffs = sorted(
                    [(new_val_cache[stage][k], k) for k in name_diffs],
                    key=lambda x: x[0],
                )
                for index, new_name in enumerate([p[1] for p in nums_for_name_diffs]):
                    new_val_cache[stage][new_name] = start_new_count + index

            if start_new_count:
                for mindex, mic in enumerate(stage[job]):
                    stage[job][mindex] = stage.mutate_result(
                        mic, micrograph_number=new_val_cache[stage][mic.micrograph_name]
                    )
                for index, res in enumerate(self._results):
                    if res.job_name == job:
                        self._results[index] = (stage, job)

            self._validation_cache[stage].update(new_val_cache[stage])

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

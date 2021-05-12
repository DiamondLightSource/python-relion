"""
Relion Python API
https://github.com/DiamondLightSource/python-relion
"""

import functools
import pathlib
from gemmi import cif
from relion._parser.ctffind import CTFFind
from relion._parser.motioncorrection import MotionCorr
from relion._parser.autopick import AutoPick
from relion._parser.cryolo import Cryolo
from relion._parser.class2D import Class2D
from relion._parser.class3D import Class3D
from relion._parser.relion_pipeline import RelionPipeline
import time
import copy
import os

__all__ = []
__author__ = "Diamond Light Source - Scientific Software"
__email__ = "scientificsoftware@diamond.ac.uk"
__version__ = "0.4.13"
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
            "AutoPick": self.autopick,
            "External:crYOLO": self.cryolo,
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
        self.collect_job_times(
            list(self.schedule_files), self.basepath / "pipeline_PREPROCESS.log"
        )

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
                if "crYOLO" in jtnode.attributes.get("alias"):
                    res_obj = self._results_dict.get(f"{jtnode._path}:crYOLO")
                else:
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
        Project.autopick.fget.cache_clear()
        Project.cryolo.fget.cache_clear()
        Project.class2D.fget.cache_clear()
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
            data_block = star_doc[block_index]
            values = list(data_block.find_loop("_rlnMicrographMovieName"))
            if not values:
                return []
            return values
        except (FileNotFoundError, RuntimeError):
            return []


class RelionResults:
    def __init__(self):
        self._results = []
        self._fresh_results = []
        self._seen_before = []
        self._fresh_called = False
        self._cache = {}
        self._validation_cache = {}

    def consume(self, results):
        self._results = [(r[0], r[1]) for r in results]
        curr_val_cache = {}
        results_for_validation = []
        if not self._fresh_called:
            self._fresh_results = self._results
            for r in results:
                end_time_not_seen_before = r[2] not in [p[1] for p in self._seen_before]
                if end_time_not_seen_before:
                    self._cache[r[1]] = []
                for single_res in r[0][r[1]]:
                    validation_check = r[0].for_validation(single_res)
                    if validation_check:
                        if not curr_val_cache.get(r[0]):
                            curr_val_cache[r[0]] = {}
                        if (r[0], r[1]) not in results_for_validation:
                            results_for_validation.append((r[0], r[1]))
                        curr_val_cache[r[0]].update(validation_check)
                    if end_time_not_seen_before:
                        self._cache[r[1]].append(r[0].for_cache(single_res))
                if (r[1], r[2]) not in self._seen_before:
                    self._seen_before.append((r[1], r[2]))
            validated_results = self._validate(curr_val_cache, results_for_validation)
            if validated_results:
                for r0, r1 in validated_results:
                    for index, t in enumerate(self._results):
                        if r1 == t[1]:
                            self._results[index] = (r0, r1)
        else:
            self._fresh_results = []
            results_copy = copy.deepcopy(results)
            for r in results_copy:
                current_job_results = list(r[0][r[1]])
                for single_res in current_job_results:
                    validation_check = r[0].for_validation(single_res)
                    if validation_check:
                        if not curr_val_cache.get(r[0]):
                            curr_val_cache[r[0]] = {}
                        if (r[0], r[1]) not in results_for_validation:
                            results_for_validation.append((r[0], r[1]))
                        curr_val_cache[r[0]].update(validation_check)
                if (r[1], r[2]) not in self._seen_before:
                    for single_res in current_job_results:
                        if self._cache.get(r[1]) is None:
                            self._cache[r[1]] = []
                        if r[0].for_cache(single_res) in self._cache[r[1]]:
                            r[0][r[1]].remove(single_res)
                        else:
                            self._cache[r[1]].append(r[0].for_cache(single_res))

                    self._fresh_results.append((r[0], r[1]))
                    self._seen_before.append((r[1], r[2]))
            validated_results = self._validate(curr_val_cache, results_for_validation)
            if validated_results:
                for r0, r1 in validated_results:
                    for index, t in enumerate(self._results):
                        if r1 == t[1]:
                            self._results[index] = (r0, r1)

    def _validate(self, new_val_cache, job_results):
        # print(new_val_cache)
        changes_made = False
        for stage, job in job_results:
            validation_failed = False

            new_numbers = sorted(new_val_cache[stage].values())

            new_missing_numbers = sorted(
                set(range(new_numbers[0], new_numbers[-1] + 1)).difference(new_numbers)
            )
            if len(new_missing_numbers) != 0:
                raise ValueError("Validation numbers were not contiguous")

            if self._validation_cache.get(stage) is None:
                self._validation_cache[stage] = new_val_cache[stage]
                continue

            numbers = sorted(self._validation_cache[stage].values())

            if len(new_numbers) == 0:
                return

            if len(new_numbers) < len(numbers):
                for key in new_val_cache[stage].keys():
                    if key not in self._validation_cache[stage].keys():
                        raise ValueError(
                            f"New result for {key} found but there are fewer results overall than those cached"
                        )
                    if new_val_cache[stage][key] != self._validation_cache[stage][key]:
                        raise ValueError(
                            f"Results validation failed for {key} but there are fewer new results than cached results: unsupported"
                        )
                return

            if numbers == new_numbers:
                if set(self._validation_cache[stage].keys()) != set(
                    new_val_cache[stage].keys()
                ):
                    raise ValueError(
                        "Numbering validation failed because new names didn't match the cached names"
                    )

            start_new_count = None

            for name, num in self._validation_cache[stage].items():
                if new_val_cache[stage][name] != num:
                    validation_failed = True
                    new_val_cache[stage][name] = num
                    if start_new_count is None:
                        start_new_count = len(numbers) + 1

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

            if validation_failed:
                changes_made = True
                for mindex, mic in enumerate(stage[job]):
                    stage[job][mindex] = stage.mutate_result(
                        mic, micrograph_number=new_val_cache[stage][mic.micrograph_name]
                    )

            self._validation_cache[stage] = new_val_cache[stage]
        # print(self._validation_cache)
        if changes_made:
            return job_results

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

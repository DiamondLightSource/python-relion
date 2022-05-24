from __future__ import annotations

import functools
import math
import os
import pathlib
import queue
import subprocess
import threading
import time
from typing import Dict, List, Optional, Set, Tuple

from gemmi import cif
from pipeliner.api.api_utils import (
    edit_jobstar,
    job_parameters_dict,
    write_default_jobstar,
)
from pipeliner.api.manage_project import PipelinerProject
from pipeliner.job_runner import JobRunner

from relion.cryolo_relion_it.cryolo_relion_it import RelionItOptions
from relion.pipeline.extra_options import generate_extra_options
from relion.pipeline.options import generate_pipeline_options


def _clear_queue(q: queue.Queue) -> List[str]:
    results = []
    while not q.empty():
        results.append(q.get())
    return results


class PipelineRunner:
    def __init__(
        self,
        projpath: pathlib.Path,
        stopfile: pathlib.Path,
        options: RelionItOptions,
        moviesdir: str = "Movies",
        movietype: str = "mrc",
    ):
        self.path = projpath
        self.movies_path = projpath / moviesdir
        self.movietype = movietype if not movietype[0] == "." else movietype[1:]
        self.project = PipelinerProject()
        self.stopfile = stopfile
        self.options = options
        self.pipeline_options: Dict[str, dict] = self._generate_pipeline_options()
        self.job_paths: Dict[str, pathlib.Path] = {}
        self.job_paths_batch: Dict[str, Dict[str, pathlib.Path]] = {}
        self._past_class_threshold = False
        self._queues: Dict[str, List[queue.Queue]] = {
            "class2D": [queue.Queue()],
            "class3D": [queue.Queue()],
            "ib_group": [queue.Queue()],
        }
        self._passes: List[Set[str]] = [set(), set()]
        self._num_seen_movies = 0
        self._lock = threading.RLock()
        self._extra_options = generate_extra_options
        if self.options.do_second_pass:
            for q in self._queues.values():
                q.append(queue.Queue())

    def _generate_pipeline_options(self):
        pipeline_jobs = {
            "relion.import.movies": "",
            "relion.motioncorr.motioncorr2": "gpu",
            "relion.motioncorr.own": "cpu",
            "icebreaker.micrograph_analysis.micrographs": "cpu-smp",
            "icebreaker.micrograph_analysis.enhancecontrast": "cpu-smp",
            "icebreaker.micrograph_analysis.summary": "cpu-smp",
            "relion.ctffind.ctffind4": "cpu",
            "relion.autopick.log": "cpu",
            "relion.autopick.ref3d": "cpu",
            "cryolo.autopick": "gpu",
            "relion.extract": "cpu",
            "relion.select.split": "",
            "icebreaker.micrograph_analysis.particles": "cpu-smp",
            "relion.class2d.em": "gpu",
            "relion.class2d.vdam": "gpu-smp",
            "relion.initialmodel": "gpu-smp",
            "relion.class3d": "gpu-smp",
        }
        return generate_pipeline_options(self.options, pipeline_jobs)

    def fresh_job(
        self,
        job: str,
        extra_params: Optional[dict] = None,
        wait: bool = True,
        lock: Optional[threading.RLock] = None,
        alias: str = "",
    ) -> pathlib.Path:
        write_default_jobstar(job)
        params = job_parameters_dict(job)
        params.update(self.pipeline_options.get(job, {}))
        if extra_params is not None:
            params.update(extra_params)
        _params = {k: str(v) for k, v in params.items() if not isinstance(v, bool)}

        def _b2s(bv: bool) -> str:
            if bv:
                return "Yes"
            return "No"

        _params.update({k: _b2s(v) for k, v in params.items() if isinstance(v, bool)})
        params = _params
        edit_jobstar(
            f"{job.replace('.', '_')}_job.star",
            params,
            f"{job.replace('.', '_')}_job.star",
        )
        if lock is None:
            job_path = self.project.run_job(
                f"{job.replace('.', '_')}_job.star",
                wait_for_queued=wait,
            )
            if alias:
                self.project.set_alias(job_path, alias)
        else:
            with lock:
                job_path = self.project.run_job(
                    f"{job.replace('.', '_')}_job.star",
                    wait_for_queued=False,
                )
                if alias:
                    self.project.set_alias(job_path, alias)
            runner = JobRunner(self.project.pipeline.name)
            runner.wait_for_queued_job_completion(job_path)
        return pathlib.Path(job_path)

    def _get_split_files(self, select_job: pathlib.Path) -> List[str]:
        all_split_files = list(select_job.glob("*particles_split*.star"))
        if len(all_split_files) == 1:
            return [str(all_split_files[0])]
        # drop the most recent batch if there is more than one as it probably isn't complete
        def batch(fname: str) -> int:
            spfname = fname.split("particles_split")
            return int(spfname[-1].replace(".star", ""))

        with_batch_numbers = [(batch(str(f)), str(f)) for f in all_split_files]
        sorted_batch_numbers = sorted(with_batch_numbers, key=lambda x: x[0])
        # if there is more than one batch then we are past the threshold for a single batch
        if len(sorted_batch_numbers) > 1:
            self._past_class_threshold = True
        return [s[1] for s in sorted_batch_numbers[:-1]]

    def _get_num_movies(self, star_file: pathlib.Path) -> int:
        star_doc = cif.read_file(os.fspath(star_file))
        return len(list(star_doc[1].find_loop("_rlnMicrographName")))

    def preprocessing(self, ref3d: str = "", ref3d_angpix: float = -1) -> List[str]:
        if ref3d and self.options.autopick_do_cryolo:
            return []

        jobs = ["relion.import.movies"]
        if self.options.motioncor_do_own:
            jobs.append("relion.motioncorr.own")
        else:
            jobs.append("relion.motioncorr.motioncorr2")
        if self.options.do_icebreaker_job_group:
            jobs.append("icebreaker.micrograph_analysis.micrographs")
        if self.options.do_icebreaker_job_flatten:
            jobs.append("icebreaker.micrograph_analysis.enhancecontrast")
        if self.options.do_icebreaker_fivefig:
            jobs.append("icebreaker.micrograph_analysis.summary")
        jobs.append("relion.ctffind.ctffind4")

        for job in jobs:
            if not self.job_paths.get(job):
                self.job_paths[job] = self.fresh_job(
                    job,
                    extra_params=self._extra_options(job, self.job_paths, self.options),
                    lock=self._lock,
                )
            else:
                if self._lock:
                    with self._lock:
                        self.project.continue_job(
                            str(self.job_paths[job]), wait_for_queued=False
                        )
                    runner = JobRunner(self.project.pipeline.name)
                    runner.wait_for_queued_job_completion(str(self.job_paths[job]))
                else:
                    self.project.continue_job(str(self.job_paths[job]))
        if self.job_paths.get("relion.motioncorr.own"):
            self._num_seen_movies = self._get_num_movies(
                self.job_paths["relion.motioncorr.own"] / "corrected_micrographs.star"
            )
        elif self.job_paths.get("relion.motioncorr.motioncorr2"):
            self._num_seen_movies = self._get_num_movies(
                self.job_paths["relion.motioncorr.motioncorr2"]
                / "corrected_micrographs.star"
            )
        else:
            raise KeyError(
                "Neither a relion.motioncorr.own nor a relion.motioncorr.motioncorr2 job were found"
            )
        if self.options.stop_after_ctf_estimation:
            return []

        if self.options.autopick_do_cryolo:
            next_jobs = ["cryolo.autopick"]
        elif self.options.autopick_do_LoG and not ref3d:
            next_jobs = ["relion.autopick.log"]
        else:
            next_jobs = ["relion.autopick.ref3d"]
        next_jobs.extend(["relion.extract" + ref3d, "relion.select.split" + ref3d])
        for job in next_jobs:
            if not self.job_paths.get(job):
                if job == "relion.autopick.ref3d":
                    self.job_paths[job] = self.fresh_job(
                        job.replace(ref3d, ""),
                        extra_params={
                            **self._extra_options(job, self.job_paths, self.options),
                            **{"fn_ref3d_autopick": ref3d, "angpix_ref": ref3d_angpix},
                        },
                        lock=self._lock,
                    )
                else:
                    self.job_paths[job] = self.fresh_job(
                        job.replace(ref3d, ""),
                        extra_params=self._extra_options(
                            job, self.job_paths, self.options
                        ),
                        lock=self._lock,
                    )
            else:
                self.project.continue_job(str(self.job_paths[job]))
        select_path = self.job_paths["relion.select.split" + ref3d]
        return self._get_split_files(select_path)

    @functools.lru_cache(maxsize=1)
    def _best_class(
        self, job: str = "relion.initialmodel", batch: str = ""
    ) -> Tuple[Optional[str], Optional[float]]:
        try:
            model_file_candidates = list(
                (self.path / self.job_paths_batch[job][batch]).glob("*_model.star")
            )
        except (TypeError, KeyError):
            model_file_candidates = list(
                (self.path / self.job_paths[job]).glob("*_model.star")
            )

        def iteration_count(x: pathlib.Path) -> int:
            parts = str(x).split("_")
            for _x in parts:
                if _x.startswith("it"):
                    return int(_x.replace("it", ""))
            return 0

        model_file_candidates = sorted(model_file_candidates, key=iteration_count)
        model_file_candidates.reverse()
        model_file = model_file_candidates[0]
        try:
            star_doc = cif.read_file(os.fspath(model_file))
            block_num = None
            for block_index, block in enumerate(star_doc):
                if list(block.find_loop("_rlnReferenceImage")):
                    block_num = block_index
                    break
            if block_num is None:
                return None, None
            star_block = star_doc[block_num]
            ref = list(star_block.find_loop("_rlnReferenceImage"))[0]
            ref_size: float = 0
            ref_resolution = None
            for i, image in enumerate(star_block.find_loop("_rlnReferenceImage")):
                size = float(list(star_block.find_loop("_rlnClassDistribution"))[i])
                resolution = float(
                    list(star_block.find_loop("_rlnEstimatedResolution"))[i]
                )
                if ref_resolution is None or resolution < ref_resolution:
                    ref_resolution = resolution
                    ref_size = size
                    ref = image
                elif resolution == ref_resolution and size > ref_size:
                    ref_resolution = resolution
                    ref_size = size
                    ref = image
            return ref.split("@")[-1], float(star_doc[0].find_value("_rlnPixelSize"))
        except Exception as e:
            print(e)
            return None, None

    @functools.lru_cache(maxsize=1)
    def _best_class_fsc(
        self,
        angpix: float,
        boxsize: int,
        job: str = "relion.initialmodel",
        batch: str = "",
    ) -> Tuple[Optional[str], Optional[float]]:
        fsc_files = []
        try:
            model_file_candidates = list(
                (self.path / self.job_paths_batch[job][batch]).glob("*_model.star")
            )
        except (TypeError, KeyError):
            model_file_candidates = list(
                (self.path / self.job_paths[job]).glob("*_model.star")
            )

        def iteration_count(x: pathlib.Path) -> int:
            parts = str(x).split("_")
            for _x in parts:
                if _x.startswith("it"):
                    return int(_x.replace("it", ""))
            return 0

        model_file_candidates = sorted(model_file_candidates, key=iteration_count)
        model_file_candidates.reverse()
        model_file = model_file_candidates[0]
        data_file = pathlib.Path(str(model_file).replace("model", "data"))
        try:
            star_doc = cif.read_file(os.fspath(model_file))
            block_num = None
            for block_index, block in enumerate(star_doc):
                if list(block.find_loop("_rlnReferenceImage")):
                    block_num = block_index
                    break
            if block_num is None:
                return None, None
            star_block = star_doc[block_num]
        except Exception:
            return None, None

        mask_outer_radius = math.floor(0.98 * self.options.mask_diameter / (2 * angpix))
        self.job_paths["relion.external.mask_soft_edge"] = self.fresh_job(
            "relion.external",
            extra_params={
                "fn_exe": "external_job_mask_soft_edge",
                "param1_label": "box_size",
                "param1_value": str(boxsize),
                "param2_label": "angpix",
                "param2_value": str(angpix),
                "param3_label": "outer_radius",
                "param3_value": str(mask_outer_radius),
            },
            alias="MaskSoftEdge",
            lock=self._lock,
        )
        for iclass in range(1, len(star_block.find_loop("_rlnReferenceImage")) + 1):
            self.job_paths[
                f"relion.external.select_and_split_{iclass}"
            ] = self.fresh_job(
                "relion.external",
                extra_params={
                    "fn_exe": "external_job_select_and_split",
                    "in_mic": str(data_file.relative_to(self.path)),
                    "param1_label": "in_dir",
                    "param1_value": f"{self.job_paths[job]}",
                    "param2_label": "outfile",
                    "param2_value": f"particles_class{iclass}.star",
                    "param3_label": "class_number",
                    "param3_value": str(iclass),
                },
                alias=f"SelectAndSplit_{iclass}",
                lock=self._lock,
            )
            self.job_paths[
                f"relion.external.reconstruct_halves_{iclass}"
            ] = self.fresh_job(
                "relion.external",
                extra_params={
                    "fn_exe": "external_job_reconstruct_halves",
                    "in_mic": f"External/SelectAndSplit_{iclass}/particles_class{iclass}.star",
                    "param1_label": "in_dir",
                    "param1_value": f"External/SelectAndSplit_{iclass}",
                    "param2_label": "i",
                    "param2_value": f"particles_class{iclass}.star",
                    "param3_label": "mask_diameter",
                    "param3_value": f"{self.options.mask_diameter}",
                    "param4_label": "class_number",
                    "param4_value": str(iclass),
                    "do_queue": "Yes",
                    "qsubscript": self.options.queue_submission_template_cpu_smp,
                },
                alias=f"ReconstructHalves_{iclass}",
                lock=self._lock,
            )
            self.job_paths[f"relion.postprocess_{iclass}"] = self.fresh_job(
                "relion.postprocess",
                extra_params={
                    "fn_mask": "External/MaskSoftEdge/mask.mrc",
                    "fn_in": f"External/ReconstructHalves_{iclass}/3d_half1_model{iclass}.mrc",
                    "angpix": str(angpix),
                    "do_queue": "Yes",
                    "qsubscript": self.options.queue_submission_template_cpu_smp,
                },
                alias=f"GetFSC_{iclass}",
                lock=self._lock,
            )
            fsc_files.append(f"PostProcess/GetFSC_{iclass}/postprocess.star")

        self.job_paths["relion.external.fsc_fitting"] = self.fresh_job(
            "relion.external",
            extra_params={
                "fn_exe": "external_job_fsc_fitting",
                "param1_label": "i",
                "param1_value": " ".join(fsc_files),
            },
            alias="FSCFitting",
            lock=self._lock,
        )
        with open(
            self.job_paths["relion.external.fsc_fitting"] / "BestClass.txt", "r"
        ) as f:
            class_index = int(f.readline())
        split_ref_img = list(star_block.find_loop("_rlnReferenceImage"))[
            class_index
        ].split("@")
        return (
            split_ref_img[-1],
            float(star_doc[0].find_value("_rlnPixelSize")),
        )

    def _new_movies(self) -> bool:
        num_movies = len(
            [
                m
                for m in self.movies_path.glob("**/*")
                if m.suffix == "." + self.movietype
            ]
        )
        return not num_movies == self._num_seen_movies

    def _classification_3d(
        self,
        angpix: Optional[float] = None,
        boxsize: Optional[int] = None,
        iteration: int = 0,
    ):
        while True:
            try:
                batch_file = self._queues["class3D"][iteration].get()
                if not batch_file:
                    return
                if self.job_paths.get("relion.initialmodel") is None:
                    self.job_paths["relion.initialmodel"] = self.fresh_job(
                        "relion.initialmodel",
                        extra_params={"fn_img": batch_file},
                        lock=self._lock,
                    )
                if not self.job_paths_batch.get("relion.class3d"):
                    self.job_paths_batch["relion.class3d"] = {}
                if self.options.use_fsc_criterion and angpix is None:
                    raise ValueError(
                        "use_fsc_criterion is True but angpix has not been specified"
                    )
                self.job_paths_batch["relion.class3d"][batch_file] = self.fresh_job(
                    "relion.class3d",
                    extra_params={
                        "fn_img": batch_file,
                        "fn_ref": self._best_class_fsc(angpix, boxsize)[0]
                        if self.options.use_fsc_criterion
                        else self._best_class()[0],
                    },
                    lock=self._lock,
                )
            except (AttributeError, FileNotFoundError) as e:
                print(
                    f"Exception encountered in 3D classification runner. Try again: {e}"
                )

    def classification(
        self,
        angpix: Optional[float] = None,
        boxsize: Optional[int] = None,
        iteration: int = 0,
    ):
        first_batch = ""
        class3d_thread = None
        while True:
            try:
                batch_file = self._queues["class2D"][iteration].get()
                if not batch_file:
                    if class3d_thread is None:
                        return
                    self._queues["class3D"][iteration].put("")
                    class3d_thread.join()
                    return
                if batch_file == "__kill__":
                    if class3d_thread is None:
                        return
                    _clear_queue(self._queues["class3D"][iteration])
                    self._queues["class3D"][iteration].put("")
                    class3d_thread.join()
                    return
                if self.options.do_class2d_vdam:
                    class2d_type = "relion.class2d.vdam"
                else:
                    class2d_type = "relion.class2d.em"
                if not self.job_paths_batch.get(class2d_type):
                    first_batch = batch_file
                    self.job_paths_batch[class2d_type] = {}
                    self.job_paths_batch[class2d_type][batch_file] = self.fresh_job(
                        class2d_type,
                        extra_params={"fn_img": batch_file},
                        lock=self._lock,
                    )

                    if self._past_class_threshold and self.options.do_class3d:
                        class3d_thread = threading.Thread(
                            target=self._classification_3d,
                            name="3D_classification_runner",
                            kwargs={
                                "iteration": iteration,
                                "angpix": angpix,
                                "boxsize": boxsize,
                            },
                        )
                        class3d_thread.start()
                        self._queues["class3D"][iteration].put(first_batch)
                elif self.job_paths_batch[class2d_type].get(batch_file):
                    if self._lock:
                        with self._lock:
                            self.project.run_job(
                                f"{class2d_type.replace('.', '_')}_job.star",
                                overwrite=str(
                                    self.job_paths_batch[class2d_type][batch_file]
                                ),
                                wait_for_queued=False,
                            )
                        runner = JobRunner(self.project.pipeline.name)
                        runner.wait_for_queued_job_completion(
                            str(self.job_paths_batch[class2d_type][batch_file])
                        )
                    else:
                        self.project.run_job(
                            f"{class2d_type.replace('.', '_')}_job.star",
                            overwrite=str(
                                self.job_paths_batch[class2d_type][batch_file]
                            ),
                            wait_for_queued=True,
                        )
                    if self._past_class_threshold and self.options.do_class3d:
                        class3d_thread = threading.Thread(
                            target=self._classification_3d,
                            name="3D_classification_runner",
                            kwargs={
                                "iteration": iteration,
                                "angpix": angpix,
                                "boxsize": boxsize,
                            },
                        )
                        class3d_thread.start()
                        self._queues["class3D"][iteration].put(first_batch)
                else:
                    if self.options.do_class3d and class3d_thread is None:
                        class3d_thread = threading.Thread(
                            target=self._classification_3d,
                            name="3D_classification_runner",
                            kwargs={
                                "iteration": iteration,
                                "angpix": angpix,
                                "boxsize": boxsize,
                            },
                        )
                        class3d_thread.start()
                        self._queues["class3D"][iteration].put(first_batch)
                    self.job_paths_batch[class2d_type][batch_file] = self.fresh_job(
                        class2d_type,
                        extra_params={"fn_img": batch_file},
                        lock=self._lock,
                    )

                    if self.options.do_class3d:
                        self._queues["class3D"][iteration].put(batch_file)
            except (AttributeError, FileNotFoundError) as e:
                print(
                    f"Exception encountered in 2D classification runner. Try again: {e}"
                )

    def ib_group(self, iteration: int = 0):
        while True:
            try:
                batch_file = self._queues["ib_group"][iteration].get()
                if not batch_file:
                    return
                if not self.job_paths_batch.get(
                    "icebreaker.micrograph_analysis.particles"
                ):
                    self.job_paths_batch[
                        "icebreaker.micrograph_analysis.particles"
                    ] = {}
                self.job_paths_batch["icebreaker.micrograph_analysis.particles"][
                    batch_file
                ] = self.fresh_job(
                    "icebreaker.micrograph_analysis.particles",
                    extra_params={
                        "in_mics": str(
                            self.job_paths["icebreaker.micrograph_analysis.micrographs"]
                            / "grouped_micrographs.star"
                        ),
                        "in_parts": batch_file,
                    },
                    lock=self._lock,
                )
            except (AttributeError, FileNotFoundError) as e:
                print(f"Exception encountered in IceBreaker runner. Try again: {e}")

    def run(self, timeout: int):
        start_time = time.time()
        current_time = start_time
        class_thread = None
        ib_thread = None
        class_thread_second_pass = None
        ib_thread_second_pass = None
        ref3d: str = ""
        ref3d_angpix: float = 1
        iteration = 0
        old_iteration = 0
        first_batch = ""
        continue_anyway = False
        while (
            (current_time - start_time < timeout and not self.stopfile.exists())
            or continue_anyway
            or self._new_movies()
        ):
            if self._new_movies() or iteration - old_iteration:
                if iteration - old_iteration:
                    continue_anyway = False
                try:
                    split_files = self.preprocessing(
                        ref3d=ref3d, ref3d_angpix=ref3d_angpix
                    )
                except (AttributeError, FileNotFoundError) as e:
                    print(f"Exception encountered in preprocessing. Try again: {e}")
                    continue
                if not first_batch:
                    first_batch = split_files[0]
                if self.options.do_icebreaker_group:
                    if ib_thread is None and not iteration:
                        ib_thread = threading.Thread(
                            target=self.ib_group,
                            name="ib_group_runner",
                            kwargs={"iteration": iteration},
                        )
                        ib_thread.start()
                    elif ib_thread_second_pass is None and iteration:
                        ib_thread_second_pass = threading.Thread(
                            target=self.ib_group,
                            name="ib_group_runner_second_pass",
                            kwargs={"iteration": iteration},
                        )
                        ib_thread_second_pass.start()
                    new_batches = [
                        f for f in split_files if f not in self._passes[iteration]
                    ]
                    for sf in new_batches:
                        self._queues["ib_group"][iteration].put(sf)
                if self.options.do_class2d:
                    if class_thread is None and not iteration:
                        curr_angpix = (
                            self.options.angpix * self.options.motioncor_binning
                        )
                        bcb = int(
                            self.options.extract_boxsize
                            / self.options.motioncor_binning
                        )
                        curr_boxsize = bcb + bcb % 2
                        if self.options.extract_downscale:
                            curr_angpix *= (
                                self.options.extract_boxsize
                                / self.options.extract_small_boxsize
                            )
                            curr_boxsize = self.options.extract_small_boxsize
                        class_thread = threading.Thread(
                            target=self.classification,
                            name="classification_runner",
                            kwargs={
                                "iteration": iteration,
                                "angpix": curr_angpix,
                                "boxsize": curr_boxsize,
                            },
                        )
                        class_thread.start()
                    elif class_thread_second_pass is None and iteration:
                        curr_angpix = (
                            self.options.angpix * self.options.motioncor_binning
                        )
                        bcb = int(
                            self.options.extract_boxsize
                            / self.options.motioncor_binning
                        )
                        curr_boxsize = bcb + bcb % 2
                        if self.options.extract2_downscale:
                            curr_angpix *= (
                                self.options.extract_boxsize
                                / self.options.extract2_small_boxsize
                            )
                            curr_boxsize = self.options.extract2_small_boxsize
                        class_thread_second_pass = threading.Thread(
                            target=self.classification,
                            name="classification_runner_second_pass",
                            kwargs={
                                "iteration": iteration,
                                "angpix": curr_angpix,
                                "boxsize": curr_boxsize,
                            },
                        )
                        class_thread_second_pass.start()
                    if len(split_files) == 1:
                        self._queues["class2D"][iteration].put(split_files[0])
                        self._passes[iteration].update(split_files)
                    else:
                        new_batches = [
                            f for f in split_files if f not in self._passes[iteration]
                        ]
                        if not self._past_class_threshold:
                            self._past_class_threshold = True
                        for sf in new_batches:
                            self._queues["class2D"][iteration].put(sf)
                        self._passes[iteration].update(new_batches)
                old_iteration = iteration
                if (
                    self.options.do_second_pass
                    and self.options.do_class2d
                    and self.options.do_class3d
                    and not self.options.autopick_do_cryolo
                    and not iteration
                ):
                    self._queues["ib_group"][0].put("")
                    self._queues["class2D"][0].put("")
                    if ib_thread:
                        ib_thread.join()
                    if class_thread:
                        class_thread.join()
                    _ref3d, _ref3d_angpix = self._best_class(
                        "relion.class3d", first_batch
                    )
                    ref3d = _ref3d or ""
                    ref3d_angpix = _ref3d_angpix or ref3d_angpix
                    new_angpix = self.options.angpix * self.options.motioncor_binning
                    if self.options.extract2_downscale:
                        new_angpix *= (
                            self.options.extract_boxsize
                            / self.options.extract2_small_boxsize
                        )

                    if abs(new_angpix - float(self.options.autopick_ref_angpix)) > 1e-3:
                        command = [
                            "relion_image_handler",
                            "--i",
                            str(ref3d),
                            "--o",
                            str("ref3d"),
                            "--angpix",
                            str(ref3d_angpix),
                            "--rescale_angpix",
                            str(new_angpix),
                            "--new_box",
                            str(
                                self.options.extract2_small_boxsize
                                if self.options.extract2_downscale
                                else self.options.extract_boxsize
                            ),
                        ]
                        ref3d = ref3d.replace(".mrcs", "_ref3d.mrcs")
                        subprocess.run(command)
                    iteration = 1
                    continue_anyway = True

            time.sleep(10)
            current_time = time.time()
        if ib_thread is not None:
            self._queues["ib_group"][0].put("")
        if class_thread is not None:
            self._queues["class2D"][0].put("")
        if ib_thread is not None:
            ib_thread.join()
        if class_thread is not None:
            class_thread.join()

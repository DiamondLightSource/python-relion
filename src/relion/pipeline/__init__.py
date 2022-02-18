import functools
import os
import pathlib
import queue
import subprocess
import threading
import time
from typing import Dict, List, Optional, Set, Tuple, Union

from gemmi import cif
from pipeliner.api.api_utils import (
    edit_jobstar,
    job_parameters_dict,
    write_default_jobstar,
)
from pipeliner.api.manage_project import PipelinerProject
from pipeliner.job_runner import JobRunner

from relion.cryolo_relion_it.cryolo_relion_it import RelionItOptions
from relion.pipeline.options import generate_pipeline_options


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
        self.job_paths: Dict[str, Union[str, dict]] = {}
        self._past_class_threshold = False
        self._class2d_queue: List[queue.Queue] = [queue.Queue()]
        self._class3d_queue: List[queue.Queue] = [queue.Queue()]
        self._ib_group_queue: List[queue.Queue] = [queue.Queue()]
        self._batches: List[Set[str]] = [set(), set()]
        self._num_seen_movies = 0
        self._lock = threading.RLock()

    def _generate_pipeline_options(self):
        pipeline_jobs = {
            "relion.import.movies": "",
            "relion.motioncorr.motioncorr2": "gpu",
            "icebreaker.analysis.micrographs": "cpu",
            "icebreaker.enhancecontrast": "cpu",
            "relion.ctffind.ctffind4": "gpu",
            "relion.autopick.log": "gpu",
            "relion.autopick.ref3d": "gpu",
            "cryolo.autopick": "gpu",
            "relion.extract": "gpu",
            "relion.select.split": "",
            "icebreaker.analysis.particles": "cpu",
            "relion.class2d.em": "gpu",
            "relion.initialmodel": "gpu-smp",
            "relion.class3d": "gpu-smp",
        }
        return generate_pipeline_options(self.options, pipeline_jobs)

    def _extra_options(self, job: str) -> dict:
        if job == "relion.motioncorr.motioncorr2":
            return {
                "input_star_mics": self.job_paths["relion.import.movies"]
                + "/movies.star"
            }
        if job == "icebreaker.analysis.micrographs":
            return {
                "in_mics": self.job_paths["relion.motioncorr.motioncorr2"]
                + "corrected_micrographs.star"
            }
        if job == "icebreaker.enhancecontrast":
            return {
                "in_mics": self.job_paths["relion.motioncorr.motioncorr2"]
                + "corrected_micrographs.star"
            }
        if job == "relion.ctffind.ctffind4":
            if self.options.images_are_movies:
                if self.job_paths.get("relion.motioncorr.motioncorr2"):
                    return {
                        "input_star_mics": self.job_paths[
                            "relion.motioncorr.motioncorr2"
                        ]
                        + "/corrected_micrographs.star"
                    }
                else:
                    return {
                        "input_star_mics": self.job_paths["relion.motioncorr.own"]
                        + "/corrected_micrographs.star"
                    }
            else:
                return {
                    "input_star_mics": self.job_paths["relion.import.movies"]
                    + "/micrographs.star"
                }
        if job == "relion.autopick.log" or job == "relion.autopick.ref3d":
            if self.options.use_ctffind_instead:
                return {
                    "fn_input_autopick": self.job_paths["relion.ctffind.ctffind4"]
                    + "/micrographs_ctf.star"
                }
            return {
                "fn_input_autopick": self.job_paths["relion.ctffind.gctf"]
                + "/micrographs_ctf.star"
            }
        if job == "cryolo.autopick":
            if self.options.use_ctffind_instead:
                return {
                    "input_file": self.job_paths["relion.ctffind.ctffind4"]
                    + "/micrographs_ctf.star"
                }
            return {
                "input_file": self.job_paths["relion.ctffind.gctf"]
                + "/micrographs_ctf.star"
            }
        if job.startswith("relion.extract"):
            ref = job.replace("relion.extract", "")
            if self.options.autopick_do_cryolo:
                coords = (
                    self.job_paths["cryolo.autopick"] + "/coords_suffix_autopick.star"
                )
            else:
                if ref:
                    coords = self.job_paths["relion.autopick.ref3d"] + "/autopick.star"
                else:
                    coords = self.job_paths["relion.autopick.log"] + "/autopick.star"
            if self.options.use_ctffind_instead:
                star_mics = (
                    self.job_paths["relion.ctffind.ctffind4"] + "/micrographs_ctf.star"
                )
            else:
                star_mics = (
                    self.job_paths["relion.ctffind.gctf"] + "/micrographs_ctf.star"
                )
            return {
                "coords_suffix": coords,
                "star_mics": star_mics,
            }
        if job.startswith("relion.select.split"):
            ref = job.replace("relion.select.split", "")
            return {
                "fn_data": self.job_paths["relion.extract" + ref] + "/particles.star"
            }
        return {}

    def fresh_job(
        self,
        job: str,
        extra_params: Optional[dict] = None,
        wait: bool = True,
        lock: Optional[threading.RLock] = None,
    ) -> str:
        write_default_jobstar(job)
        params = job_parameters_dict(job)
        params.update(self.pipeline_options.get(job, {}))
        if extra_params is not None:
            params.update(extra_params)
        params = {k: str(v) for k, v in params.items() if not isinstance(v, bool)}
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
        else:
            with lock:
                job_path = self.project.run_job(
                    f"{job.replace('.', '_')}_job.star",
                    wait_for_queued=False,
                )
            runner = JobRunner(self.project.pipeline.name)
            runner.wait_for_queued_job_completion(job_path)
        return job_path

    def _get_split_files(self, select_job: str) -> List[str]:
        all_split_files = list(pathlib.Path(select_job).glob("*particles_split*.star"))
        if len(all_split_files) == 1:
            return [str(all_split_files[0])]
        # drop the most recent batch if there is more than one as it probably isn't complete
        def batch(fname: str) -> int:
            spfname = fname.split("particles_split")
            return int(spfname[-1].replace(".star", ""))

        with_batch_numbers = [(batch(str(f)), str(f)) for f in all_split_files]
        sorted_batch_numbers = sorted(with_batch_numbers, key=lambda x: x[0])
        return [s[1] for s in sorted_batch_numbers[:-1]]

    def _get_num_movies(self, star_file: str) -> int:
        star_doc = cif.read_file(os.fspath(star_file))
        return len(list(star_doc[1].find_loop("_rlnMicrographMovieName")))

    def preprocessing(self, ref3d: str = "", ref3d_angpix: float = -1) -> List[str]:
        if ref3d and self.options.autopick_do_cryolo:
            return []

        jobs = ["relion.import.movies", "relion.motioncorr.motioncorr2"]
        if self.options.do_icebreaker_job_group:
            jobs.append("icebreaker.analysis.micrographs")
        if self.options.do_icebreaker_job_flatten:
            jobs.append("icebreaker.enhancecontrast")
        jobs.append("relion.ctffind.ctffind4")

        for job in jobs:
            if not self.job_paths.get(job):
                self.job_paths[job] = self.fresh_job(
                    job,
                    extra_params=self._extra_options(job),
                    lock=self._lock,
                )
            else:
                self.project.continue_job(self.job_paths[job])
        self._num_seen_movies = self._get_num_movies(
            self.job_paths["relion.import.movies"] + "/movies.star"
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
                            **self._extra_options(job),
                            **{"fn_ref3d_autopick": ref3d, "angpix_ref": ref3d_angpix},
                        },
                        lock=self._lock,
                    )
                else:
                    self.job_paths[job] = self.fresh_job(
                        job.replace(ref3d, ""),
                        extra_params=self._extra_options(job),
                        lock=self._lock,
                    )
            else:
                self.project.continue_job(self.job_paths[job])
        return self._get_split_files(self.job_paths["relion.select.split" + ref3d])

    @functools.lru_cache(maxsize=1)
    def _best_class(
        self, job: str = "relion.initialmodel", batch: str = ""
    ) -> Tuple[Optional[str], Optional[float]]:
        if isinstance(self.job_paths[job], dict):
            model_file_candidates = list(
                (self.path / self.job_paths[job][batch]).glob("*_model.star")
            )

            def iteration_count(x: pathlib.Path) -> int:
                parts = str(x).split("_")
                for _x in parts:
                    if _x.startswith("it"):
                        return int(_x.replace("it", ""))

            model_file_candidates = sorted(model_file_candidates, key=iteration_count)
            model_file_candidates.reverse()
        else:
            model_file_candidates = list(
                (self.path / self.job_paths[job]).glob("*_model.star")
            )
            if len(model_file_candidates) != 1:
                return None, None
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
            ref_size = 0
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

    def _new_movies(self) -> bool:
        num_movies = len(
            [
                m
                for m in self.movies_path.glob("**/*")
                if m.suffix == "." + self.movietype
            ]
        )
        return not num_movies == self._num_seen_movies

    def _classification_3d(self, iteration: int = 0):
        while True:
            batch_file = self._class3d_queue[iteration].get()
            if not batch_file:
                return
            if self.job_paths.get("relion.initialmodel") is None:
                self.job_paths["relion.initialmodel"] = self.fresh_job(
                    "relion.initialmodel",
                    extra_params={"fn_img": batch_file},
                    lock=self._lock,
                )
            if not self.job_paths.get("relion.class3d"):
                self.job_paths["relion.class3d"] = {}
            self.job_paths["relion.class3d"][batch_file] = self.fresh_job(
                "relion.class3d",
                extra_params={
                    "fn_img": batch_file,
                    "fn_ref": self._best_class()[0],
                },
                lock=self._lock,
            )

    def classification(self, iteration: int = 0):
        first_batch = ""
        class3d_thread = None
        while True:
            batch_file = self._class2d_queue[iteration].get()
            if not batch_file:
                if class3d_thread is None:
                    return
                self._class3d_queue[iteration].put("")
                class3d_thread.join()
                return
            if batch_file == "__kill__":
                if class3d_thread is None:
                    return
                self._class3d_queue[iteration].clear()
                self._class3d_queue[iteration].put("")
                class3d_thread.join()
                return
            if not self.job_paths.get("relion.class2d.em"):
                first_batch = batch_file
                self.job_paths["relion.class2d.em"] = {}
                self.job_paths["relion.class2d.em"][batch_file] = self.fresh_job(
                    "relion.class2d.em",
                    extra_params={"fn_img": batch_file},
                    lock=self._lock,
                )
                if self._past_class_threshold and self.options.do_class3d:
                    class3d_thread = threading.Thread(
                        target=self._classification_3d,
                        name="3D_classification_runner",
                        kwargs={"iteration": iteration},
                    )
                    class3d_thread.start()
                    self._class3d_queue[iteration].put(first_batch)
            elif self.job_paths["relion.class2d.em"].get(batch_file):
                self.project.continue_job(
                    self.job_paths["relion.class2d.em"].get(batch_file)
                )
                if self._past_class_threshold and self.options.do_class3d:
                    class3d_thread = threading.Thread(
                        target=self._classification_3d,
                        name="3D_classification_runner",
                        kwargs={"iteration": iteration},
                    )
                    class3d_thread.start()
                    self._class3d_queue[iteration].put(first_batch)
            else:
                if self.options.do_class3d and class3d_thread is None:
                    class3d_thread = threading.Thread(
                        target=self._classification_3d,
                        name="3D_classification_runner",
                        kwargs={"iteration": iteration},
                    )
                    class3d_thread.start()
                    self._class3d_queue[iteration].put(first_batch)
                self.job_paths["relion.class2d.em"][batch_file] = self.fresh_job(
                    "relion.class2d.em",
                    extra_params={"fn_img": batch_file},
                    lock=self._lock,
                )
                if self.options.do_class3d:
                    self._class3d_queue[iteration].put(batch_file)

    def ib_group(self, iteration: int = 0):
        while True:
            batch_file = self._ib_group_queue[iteration].get()
            if not batch_file:
                return
            if not self.job_paths.get("icebreaker.analysis.particles"):
                self.job_paths["icebreaker.analysis.particles"] = {}
            self.job_paths["icebreaker.analysis.particles"][
                batch_file
            ] = self.fresh_job(
                "icebreaker.analysis.particles",
                extra_params={
                    "in_mics": self.job_paths["icebreaker.analysis.micrographs"]
                    + "grouped_micrographs.star",
                    "in_parts": batch_file,
                },
                lock=self._lock,
            )

    def run(self, timeout: int):
        start_time = time.time()
        current_time = start_time
        class_thread = None
        ib_thread = None
        class_thread_second_pass = None
        ib_thread_second_pass = None
        ref3d = ""
        ref3d_angpix = 1
        iteration = 0
        old_iteration = 0
        first_batch = ""
        continue_anyway = False
        while (
            current_time - start_time < timeout and not self.stopfile.exists()
        ) or continue_anyway:
            if self._new_movies() or iteration - old_iteration:
                if iteration - old_iteration:
                    continue_anyway = False
                split_files = self.preprocessing(ref3d=ref3d, ref3d_angpix=ref3d_angpix)
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
                        f for f in split_files if f not in self._batches[iteration]
                    ]
                    for sf in new_batches:
                        self._ib_group_queue[iteration].put(sf)
                if self.options.do_class2d:
                    if class_thread is None and not iteration:
                        class_thread = threading.Thread(
                            target=self.classification,
                            name="classification_runner",
                            kwargs={"iteration": iteration},
                        )
                        class_thread.start()
                    elif class_thread_second_pass is None and iteration:
                        class_thread_second_pass = threading.Thread(
                            target=self.classification,
                            name="classification_runner_second_pass",
                            kwargs={"iteration": iteration},
                        )
                        class_thread_second_pass.start()
                    if len(split_files) == 1:
                        self._class2d_queue[iteration].put(split_files[0])
                        self._batches[iteration].update(split_files)
                    else:
                        new_batches = [
                            f for f in split_files if f not in self._batches[iteration]
                        ]
                        if not self._past_class_threshold:
                            self._past_class_threshold = True
                        for sf in new_batches:
                            self._class2d_queue[iteration].put(sf)
                        self._batches[iteration].update(new_batches)
                old_iteration = iteration
                if (
                    self.options.do_second_pass
                    and self.options.do_class2d
                    and self.options.do_class3d
                    and not self.options.autopick_do_cryolo
                    and not iteration
                ):
                    self._ib_group_queue[0].clear()
                    self._ib_group_queue[0].put("")
                    self._class2d_queue[0].clear()
                    self._class2d_queue[0].put("__kill__")
                    ib_thread.join()
                    class_thread.join()
                    ref3d, ref3d_angpix = self._best_class(
                        "relion.class3d", first_batch
                    )
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
            self._ib_group_queue[0].put("")
        if class_thread is not None:
            self._class2d_queue[0].put("")
        if ib_thread is not None:
            ib_thread.join()
        if class_thread is not None:
            class_thread.join()

import functools
import os
import pathlib
import queue
import threading
import time
from typing import Dict, List, Optional, Set, Union

from gemmi import cif
from pipeliner.api.api_utils import (
    edit_jobstar,
    job_parameters_dict,
    write_default_jobstar,
)
from pipeliner.api.manage_project import PipelinerProject
from pipeliner.job_runner import JobRunner

from relion.cryolo_relion_it.cryolo_relion_it import RelionItOptions


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
        self.movietype = movietype
        self.project = PipelinerProject()
        self.stopfile = stopfile
        self.options = options
        self.pipeline_options: Dict[str, dict] = self._generate_pipeline_options()
        self.job_paths: Dict[str, Union[str, dict]] = {}
        self._past_class_threshold = False
        self._class2d_queue: List[queue.Queue] = [queue.Queue()]
        self._class3d_queue: List[queue.Queue] = [queue.Queue()]
        self._ib_group_queue: List[queue.Queue] = [queue.Queue()]
        self._batches: Set[str] = set()
        self._num_seen_movies = 0
        self._lock = threading.RLock()

    def _generate_pipeline_options(self):
        import_options = {
            "fn_in_raw": self.options.import_images,
            "angpix": self.options.angpix,
        }

        queue_options = {
            "do_queue": "Yes",
            "qsubscript": self.options.queue_submission_template,
        }
        queue_options_cpu = {
            "do_queue": "Yes",
            "qsubscript": self.options.queue_submission_template_cpu_smp,
        }

        motioncorr_options = {
            "fn_motioncor2_exe": self.options.motioncor_exe,
            "fn_defect": self.options.motioncor_defectfile,
            "dose_per_frame": self.options.motioncor_doseperframe,
            "fn_gain_ref": self.options.motioncor_gainreference,
            "eer_grouping": self.options.eer_grouping,
            "patch_x": self.options.motioncor_patches_x,
            "patch_y": self.options.motioncor_patches_y,
            "bfactor": self.options.motioncor_bfactor,
            "bin_factor": self.options.motioncor_binning,
            "gain_flip": self.options.motioncor_gainflip,
            "gain_rot": self.options.motioncor_gainrot,
            "other_motioncor2_args": self.options.motioncor2_other_args,
            "gpu_ids": "0:1:2:3",
            "nr_mpi": self.options.motioncor_mpi,
            "nr_threads": self.options.motioncor_threads,
        }
        if self.options.motioncor_submit_to_queue:
            motioncorr_options.update(queue_options)
            motioncorr_options["queuename"] = "motioncorr2_gpu"

        icebreaker_options = {
            "nr_threads": self.options.icebreaker_threads_number,
            "nr_mpi": 1,
        }
        icebreaker_options.update(queue_options_cpu)
        icebreaker_options["queuename"] = "icebreaker"

        ctffind_options = {
            "dast": self.options.ctffind_astigmatism,
            "box": self.options.ctffind_boxsize,
            "dfmax": self.options.ctffind_defocus_max,
            "dfmin": self.options.ctffind_defocus_min,
            "dfstep": self.options.ctffind_defocus_step,
            "resmax": self.options.ctffind_maxres,
            "resmin": self.options.ctffind_minres,
            "gpu_ids": "0:1:2:3",
            "nr_mpi": self.options.ctffind_mpi,
        }
        if self.options.ctffind_submit_to_queue:
            ctffind_options.update(queue_options)
            ctffind_options["queuename"] = "ctffind_gpu"

        autopick_LoG_options = {
            "log_diam_min": self.options.autopick_LoG_diam_min,
            "log_diam_max": self.options.autopick_LoG_diam_max,
            "log_maxres": self.options.autopick_lowpass,
            "log_adjust_thr": self.options.autopick_LoG_adjust_threshold,
            "log_upper_thr": self.options.autopick_LoG_upper_threshold,
            "gpu_ids": "0:1:2:3",
            "nr_mpi": self.options.autopick_mpi,
        }
        if self.options.autopick_submit_to_queue:
            autopick_LoG_options.update(queue_options)
            autopick_LoG_options["queuename"] = "relion_autopick_gpu"

        autopick_3dref_options = {
            "ref3d_symmetry": self.options.autopick_3dref_symmetry,
            "ref3d_sampling": self.options.autopick_3dref_sampling,
            "lowpass": self.options.autopick_lowpass,
            "angpix_ref": self.options.autopick_ref_angpix,
            "threshold_autopick": self.options.autopick_refs_threshold,
            "mindist_autopick": self.options.autopick_refs_min_distance,
            "maxstddevnoise_autopick": self.options.autopick_stddev_noise,
            "minavgnoise_autopick": self.options.autopick_avg_noise,
        }
        if self.options.autopick_submit_to_queue:
            autopick_3dref_options.update(queue_options)
            autopick_3dref_options["queuename"] = "relion_autopick_gpu"

        cryolo_options = {
            "model_path": self.options.cryolo_gmodel,
            "box_size": int(
                self.options.extract_boxsize / self.options.motioncor_binning
            ),
            "confidence_threshold": self.options.cryolo_threshold,
        }
        if self.options.cryolo_submit_to_queue:
            cryolo_options.update(queue_options)
            cryolo_options["queuename"] = "cryolo_gpu"

        bin_corrected_box_size = int(
            self.options.extract_boxsize / self.options.motioncor_binning
        )
        extract_options = {
            "bg_diameter": self.options.extract_bg_diameter,
            "extract_size": bin_corrected_box_size + bin_corrected_box_size % 2,
            "do_rescale": bool(self.options.extract_downscale),
            "rescale": self.options.extract_small_boxsize,
            "nr_mpi": self.options.extract_mpi,
        }
        if self.options.extract_submit_to_queue:
            extract_options.update(queue_options)
            extract_options["queuename"] = "relion_extract"

        select_options = {
            "split_size": self.options.batch_size,
        }

        class2d_options = {
            "nr_classes": self.options.class2d_nr_classes,
            "do_em": "Yes",
            "do_grad": "No",
            "nr_iter_em": self.options.class2d_nr_iter,
            "psi_sampling": self.options.class2d_angle_step,
            "offset_range": self.options.class2d_offset_range,
            "offset_step": self.options.class2d_offset_step,
            "ctf_intact_first_peak": self.options.class2d_ctf_ign1stpeak,
            "do_preread_images": self.options.refine_preread_images,
            "scratch_dir": self.options.refine_scratch_disk,
            "nr_pool": self.options.refine_nr_pool,
            "use_gpu": self.options.refine_do_gpu,
            "gpu_ids": "0:1:2:3",
            "nr_mpi": self.options.refine_mpi,
            "nr_threads": self.options.refine_threads,
        }
        if self.options.refine_submit_to_queue:
            class2d_options.update(queue_options)
            class2d_options["queuename"] = "class2d_gpu"

        inimodel_options = {
            "nr_classes": self.options.inimodel_nr_classes,
            "sampling": self.options.inimodel_angle_step,
            "offset_step": self.options.inimodel_offset_step,
            "offset_range": self.options.inimodel_offset_range,
            "nr_iter": self.options.inimodel_nr_iter_inbetween,
            "use_gpu": self.options.refine_do_gpu,
            "gpu_ids": "0:1:2:3",
            "nr_mpi": 1,
            "nr_threads": self.options.refine_threads,
        }
        if self.options.refine_submit_to_queue:
            inimodel_options.update(queue_options)
            inimodel_options["queuename"] = "inimodel_gpu"

        class3d_options = {
            "fn_mask": self.options.class3d_reference,
            "nr_classes": self.options.class3d_nr_classes,
            "sym_name": self.options.symmetry,
            "ini_high": self.options.class3d_ini_lowpass,
            "tau_fudge": self.options.class3d_T_value,
            "particle_diameter": self.options.mask_diameter,
            "nr_iter": self.options.class3d_nr_iter,
            "sampling": self.options.class3d_angle_step,
            "offset_range": self.options.class3d_offset_range,
            "offset_step": self.options.class3d_offset_step,
            "ref_correct_greyscale": self.options.class3d_ref_is_correct_greyscale,
            "do_ctf_correction": self.options.class3d_ref_is_ctf_corrected,
            "ctf_intact_first_peak": self.options.class3d_ctf_ign1stpeak,
            "do_preread_images": self.options.refine_preread_images,
            "gpu_ids": "0:1:2:3",
            "nr_mpi": self.options.refine_mpi,
            "nr_threads": self.options.refine_threads,
        }
        if self.options.refine_submit_to_queue:
            class3d_options.update(queue_options)
            class3d_options["queuename"] = "class3d_gpu"

        return {
            "relion.import.movies": import_options,
            "relion.motioncorr.motioncorr2": motioncorr_options,
            "icebreaker.analysis.micrographs": icebreaker_options,
            "icebreaker.enhancecontrast": icebreaker_options,
            "relion.ctffind.ctffind4": ctffind_options,
            "relion.autopick.log": autopick_LoG_options,
            "relion.autopick.ref3d": autopick_3dref_options,
            "cryolo.autopick": cryolo_options,
            "relion.extract": extract_options,
            "relion.select.split": select_options,
            "icebreaker.analysis.particles": icebreaker_options,
            "relion.class2d": class2d_options,
            "relion.initialmodel": inimodel_options,
            "relion.class3d": class3d_options,
        }

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

    def preprocessing(self, ref3d: str = "") -> List[str]:
        if ref3d and self.options.autopick_do_cryolo:
            return []
        if not ref3d:
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
                        job,
                        extra_params={
                            **self._extra_options(job),
                            **{"fn_ref3d_autopick": ref3d},
                        },
                        lock=self._lock,
                    )
                else:
                    self.job_paths[job] = self.fresh_job(
                        job,
                        extra_params=self._extra_options(job),
                        lock=self._lock,
                    )
            else:
                self.project.continue_job(self.job_paths[job])
        return self._get_split_files(self.job_paths["relion.select.split" + ref3d])

    @functools.lru_cache(maxsize=1)
    def _best_class(
        self, job: str = "relion.initialmodel", batch: str = ""
    ) -> Optional[str]:
        if isinstance(self.job_paths[job], dict):
            model_file_candidates = list(
                (self.path / self.job_paths[job][batch]).glob("*_model.star")
            )
        else:
            model_file_candidates = list(
                (self.path / self.job_paths[job]).glob("*_model.star")
            )
        if len(model_file_candidates) != 1:
            return None
        model_file = model_file_candidates[0]
        try:
            star_doc = cif.read_file(os.fspath(model_file))
            ref = star_doc["model_classes"]["rlnReferenceImage"][0]
            ref_size = 0
            ref_resolution = None
            for i, image in star_doc["model_classes"]["rlnReferenceImage"]:
                size = float(star_doc["model_classes"]["rlnClassDistribution"][i])
                resolution = float(
                    star_doc["model_classes"]["rlnEstimatedResolution"][i]
                )
                if ref_resolution is None or resolution < ref_resolution:
                    ref_resolution = resolution
                    ref_size = size
                    ref = image
                elif resolution == ref_resolution and size > ref_size:
                    ref_resolution = resolution
                    ref_size = size
                    ref = image
            return ref
        except Exception:
            return None

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
            if batch_file == "__terminate__":
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
                    "fn_ref": self._best_class(),
                },
                lock=self._lock,
            )

    def classification(self, iteration: int = 0):
        first_batch = ""
        class3d_thread = None
        while True:
            batch_file = self._class2d_queue[iteration].get()
            if batch_file == "__terminate__":
                if class3d_thread is None:
                    return
                self._class3d_queue[iteration].put("__terminate__")
                class3d_thread.join()
                return
            if not self.job_paths.get("relion.class2d"):
                first_batch = batch_file
                self.job_paths["relion.class2d"] = {}
                self.job_paths["relion.class2d"][batch_file] = self.fresh_job(
                    "relion.class2d",
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
            elif self.job_paths["relion.class2d"].get(batch_file):
                self.project.continue_job(
                    self.job_paths["relion.class2d"].get(batch_file)
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
                self.job_paths["relion.class2d"][batch_file] = self.fresh_job(
                    "relion.class2d",
                    extra_params={"fn_img": batch_file},
                    lock=self._lock,
                )
                if self.options.do_class3d:
                    self._class3d_queue[iteration].put(batch_file)

    def ib_group(self, iteration: int = 0):
        while True:
            batch_file = self._ib_group_queue[iteration].get()
            if batch_file == "__terminate__":
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
        ref3d = ""
        iteration = 0
        first_batch = ""
        while current_time - start_time < timeout and not self.stopfile.exists():
            old_iteration = iteration
            if self._new_movies() or iteration - old_iteration:
                split_files = self.preprocessing(ref3d=ref3d)
                if not first_batch:
                    first_batch = split_files[0]
                if self.options.do_icebreaker_group:
                    if ib_thread is None:
                        ib_thread = threading.Thread(
                            target=self.ib_group,
                            name="ib_group_runner",
                            kwargs={"iteration": iteration},
                        )
                        ib_thread.start()
                    new_batches = [f for f in split_files if f not in self._batches]
                    for sf in new_batches:
                        self._ib_group_queue[0].put(sf)
                if self.options.do_class2d:
                    if class_thread is None:
                        class_thread = threading.Thread(
                            target=self.classification,
                            name="classification_runner",
                            kwargs={"iteration": iteration},
                        )
                        class_thread.start()
                    if len(split_files) == 1:
                        self._class2d_queue[0].put(split_files[0])
                        self._batches.update(split_files)
                    else:
                        new_batches = [f for f in split_files if f not in self._batches]
                        if not self._past_class_threshold:
                            self._past_class_threshold = True
                        for sf in new_batches:
                            self._class2d_queue[0].put(sf)
                        self._batches.update(new_batches)
                if (
                    self.options.do_second_pass
                    and self.options.do_class2d
                    and self.options.do_class3d
                    and not self.options.autopick_do_cryolo
                    and not iteration
                ):
                    self._ib_group_queue[0].put("__terminate__")
                    self._class2d_queue[0].put("__terminate__")
                    ib_thread.join()
                    class_thread.join()
                    ref3d = self._best_class("relion.class3d", first_batch)
                    iteration = 1

            time.sleep(10)
            current_time = time.time()
        if ib_thread is not None:
            self._ib_group_queue[0].put("__terminate__")
        if class_thread is not None:
            self._class2d_queue[0].put("__terminate__")
        if ib_thread is not None:
            ib_thread.join()
        if class_thread is not None:
            class_thread.join()

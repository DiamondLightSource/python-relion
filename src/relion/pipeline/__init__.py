import functools
import os
import pathlib
import queue
import threading
import time
from typing import Dict, Optional, Set, Union

from gemmi import cif
from pipeliner.api.api_utils import (
    edit_jobstar,
    job_parameters_dict,
    write_default_jobstar,
)
from pipeliner.api.manage_project import PipelinerProject

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
        self._class_queue = queue.Queue()
        self._batches: Set[str] = set()
        self._num_seen_movies = 0

    def _generate_pipeline_options(self):
        import_options = {
            "fn_in_raw": self.options.import_images,
            "angpix": self.options.angpix,
        }

        queue_options = {
            "do_queue": "Yes",
            "qsubscript": self.options.queue_submission_template,
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
        }
        if self.options.refine_submit_to_queue:
            class3d_options.update(queue_options)
            class3d_options["queuename"] = "class3d_gpu"

        return {
            "relion.import.movies": import_options,
            "relion.motioncorr.motioncorr2": motioncorr_options,
            "relion.ctffind.ctffind4": ctffind_options,
            "plugin.cryolo": cryolo_options,
            "relion.extract": extract_options,
            "relion.select.split": select_options,
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
        if job == "plugin.cryolo":
            if self.options.use_ctffind_instead:
                return {
                    "input_file": self.job_paths["relion.ctffind.ctffind4"]
                    + "/micrographs_ctf.star"
                }
            return {
                "input_file": self.job_paths["relion.ctffind.gctf"]
                + "/micrographs_ctf.star"
            }
        if job == "relion.extract":
            if self.options.autopick_do_cryolo:
                coords = (
                    self.job_paths["plugin.cryolo"] + "/coords_suffix_autopick.star"
                )
            else:
                coords = (
                    self.job_paths["relion.autopick"] + "/coords_suffix_autopick.star"
                )
            if self.options.use_ctffind_instead:
                star_mics = self.job_paths["relion.ctffind.ctffind4"] + "/particle.star"
            else:
                star_mics = self.job_paths["relion.ctffind.gctf"] + "/particle.star"
            return {
                "coords_suffix": coords,
                "star_mics": star_mics,
            }
        if job == "relion.select.split":
            return {"fn_data": self.job_paths["relion.extract"] + "/particle.star"}
        return {}

    def fresh_job(self, job: str, extra_params: Optional[dict] = None) -> str:
        write_default_jobstar(job)
        params = job_parameters_dict(job)
        params.update(self.pipeline_options[job])
        if extra_params is not None:
            params.update(extra_params)
        params = {k: str(v) for k, v in params.items() if not isinstance(v, bool)}
        edit_jobstar(
            f"{job.replace('.', '_')}_job.star",
            params,
            f"{job.replace('.', '_')}_job.star",
        )
        job_path = self.project.run_job(
            f"{job.replace('.', '_')}_job.star",
            wait_for_queued=True,
        )
        return job_path

    def _get_split_files(self, select_job: str) -> Set[str]:
        all_split_files = list(pathlib.Path(select_job).glob("*particles_split*.star"))
        if len(all_split_files) == 1:
            return {str(all_split_files[0])}
        # drop the most recent batch if there is more than one as it probably isn't complete
        def batch(fname: str) -> int:
            spfname = fname.split("particles_split")
            return int(spfname[-1].replace(".star", ""))

        with_batch_numbers = [(batch(f), f) for f in all_split_files]
        sorted_batch_numbers = sorted(with_batch_numbers, key=lambda x: x[0])
        return {s[1] for s in sorted_batch_numbers[:-1]}

    def _get_num_movies(self, star_file: str) -> int:
        star_doc = cif.read_file(os.fspath(star_file))
        return len(star_doc[1]["_rlnMocrographMovieName"])

    def preprocessing(self) -> Set[str]:
        jobs = [
            "relion.import.movies",
            "relion.motioncorr.motioncorr2",
            "relion.ctffind.ctffind4",
        ]

        for job in jobs:
            if not self.job_paths.get(job):
                self.job_paths[job] = self.fresh_job(
                    job, extra_params=self._extra_options(job)
                )
            else:
                self.project.continue_job(self.job_paths[job])
        self._num_seen_movies = self._get_num_movies(
            self.job_paths["relion.import.movies"] + "/movies.star"
        )
        if self.options.stop_after_ctf_estimation:
            return set()
        if self.options.autopick_do_cryolo:
            next_jobs = ["plugin.cryolo"]
        next_jobs.extend(["relion.extract", "relion.select.split"])
        for job in next_jobs:
            if not self.job_paths.get(job):
                self.fresh_job(job, extra_params=self._extra_options(job))
            else:
                self.project.continue_job(self.job_paths[job])
        return self._get_split_files(self.job_paths["relion.select.split"])

    @property
    @functools.lru_cache(maxsize=1)
    def _best_inimodel_class(self) -> Optional[str]:
        model_file_candidates = list(
            (self.path / self.job_paths["relion.initialmodel"]).glob("*_model.star")
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

    def _wait_for_movies(self, wait_for: int = 10):
        num_movies = len(
            [
                m
                for m in self.movies_path.glob("**/*")
                if m.suffix == "." + self.movietype
            ]
        )
        while num_movies == self._num_seen_movies:
            time.sleep(wait_for)
            num_movies = len(
                [
                    m
                    for m in self.movies_path.glob("**/*")
                    if m.suffix == "." + self.movietype
                ]
            )
        self._num_seen_movies = num_movies

    def classification(self):
        first_batch = ""
        while True:
            batch_file = self._class_queue.get()
            if batch_file == "__terminate__":
                return
            if not self.job_paths.get("relion.class2d"):
                first_batch = batch_file
                self.job_paths["relion.class2d"] = {}
                self.job_paths["relion.class2d"][batch_file] = self.fresh_job(
                    "relion.class2d", extra_params={"fn_img": batch_file}
                )
            elif self.job_paths["relion.class2d"].get(batch_file):
                self.project.continue_job(
                    self.job_paths["relion.class2d"].get(batch_file)
                )
            else:
                if self.options.do_class3d and not self.job_paths.get(
                    "relion.initialmodel"
                ):
                    self.job_paths["relion.initialmodel"] = self.fresh_job(
                        "relion.initialmodel"
                    )
                    self.job_paths["relion.class3d"][first_batch] = self.fresh_job(
                        "relion.class3d",
                        extra_params={
                            "fn_img": batch_file,
                            "fn_ref": self._best_inimodel_class,
                        },
                    )
                self.job_paths["relion.class2d"][batch_file] = self.fresh_class2d_job(
                    "relion.class2d", extra_params={"fn_img": batch_file}
                )
                if self.options.do_class3d:
                    self.job_paths["relion.class3d"][
                        batch_file
                    ] = self.fresh_class3d_job(
                        "relion.class3d",
                        extra_params={
                            "fn_img": batch_file,
                            "fn_ref": self._best_inimodel_class,
                        },
                    )

    def run(self, timeout: int):
        start_time = time.time()
        current_time = start_time
        class_thread = None
        while current_time - start_time < timeout and not self.stopfile.exists():
            self._wait_for_movies()
            split_files = self.preprocessing()
            if self.options.do_class2d:
                if len(split_files) == 1:
                    if class_thread is None:
                        class_thread = threading.Thread(
                            target=self.classification, name="classification_runner"
                        )
                    self._class_queue.put(list(split_files)[0])
                    self._batches.update(split_files)
                else:
                    new_batches = split_files - self._batches
                    if not self._past_class_threshold:
                        self._past_class_threshold = True
                    for sf in new_batches:
                        self._class_queue.put(sf)
                    self._batches.update(new_batches)

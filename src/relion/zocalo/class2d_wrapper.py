from __future__ import annotations

import logging
import os
import re
import subprocess
from pathlib import Path

import numpy as np
import zocalo.wrapper
from gemmi import cif
from pydantic import BaseModel, Field, ValidationError

from relion.zocalo.spa_relion_service_options import RelionServiceOptions

logger = logging.getLogger("relion.class2d.wrapper")


class Class2DParameters(BaseModel):
    particles_file: str = Field(..., min_length=1)
    class2d_dir: str = Field(..., min_length=1)
    batch_is_complete: bool
    batch_size: int
    particle_diameter: float = 0
    mask_diameter: float = 190
    do_vdam = False
    dont_combine_weights_via_disc: bool = True
    preread_images: bool = True
    scratch_dir: str = None
    nr_pool: int = 10
    pad: int = 2
    skip_gridding: bool = False
    do_ctf: bool = True
    ctf_intact_first_peak: bool = False
    nr_iter: int = 20
    tau_fudge: float = 2
    nr_classes: int = 50
    flatten_solvent: bool = True
    do_zero_mask: bool = True
    highres_limit: float = None
    centre_classes: bool = True
    oversampling: int = 1
    skip_align: bool = False
    psi_step: float = 12.0
    offset_range: float = 5
    offset_step: float = 1
    allow_coarser: bool = False
    do_norm: bool = True
    do_scale: bool = True
    threads: int = 4
    gpus: str = "0"
    relion_options: RelionServiceOptions
    combine_star_job_number: int
    particle_picker_id: int
    class2d_grp_id: int
    autoselect_min_score: int = 0
    autoselect_python: str = "python"


class SelectClassesParameters(BaseModel):
    input_file: str = Field(..., min_length=1)
    combine_star_job_number: int
    particles_file: str = "particles.star"
    classes_file: str = "class_averages.star"
    python_exe: str = "/dls_sw/apps/EM/relion/4.0/conda/bin/python"
    min_score: float = 0
    min_particles: int = 500
    class3d_batch_size: int = 50000
    class3d_max_size: int = 200000
    relion_options: RelionServiceOptions


class Class2DWrapper(zocalo.wrapper.BaseWrapper):
    """
    A wrapper for the Relion 2D classification job.
    """

    # Values to extract for ISPyB
    previous_total_count = 0
    total_count = 0

    def parse_combiner_output(self, combiner_stdout: str):
        """
        Read the output logs of the star file combination
        """
        for line in combiner_stdout.split("\n"):
            if line.startswith("Adding") and "particles_all.star" in line:
                line_split = line.split()
                self.previous_total_count = int(line_split[3])

            if line.startswith("Combined"):
                line_split = line.split()
                self.total_count = int(line_split[6])

    def class_selection(self, parameters: dict):
        """
        Run the 2D classification and register results
        """
        job_type = "relion.select.class2dauto"
        try:
            autoselect_params = SelectClassesParameters(**parameters)
        except (ValidationError, TypeError) as e:
            self.log.warning(
                f"Selection parameter validation failed for parameters: {parameters} "
                f"with exception: {e}"
            )
            return False

        self.log.info(f"Inputs: {autoselect_params.input_file}")

        class2d_job_dir = Path(
            re.search(".+/job[0-9]{3}/", autoselect_params.input_file)[0]
        )
        project_dir = class2d_job_dir.parent.parent

        select_job_num = (
            int(re.search("/job[0-9]{3}", str(class2d_job_dir))[0][4:7]) + 2
        )
        select_dir = project_dir / f"Select/job{select_job_num:03}"
        select_dir.mkdir(parents=True, exist_ok=True)

        autoselect_flags = {
            "particles_file": "--fn_sel_parts",
            "classes_file": "--fn_sel_classavgs",
            "python_exe": "--python",
            "min_particles": "--select_min_nr_particles",
        }
        # Create the class selection command
        autoselect_command = [
            "relion_class_ranker",
            "--opt",
            autoselect_params.input_file,
            "--o",
            f"{select_dir.relative_to(project_dir)}/",
            "--auto_select",
            "--fn_root",
            "rank",
            "--do_granularity_features",
        ]
        for k, v in autoselect_params.dict().items():
            if v and (k in autoselect_flags):
                autoselect_command.extend((autoselect_flags[k], str(v)))
        autoselect_command.extend(
            ("--pipeline_control", f"{select_dir.relative_to(project_dir)}/")
        )

        if not autoselect_params.min_score:
            autoselect_command.extend(("--min_score", "0.0"))
        else:
            autoselect_command.extend(("--min_score", str(autoselect_params.min_score)))

        # Run the class selection
        result = subprocess.run(
            autoselect_command, cwd=str(project_dir), capture_output=True
        )
        if result.returncode:
            self.log.error(
                f"2D autoselection failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            return False

        if not autoselect_params.min_score:
            # If a minimum score isn't given, then work it out and rerun the job
            star_doc = cif.read_file(str(select_dir / "rank_model.star"))
            star_block = star_doc["model_classes"]
            class_scores = np.array(star_block.find_loop("_rlnClassScore"), dtype=float)
            quantile_threshold = np.quantile(
                class_scores,
                float(
                    autoselect_params.relion_options.class2d_fraction_of_classes_to_remove
                ),
            )

            self.log.info(f"Sending new threshold {quantile_threshold} to Murfey")
            murfey_params = {
                "register": "save_class_selection_score",
                "class_selection_score": quantile_threshold,
            }
            self.recwrap.send_to("murfey_feedback", murfey_params)

            self.log.info(
                f"Re-running class selection with new threshold {quantile_threshold}"
            )
            autoselect_command[-1] = str(quantile_threshold)

            # Re-run the class selection
            result = subprocess.run(
                autoselect_command, cwd=str(project_dir), capture_output=True
            )
            if result.returncode:
                self.log.error(
                    f"2D autoselection failed with exitcode {result.returncode}:\n"
                    + result.stderr.decode("utf8", "replace")
                )
                return False

        # Send to node creator
        self.log.info(f"Sending {job_type} to node creator")
        autoselect_node_creator_params = {
            "job_type": job_type,
            "input_file": autoselect_params.input_file,
            "output_file": str(select_dir / autoselect_params.particles_file),
            "relion_options": dict(autoselect_params.relion_options),
            "command": " ".join(autoselect_command),
            "stdout": result.stdout.decode("utf8", "replace"),
            "stderr": result.stderr.decode("utf8", "replace"),
        }
        self.recwrap.send_to("node_creator", autoselect_node_creator_params)

        # Run the combine star files job to combine the files into particles_all.star
        self.log.info("Running star file combination and splitting")
        combine_star_command = [
            "combine_star_files.py",
            str(select_dir / autoselect_params.particles_file),
        ]

        combine_star_dir = Path(
            project_dir / f"Select/job{autoselect_params.combine_star_job_number:03}"
        )
        if (combine_star_dir / "particles_all.star").exists():
            combine_star_command.append(str(combine_star_dir / "particles_all.star"))
        else:
            combine_star_dir.mkdir(parents=True, exist_ok=True)
            self.previous_total_count = 0
        combine_star_command.extend(("--output_dir", str(combine_star_dir)))

        result = subprocess.run(
            combine_star_command, cwd=str(project_dir), capture_output=True
        )
        self.parse_combiner_output(result.stdout.decode("utf8", "replace"))
        if result.returncode:
            self.log.error(
                f"Star file combination failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            return False

        # Determine the next split size to use and whether to run 3D classification
        send_to_3d_classification = False
        if self.previous_total_count == 0:
            # First run of this job, use class3d_max_size
            next_batch_size = autoselect_params.class3d_batch_size
            if self.total_count > autoselect_params.class3d_batch_size:
                # Do 3D classification if there are more particles than the batch size
                send_to_3d_classification = True
        elif self.previous_total_count >= autoselect_params.class3d_max_size:
            # Iterations beyond those where 3D classification is run
            next_batch_size = autoselect_params.class3d_max_size
        else:
            # Re-runs with fewer particles than the maximum
            previous_batch_multiple = (
                self.previous_total_count // autoselect_params.class3d_batch_size
            )
            new_batch_multiple = (
                self.total_count // autoselect_params.class3d_batch_size
            )
            if new_batch_multiple > previous_batch_multiple:
                # Do 3D classification if a batch threshold has been crossed
                send_to_3d_classification = True
                # Set the batch size from the total count, but do not exceed the maximum
                next_batch_size = (
                    new_batch_multiple * autoselect_params.class3d_batch_size
                )
                if next_batch_size > autoselect_params.class3d_max_size:
                    next_batch_size = autoselect_params.class3d_max_size
            else:
                # Otherwise just get the next threshold
                next_batch_size = (
                    previous_batch_multiple + 1
                ) * autoselect_params.class3d_batch_size

        # Run the combine star files job to split particles_all.star into batches
        split_star_command = [
            "combine_star_files.py",
            str(combine_star_dir / "particles_all.star"),
            "--output_dir",
            str(combine_star_dir),
            "--split",
            "--split_size",
            str(next_batch_size),
        ]

        result = subprocess.run(
            split_star_command, cwd=str(project_dir), capture_output=True
        )
        self.parse_combiner_output(result.stdout.decode("utf8", "replace"))
        if result.returncode:
            self.log.error(
                f"Star file splitting failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            return False

        # Send to node creator
        self.log.info("Sending combine_star_files_job to node creator")
        combine_node_creator_params = {
            "job_type": "combine_star_files_job",
            "input_file": f"{select_dir}/{autoselect_params.particles_file}",
            "output_file": f"{combine_star_dir}/particles_all.star",
            "relion_options": dict(autoselect_params.relion_options),
            "command": (
                " ".join(combine_star_command) + "\n" + " ".join(split_star_command)
            ),
            "stdout": result.stdout.decode("utf8", "replace"),
            "stderr": result.stderr.decode("utf8", "replace"),
        }
        self.recwrap.send_to("node_creator", combine_node_creator_params)

        # Create 3D classification jobs
        if send_to_3d_classification:
            # Only send to 3D if a new multiple of the batch threshold is crossed
            # and the count has not passed the maximum
            self.log.info("Sending to Murfey for Class3D")
            class3d_params = {
                "particles_file": f"{combine_star_dir}/particles_split1.star",
                "class3d_dir": f"{project_dir}/Class3D/job",
                "batch_size": next_batch_size,
            }
            murfey_params = {
                "register": "run_class3d",
                "class3d_message": class3d_params,
            }
            self.recwrap.send_to("murfey_feedback", murfey_params)

        self.log.info(f"Done {job_type} for {autoselect_params.input_file}.")
        return True

    def run(self):
        """
        Run the 2D classification and register results
        """
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params_dict = self.recwrap.recipe_step["job_parameters"]
        try:
            class2d_params = Class2DParameters(**params_dict)
        except (ValidationError, TypeError) as e:
            self.log.warning(
                f"Class2D parameter validation failed for parameters: {params_dict} "
                f"with exception: {e}"
            )
            return False

        if class2d_params.do_vdam:
            job_type = "relion.class2d.vdam"
        else:
            job_type = "relion.class2d.em"

        # Update the relion options to get out the box sizes
        if class2d_params.particle_diameter:
            class2d_params.relion_options.particle_diameter = (
                class2d_params.particle_diameter
            )
        else:
            class2d_params.relion_options.mask_diameter = class2d_params.mask_diameter

        # Make the job directory and move to the project directory
        job_dir = Path(class2d_params.class2d_dir)
        job_dir.mkdir(parents=True, exist_ok=True)
        project_dir = job_dir.parent.parent
        os.chdir(project_dir)
        job_num = int(
            re.search("/job[0-9]{3}", str(class2d_params.class2d_dir))[0][4:7]
        )

        particles_file = str(
            Path(class2d_params.particles_file).relative_to(project_dir)
        )
        self.log.info(f"Running Class2D for {particles_file}")

        class2d_flags = {
            "dont_combine_weights_via_disc": "--dont_combine_weights_via_disc",
            "preread_images": "--preread_images",
            "scratch_dir": "--scratch_dir",
            "nr_pool": "--pool",
            "pad": "--pad",
            "skip_gridding": "--skip_gridding",
            "do_ctf": "--ctf",
            "ctf_intact_first_peak": "--ctf_intact_first_peak",
            "nr_iter": "--iter",
            "tau_fudge": "--tau2_fudge",
            "nr_classes": "--K",
            "flatten_solvent": "--flatten_solvent",
            "do_zero_mask": "--zero_mask",
            "highres_limit": "--strict_highres_exp",
            "centre_classes": "--center_classes",
            "oversampling": "--oversampling",
            "skip_align": "--skip_align",
            "psi_step": "--psi_step",
            "offset_range": "--offset_range",
            "offset_step": "--offset_step",
            "allow_coarser": "--allow_coarser_sampling",
            "do_norm": "--norm",
            "do_scale": "--scale",
            "threads": "--j",
            "gpus": "--gpu",
        }

        # Create the classification command
        class2d_command = [
            "relion_refine",
            "--i",
            particles_file,
            "--o",
            f"{job_dir.relative_to(project_dir)}/run",
            "--particle_diameter",
            f"{class2d_params.relion_options.mask_diameter}",
        ]
        for k, v in class2d_params.dict().items():
            if v and (k in class2d_flags):
                if type(v) is bool:
                    class2d_command.append(class2d_flags[k])
                else:
                    class2d_command.extend((class2d_flags[k], str(v)))
        class2d_command.extend(
            ("--pipeline_control", f"{job_dir.relative_to(project_dir)}/")
        )

        # Run Class2D and confirm it ran successfully
        result = subprocess.run(
            class2d_command, cwd=str(project_dir), capture_output=True
        )
        if result.returncode:
            self.log.error(
                f"Relion Class2D failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            return False

        # Register the Class2D job with the node creator
        self.log.info(f"Sending {job_type} to node creator")
        node_creator_parameters = {
            "job_type": job_type,
            "input_file": class2d_params.particles_file,
            "output_file": class2d_params.class2d_dir,
            "relion_options": dict(class2d_params.relion_options),
            "command": " ".join(class2d_command),
            "stdout": result.stdout.decode("utf8", "replace"),
            "stderr": result.stderr.decode("utf8", "replace"),
        }
        self.recwrap.send_to("node_creator", node_creator_parameters)

        # Send classification job information to ispyb
        ispyb_parameters = {
            "type": "2D",
            "batch_number": int(
                class2d_params.particles_file.split("particles_split")[1].split(".")[0]
            ),
            "number_of_particles_per_batch": class2d_params.batch_size,
            "number_of_classes_per_batch": class2d_params.nr_classes,
            "symmetry": "C1",
        }
        ispyb_parameters.update(
            {
                "ispyb_command": "buffer",
                "buffer_store": class2d_params.class2d_grp_id,
                "buffer_lookup": {
                    "particle_picker_id": class2d_params.particle_picker_id,
                },
                "buffer_command": {
                    "ispyb_command": "insert_particle_classification_group"
                },
            }
        )
        self.log.info(f"Sending to ispyb {ispyb_parameters}")
        self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_parameters})

        # Send individual classes to ispyb
        class_star_file = cif.read_file(
            f"{class2d_params.class2d_dir}/run_it{class2d_params.nr_iter:03}_model.star"
        )
        classes_block = class_star_file.find_block("model_classes")
        classes_loop = classes_block.find_loop("_rlnReferenceImage").get_loop()

        for class_id in range(class2d_params.nr_classes):
            ispyb_parameters = {
                "class_number": class_id + 1,
                "class_image_full_path": (
                    f"{class2d_params.class2d_dir}/Class_images"
                    f"/run_it{class2d_params.nr_iter:03}_classes_{class_id+1}.jpeg"
                ),
                "particles_per_class": (
                    float(classes_loop.val(class_id, 1)) * class2d_params.batch_size
                ),
                "class_distribution": classes_loop.val(class_id, 1),
                "rotation_accuracy": classes_loop.val(class_id, 2),
                "translation_accuracy": classes_loop.val(class_id, 3),
                "estimated_resolution": classes_loop.val(class_id, 4),
                "overall_fourier_completeness": classes_loop.val(class_id, 5),
            }
            ispyb_parameters.update(
                {
                    "ispyb_command": "buffer",
                    "buffer_lookup": {
                        "particle_classification_group_id": class2d_params.class2d_grp_id
                    },
                    "buffer_command": {
                        "ispyb_command": "insert_particle_classification"
                    },
                }
            )
            self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_parameters})

        if class2d_params.batch_is_complete:
            # Create an icebreaker job
            if class2d_params.relion_options.do_icebreaker_jobs:
                self.log.info("Sending to icebreaker particle analysis")
                icebreaker_params = {
                    "icebreaker_type": "particles",
                    "input_micrographs": (
                        f"{project_dir}/IceBreaker/job003/grouped_micrographs.star"
                    ),
                    "input_particles": class2d_params.particles_file,
                    "output_path": f"{project_dir}/IceBreaker/job{job_num + 1:03}/",
                    "mc_uuid": -1,
                    "relion_options": dict(class2d_params.relion_options),
                }
                self.recwrap.send_to("icebreaker", icebreaker_params)

            # Create a 2D autoselection job
            self.log.info("Sending to class selection")
            autoselect_parameters = {
                "input_file": f"{class2d_params.class2d_dir}/run_it{class2d_params.nr_iter:03}_optimiser.star",
                "combine_star_job_number": class2d_params.combine_star_job_number,
                "min_score": class2d_params.autoselect_min_score,
                "relion_options": dict(class2d_params.relion_options),
                "python": class2d_params.autoselect_python,
            }
            # Currently options on this to run in a separate service (next line)
            # or as part of this wrapper (3 lines following that)
            self.recwrap.send_to("select_classes", autoselect_parameters)
            # autoselect_outcome = self.class_selection(autoselect_parameters)
            # if not autoselect_outcome:
            #     return False

        self.log.info(f"Done {job_type} for {class2d_params.particles_file}.")
        return True

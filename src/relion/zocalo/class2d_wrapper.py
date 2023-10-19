from __future__ import annotations

import json
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
    offset_step: float = 2
    allow_coarser: bool = False
    do_norm: bool = True
    do_scale: bool = True
    mpi_run_command: str = "srun -n 5"
    threads: int = 8
    gpus: str = "0:1:2:3"
    program_id: int
    session_id: int
    relion_options: RelionServiceOptions
    combine_star_job_number: int
    picker_id: int
    class2d_grp_uuid: int
    class_uuids: str
    autoselect_python: str = "python"


class Class2DWrapper(zocalo.wrapper.BaseWrapper):
    """
    A wrapper for the Relion 2D classification job.
    """

    # Values to extract for ISPyB
    previous_total_count = 0
    total_count = 0

    # Values for ISPyB lookups
    class_uuids_dict: dict = {}
    class_uuids_keys: list = []

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

        # Class ids get fed in as a string, need to convert these to a dictionary
        self.class_uuids_dict = json.loads(class2d_params.class_uuids.replace("'", '"'))
        self.class_uuids_keys = list(self.class_uuids_dict.keys())

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
        if (job_dir / "run_it000_model.star").exists():
            # This job over-writes a previous one
            job_is_rerun = True
        else:
            job_is_rerun = False
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
        class2d_command = class2d_params.mpi_run_command.split()
        class2d_command.extend(
            [
                "relion_refine_mpi",
                "--i",
                particles_file,
                "--o",
                f"{job_dir.relative_to(project_dir)}/run",
                "--particle_diameter",
                f"{class2d_params.relion_options.mask_diameter}",
            ]
        )
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
        self.log.info(" ".join(class2d_command))
        result = subprocess.run(
            class2d_command, cwd=str(project_dir), capture_output=True
        )

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
        if result.returncode:
            node_creator_parameters["success"] = False
        else:
            node_creator_parameters["success"] = True
        self.recwrap.send_to("node_creator", node_creator_parameters)

        # End here if the command failed
        if result.returncode:
            self.log.error(
                f"Relion Class2D failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            return False

        # Send classification job information to ispyb
        ispyb_parameters = []
        classification_grp_ispyb_parameters = {
            "ispyb_command": "buffer",
            "buffer_command": {"ispyb_command": "insert_particle_classification_group"},
            "buffer_store": class2d_params.class2d_grp_uuid,
            "type": "2D",
            "batch_number": int(
                class2d_params.particles_file.split("particles_split")[1].split(".")[0]
            ),
            "number_of_particles_per_batch": class2d_params.batch_size,
            "number_of_classes_per_batch": class2d_params.nr_classes,
            "symmetry": "C1",
            "particle_picker_id": class2d_params.picker_id,
        }
        if job_is_rerun:
            # If this job overwrites another get the id for it
            classification_grp_ispyb_parameters["buffer_lookup"] = {
                "particle_classification_group_id": class2d_params.class2d_grp_uuid,
            }
        ispyb_parameters.append(classification_grp_ispyb_parameters)

        # Send individual classes to ispyb
        class_star_file = cif.read_file(
            f"{class2d_params.class2d_dir}/run_it{class2d_params.nr_iter:03}_model.star"
        )
        classes_block = class_star_file.find_block("model_classes")
        classes_loop = classes_block.find_loop("_rlnReferenceImage").get_loop()

        for class_id in range(class2d_params.nr_classes):
            # Add an ispyb insert for each class
            if job_is_rerun:
                buffer_lookup = {
                    "particle_classification_id": self.class_uuids_dict[
                        self.class_uuids_keys[class_id]
                    ],
                    "particle_classification_group_id": class2d_params.class2d_grp_uuid,
                }
            else:
                buffer_lookup = {
                    "particle_classification_group_id": class2d_params.class2d_grp_uuid,
                }
            class_ispyb_parameters = {
                "ispyb_command": "buffer",
                "buffer_lookup": buffer_lookup,
                "buffer_command": {"ispyb_command": "insert_particle_classification"},
                "buffer_store": self.class_uuids_dict[self.class_uuids_keys[class_id]],
                "class_number": class_id + 1,
                "class_image_full_path": (
                    f"{class2d_params.class2d_dir}"
                    f"/run_it{class2d_params.nr_iter:03}_classes_{class_id+1}.jpeg"
                ),
                "particles_per_class": (
                    float(classes_loop.val(class_id, 1)) * class2d_params.batch_size
                ),
                "class_distribution": classes_loop.val(class_id, 1),
                "rotation_accuracy": classes_loop.val(class_id, 2),
                "translation_accuracy": classes_loop.val(class_id, 3),
            }

            # Add the resolution and fourier completeness if they are valid numbers
            estimated_resolution = float(classes_loop.val(class_id, 4))
            if np.isfinite(estimated_resolution):
                class_ispyb_parameters["estimated_resolution"] = estimated_resolution
            else:
                class_ispyb_parameters["estimated_resolution"] = 0.0
            fourier_completeness = float(classes_loop.val(class_id, 5))
            if np.isfinite(fourier_completeness):
                class_ispyb_parameters[
                    "overall_fourier_completeness"
                ] = fourier_completeness
            else:
                class_ispyb_parameters["overall_fourier_completeness"] = 0.0

            # Add the ispyb command to the command list
            ispyb_parameters.append(class_ispyb_parameters)

        # Send a request to make the class images
        self.log.info("Sending to images service")
        self.recwrap.send_to(
            "images",
            {
                "image_command": "mrc_to_jpeg",
                "file": (
                    f"{class2d_params.class2d_dir}"
                    f"/run_it{class2d_params.nr_iter:03}_classes.mrcs"
                ),
                "all_frames": "True",
            },
        )

        # Send all the ispyb class insertion commands
        self.log.info(f"Sending to ispyb {ispyb_parameters}")
        self.recwrap.send_to(
            "ispyb_connector",
            {
                "ispyb_command": "multipart_message",
                "ispyb_command_list": ispyb_parameters,
            },
        )

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
                "relion_options": dict(class2d_params.relion_options),
                "python": class2d_params.autoselect_python,
                "class_uuids": class2d_params.class_uuids,
            }
            self.recwrap.send_to("select_classes", autoselect_parameters)
        else:
            # Tell Murfey the incomplete batch has finished
            murfey_params = {
                "register": "done_incomplete_2d_batch",
                "job_dir": class2d_params.class2d_dir,
                "program_id": class2d_params.program_id,
                "session_id": class2d_params.session_id,
            }
            self.recwrap.send_to("murfey_feedback", murfey_params)

        self.log.info(f"Done {job_type} for {class2d_params.particles_file}.")
        return True

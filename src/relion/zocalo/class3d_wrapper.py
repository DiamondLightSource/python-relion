from __future__ import annotations

import json
import logging
import os
import re
import subprocess
from pathlib import Path

import zocalo.wrapper
from gemmi import cif
from pydantic import BaseModel, Field, ValidationError

from relion.zocalo.spa_relion_service_options import RelionServiceOptions

logger = logging.getLogger("relion.class3d.wrapper")

job_type = "relion.class3d"


class Class3DParameters(BaseModel):
    particles_file: str = Field(..., min_length=1)
    class3d_dir: str = Field(..., min_length=1)
    batch_size: int
    particle_diameter: float = 0
    mask_diameter: float = 190
    do_initial_model: bool = False
    initial_model_file: str = None
    initial_model_iterations: int = 200
    initial_model_offset_range: float = 6
    initial_model_offset_step: float = 2
    start_initial_model_C1: bool = True
    dont_combine_weights_via_disc: bool = True
    preread_images: bool = True
    scratch_dir: str = None
    nr_pool: int = 10
    pad: int = 2
    skip_gridding: bool = False
    dont_correct_greyscale: bool = True
    ini_high: float = 40.0
    do_ctf: bool = True
    ctf_intact_first_peak: bool = False
    nr_iter: int = 20
    tau_fudge: float = 4
    nr_classes: int = 4
    flatten_solvent: bool = True
    do_zero_mask: bool = True
    highres_limit: float = None
    fn_mask: str = None
    oversampling: int = 1
    skip_align: bool = False
    healpix_order: float = 2
    offset_range: float = 5
    offset_step: float = 1
    allow_coarser: bool = False
    symmetry: str = "C1"
    do_norm: bool = True
    do_scale: bool = True
    threads: int = 4
    gpus: str = "0"
    picker_id: int
    class3d_grp_uuid: int
    class_uuids: str
    class_uuids_dict: dict = None
    relion_options: RelionServiceOptions


class Class3DWrapper(zocalo.wrapper.BaseWrapper):
    """
    A wrapper for the Relion 3D classification job.
    """

    common_flags = {
        "dont_combine_weights_via_disc": "--dont_combine_weights_via_disc",
        "preread_images": "--preread_images",
        "scratch_dir": "--scratch_dir",
        "nr_pool": "--pool",
        "pad": "--pad",
        "skip_gridding": "--skip_gridding",
        "do_ctf": "--ctf",
        "ctf_intact_first_peak": "--ctf_intact_first_peak",
        "nr_classes": "--K",
        "flatten_solvent": "--flatten_solvent",
        "do_zero_mask": "--zero_mask",
        "oversampling": "--oversampling",
        "healpix_order": "--healpix_order",
        "threads": "--j",
        "gpus": "--gpu",
    }

    def run_initial_model(self, initial_model_params, project_dir, job_num):
        """
        Run the initial model for 3D classification and register results
        """
        job_dir = project_dir / f"InitialModel/job{job_num:03}/"
        job_dir.mkdir(parents=True, exist_ok=True)
        particles_file = str(
            Path(initial_model_params.particles_file).relative_to(project_dir)
        )

        initial_model_flags = {
            "initial_model_iterations": "--iter",
            "initial_model_offset_range": "--offset_range",
            "initial_model_offset_step": "--offset_step",
        }
        initial_model_flags.update(self.common_flags)

        initial_model_command = [
            "relion_refine",
            "--grad",
            "--denovo_3dref",
            "--i",
            particles_file,
            "--o",
            f"{job_dir.relative_to(project_dir)}/run",
            "--particle_diameter",
            f"{initial_model_params.relion_options.mask_diameter}",
        ]
        if initial_model_params.start_initial_model_C1:
            initial_model_command.extend(("--sym", "C1"))
        else:
            initial_model_command.extend(("--sym", initial_model_params.symmetry))
        for k, v in initial_model_params.dict().items():
            if v and (k in initial_model_flags):
                if type(v) is bool:
                    initial_model_command.append(initial_model_flags[k])
                else:
                    initial_model_command.extend((initial_model_flags[k], str(v)))
        initial_model_command.extend(
            ("--pipeline_control", f"{job_dir.relative_to(project_dir)}/")
        )

        # Run initial model and confirm it ran successfully
        self.log.info("Running initial model")
        result = subprocess.run(
            initial_model_command, cwd=str(project_dir), capture_output=True
        )
        if result.returncode:
            self.log.error(
                f"Relion initial model failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            return False

        ini_model_file = job_dir / "initial_model.mrc"
        align_symmetry_command = [
            "relion_align_symmetry",
            "--i",
            str(
                job_dir.relative_to(project_dir)
                / f"run_it{initial_model_params.initial_model_iterations:03}_model.star"
            ),
            "--o",
            f"{ini_model_file.relative_to(project_dir)}",
            "--sym",
            initial_model_params.symmetry,
            "--apply_sym",
            "--select_largest_class",
            "--pipeline_control",
            f"{job_dir.relative_to(project_dir)}/",
        ]

        # Run symmetry alignment and confirm it ran successfully
        self.log.info("Running symmetry alignment")
        result = subprocess.run(
            align_symmetry_command, cwd=str(project_dir), capture_output=True
        )
        if result.returncode:
            self.log.error(
                f"Relion initial model symmetry alignment "
                f"failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            return False

        # Register the initial model job with the node creator
        self.log.info("Sending relion.initialmodel to node creator")
        node_creator_parameters = {
            "job_type": "relion.initialmodel",
            "input_file": f"{project_dir}/{particles_file}",
            "output_file": f"{job_dir}/initial_model.mrc",
            "relion_options": dict(initial_model_params.relion_options),
            "command": (
                " ".join(initial_model_command) + "\n" + "".join(align_symmetry_command)
            ),
            "stdout": result.stdout.decode("utf8", "replace"),
            "stderr": result.stderr.decode("utf8", "replace"),
        }
        self.recwrap.send_to("node_creator", node_creator_parameters)

        # Send Murfey the location of the initial model
        murfey_params = {
            "register": "save_initial_model",
            "initial_model": f"{job_dir}/initial_model.mrc",
        }
        self.recwrap.send_to("murfey_feedback", murfey_params)

        # Extract parameters for ispyb
        initial_model_cif = cif.read_file(
            f"{job_dir}/run_it{initial_model_params.initial_model_iterations:03}_model.star"
        )
        initial_model_block = initial_model_cif.find_block("model_classes")
        model_scores = list(initial_model_block.find_loop("_rlnClassDistribution"))
        model_resolutions = list(
            initial_model_block.find_loop("_rlnEstimatedResolution")
        )
        resolution = model_resolutions[model_scores == max(model_scores)]
        number_of_particles = (
            float(model_scores[model_scores == max(model_scores)])
            * initial_model_params.batch_size
        )

        self.log.info(
            "Will send best initial model to ispyb with "
            f"resolution {resolution} and {number_of_particles} particles"
        )
        ispyb_parameters = {
            "ispyb_command": "buffer",
            "buffer_command": {"ispyb_command": "insert_cryoem_initial_model"},
            "particle_classification_id": initial_model_params.class_uuids_dict["0"],
            "resolution": resolution,
            "number_of_particles": number_of_particles,
        }

        self.log.info("Running 3D classification using new initial model")
        return f"{ini_model_file}", ispyb_parameters

    def run(self):
        """
        Run the 3D classification and register results
        """
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params_dict = self.recwrap.recipe_step["job_parameters"]
        try:
            class3d_params = Class3DParameters(**params_dict)
        except (ValidationError, TypeError) as e:
            self.log.warning(
                f"Class3D parameter validation failed for parameters: {params_dict} "
                f"with exception: {e}"
            )
            return False

        # Class ids get fed in as a string, need to convert these to a dictionary
        class3d_params.class_uuids_dict = json.loads(
            class3d_params.class_uuids.replace("'", '"')
        )

        # Update the relion options to get out the box sizes
        if class3d_params.particle_diameter:
            class3d_params.relion_options.particle_diameter = (
                class3d_params.particle_diameter
            )
        else:
            class3d_params.relion_options.mask_diameter = class3d_params.mask_diameter

        # Make the job directory and move to the project directory
        job_dir = Path(class3d_params.class3d_dir)
        if (job_dir / "run_it000_model.star").exists():
            # This job over-writes a previous one
            job_is_rerun = True
        else:
            job_is_rerun = False
            job_dir.mkdir(parents=True, exist_ok=True)
        project_dir = job_dir.parent.parent
        os.chdir(project_dir)

        particles_file = str(
            Path(class3d_params.particles_file).relative_to(project_dir)
        )
        self.log.info(f"Running Class3D for {particles_file}")

        # Run the initial model if requested, otherwise look for a pre-existing file
        if class3d_params.do_initial_model:
            job_num_3d = int(
                re.search("/job[0-9]{3}", class3d_params.class3d_dir)[0][4:7]
            )
            initial_model_file, initial_model_ispyb_parameters = self.run_initial_model(
                class3d_params, project_dir, job_num_3d - 1
            )
        else:
            initial_model_file = str(
                Path(class3d_params.initial_model_file).relative_to(project_dir)
            )
            initial_model_ispyb_parameters = {}
        if not initial_model_file:
            # If there isn't an initial model file something has gone wrong
            return False

        class3d_flags = {
            "dont_correct_greyscale": "--firstiter_cc",
            "ini_high": "--ini_high",
            "nr_iter": "--iter",
            "tau_fudge": "--tau2_fudge",
            "highres_limit": "--strict_highres_exp",
            "fn_mask": "--solvent_mask",
            "skip_align": "--skip_align",
            "offset_range": "--offset_range",
            "offset_step": "--offset_step",
            "allow_coarser": "--allow_coarser_sampling",
            "symmetry": "--sym",
            "do_norm": "--norm",
            "do_scale": "--scale",
        }
        class3d_flags.update(self.common_flags)

        # Create the classification command
        class3d_command = [
            "relion_refine",
            "--i",
            particles_file,
            "--o",
            f"{job_dir.relative_to(project_dir)}/run",
            "--ref",
            initial_model_file,
            "--particle_diameter",
            f"{class3d_params.relion_options.mask_diameter}",
        ]
        for k, v in class3d_params.dict().items():
            if v and (k in class3d_flags):
                if type(v) is bool:
                    class3d_command.append(class3d_flags[k])
                else:
                    class3d_command.extend((class3d_flags[k], str(v)))
        class3d_command.extend(
            ("--pipeline_control", f"{job_dir.relative_to(project_dir)}/")
        )

        # Run Class3D and confirm it ran successfully
        result = subprocess.run(
            class3d_command, cwd=str(project_dir), capture_output=True
        )
        if result.returncode:
            self.log.error(
                f"Relion Class3D failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            return False

        # Register the Class3D job with the node creator
        self.log.info(f"Sending {job_type} to node creator")
        node_creator_parameters = {
            "job_type": job_type,
            "input_file": class3d_params.particles_file + f":{initial_model_file}",
            "output_file": class3d_params.class3d_dir,
            "relion_options": dict(class3d_params.relion_options),
            "command": " ".join(class3d_command),
            "stdout": result.stdout.decode("utf8", "replace"),
            "stderr": result.stderr.decode("utf8", "replace"),
        }
        self.recwrap.send_to("node_creator", node_creator_parameters)

        # Send classification job information to ispyb
        ispyb_parameters = []
        classification_grp_ispyb_parameters = {
            "ispyb_command": "buffer",
            "buffer_command": {"ispyb_command": "insert_particle_classification_group"},
            "buffer_store": class3d_params.class3d_grp_uuid,
            "type": "3D",
            "batch_number": int(
                class3d_params.particles_file.split("particles_split")[1].split(".")[0]
            ),
            "number_of_particles_per_batch": class3d_params.batch_size,
            "number_of_classes_per_batch": class3d_params.nr_classes,
            "symmetry": class3d_params.symmetry,
            "particle_picker_id": class3d_params.picker_id,
        }
        if job_is_rerun:
            # If this job overwrites another get the id for it
            classification_grp_ispyb_parameters["buffer_lookup"] = {
                "particle_classification_group_id": class3d_params.class3d_grp_uuid,
            }
        ispyb_parameters.append(classification_grp_ispyb_parameters)

        # Send individual classes to ispyb
        class_star_file = cif.read_file(
            f"{class3d_params.class3d_dir}/run_it{class3d_params.nr_iter:03}_model.star"
        )
        classes_block = class_star_file.find_block("model_classes")
        classes_loop = classes_block.find_loop("_rlnReferenceImage").get_loop()

        for class_id in range(class3d_params.nr_classes):
            # Add an ispyb insert for each class
            if job_is_rerun:
                buffer_lookup = {
                    "particle_classification_id": class3d_params.class_uuids_dict["0"],
                    "particle_classification_group_id": class3d_params.class3d_grp_uuid,
                }
            else:
                buffer_lookup = {
                    "particle_classification_group_id": class3d_params.class3d_grp_uuid,
                }
            ispyb_parameters.append(
                {
                    "ispyb_command": "buffer",
                    "buffer_lookup": buffer_lookup,
                    "buffer_command": {
                        "ispyb_command": "insert_particle_classification"
                    },
                    "buffer_store": class3d_params.class_uuids_dict[str(class_id)],
                    "class_number": class_id + 1,
                    "class_image_full_path": None,
                    "particles_per_class": (
                        float(classes_loop.val(class_id, 1)) * class3d_params.batch_size
                    ),
                    "class_distribution": classes_loop.val(class_id, 1),
                    "rotation_accuracy": classes_loop.val(class_id, 2),
                    "translation_accuracy": classes_loop.val(class_id, 3),
                    "estimated_resolution": classes_loop.val(class_id, 4),
                    "overall_fourier_completeness": classes_loop.val(class_id, 5),
                }
            )

        # Add on the initial model insert before sending
        if class3d_params.do_initial_model:
            ispyb_parameters.append(initial_model_ispyb_parameters)

        self.log.info(f"Sending to ispyb {ispyb_parameters}")
        self.recwrap.send_to(
            "ispyb_connector", {"ispyb_command_list": ispyb_parameters}
        )

        self.log.info(f"Done {job_type} for {class3d_params.particles_file}.")
        return True

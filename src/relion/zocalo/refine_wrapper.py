from __future__ import annotations

import logging
import os
import re
import subprocess
from pathlib import Path

import numpy as np
import zocalo.wrapper
from gemmi import cif
from pydantic import Field, ValidationError

from relion.zocalo.refine3d import (
    CommonRefineParameters,
    run_postprocessing,
    run_refine3d,
)

logger = logging.getLogger("relion.refine.wrapper")


class RefineParameters(CommonRefineParameters):
    refine_job_dir: str = Field(..., min_length=1)
    class3d_dir: str = Field(..., min_length=1)
    micrographs_file: str = Field(..., min_length=1)
    downscaled_pixel_size: float
    class_number: int
    nr_iter_3d: int = 20
    boxsize: int = 256
    bg_radius: int = -1
    mask_lowpass: float = 15
    mask_threshold: float = 0.02
    mask_extend: int = 3
    mask_soft_edge: int = 3
    picker_id: int
    refined_grp_uuid: int
    refined_class_uuid: int


class RefineWrapper(zocalo.wrapper.BaseWrapper):
    """
    A wrapper for the Relion 3D refinement pipeline.
    """

    # Job names
    select_job_type = "relion.select.onvalue"
    extract_job_type = "relion.extract.reextract"
    refine_job_type = "relion.refine3d"
    mask_job_type = "relion.maskcreate"
    postprocess_job_type = "relion.postprocess"

    def run(self):
        """
        Run 3D refinement and postprocessing
        """
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params_dict = self.recwrap.recipe_step["job_parameters"]
        try:
            refine_params = RefineParameters(**params_dict)
        except (ValidationError, TypeError) as e:
            self.log.warning(
                f"Refinement parameter validation failed for parameters: {params_dict} "
                f"with exception: {e}"
            )
            return False

        # Determine the directory to run in
        project_dir = Path(refine_params.refine_job_dir).parent.parent
        os.chdir(project_dir)

        job_num_refine = int(
            re.search("/job[0-9]{3}", refine_params.refine_job_dir)[0][4:7]
        )

        # Link the required files
        particles_data = (
            Path(refine_params.class3d_dir)
            / f"run_it{refine_params.nr_iter_3d:03}_data.star"
        )

        # Relion options
        refine_params.relion_options.angpix = refine_params.pixel_size
        refine_params.relion_options.mask_diameter = refine_params.mask_diameter
        refine_params.relion_options.refine_class = refine_params.class_number

        self.log.info(
            f"Running refinement pipeline for {refine_params.class3d_dir} class {refine_params.class_number}"
        )

        ###############################################################################
        # Select the particles from the requested class
        select_job_dir = Path(f"Select/job{job_num_refine-2:03}")
        select_job_dir.mkdir(parents=True, exist_ok=True)

        refine_selection_link = Path(
            project_dir / f"Select/Refine_class{refine_params.class_number}"
        )
        if not refine_selection_link.is_symlink():
            refine_selection_link.symlink_to(f"job{job_num_refine-2:03}")

        self.log.info(f"Running {self.select_job_type} in {select_job_dir}")
        select_command = [
            "relion_star_handler",
            "--i",
            str(particles_data),
            "--o",
            f"{select_job_dir}/particles.star",
            "--select",
            "rlnClassNumber",
            "--minval",
            str(refine_params.class_number),
            "--maxval",
            str(refine_params.class_number),
            "--pipeline_control",
            f"{select_job_dir}/",
        ]
        select_result = subprocess.run(
            select_command, cwd=str(project_dir), capture_output=True
        )

        # Register the Selection job with the node creator
        self.log.info(f"Sending {self.select_job_type} to node creator")
        node_creator_select = {
            "job_type": self.select_job_type,
            "input_file": str(particles_data),
            "output_file": f"{project_dir}/{select_job_dir}/particles.star",
            "relion_options": dict(refine_params.relion_options),
            "command": " ".join(select_command),
            "stdout": select_result.stdout.decode("utf8", "replace"),
            "stderr": select_result.stderr.decode("utf8", "replace"),
        }
        if select_result.returncode:
            node_creator_select["success"] = False
        else:
            node_creator_select["success"] = True
        self.recwrap.send_to("node_creator", node_creator_select)

        # End here if the command failed
        if select_result.returncode:
            self.log.error(
                "Refinement selection failed with exitcode "
                f"{select_result.returncode}:\n"
                + select_result.stderr.decode("utf8", "replace")
            )
            return False

        # Find the number of particles in the class
        number_of_particles = select_result.stdout.decode("utf8", "replace").split(" ")[
            3
        ]

        ###############################################################################
        # Run re-extraction on the selected particles
        extract_job_dir = Path(f"Extract/job{job_num_refine-1:03}")
        extract_job_dir.mkdir(parents=True, exist_ok=True)

        refine_extraction_link = Path(
            project_dir / f"Extract/Reextract_class{refine_params.class_number}"
        )
        if not refine_extraction_link.is_symlink():
            refine_extraction_link.symlink_to(f"job{job_num_refine-1:03}")

        # If no background radius set diameter as 75% of box
        if refine_params.bg_radius == -1:
            refine_params.bg_radius = round(0.375 * refine_params.boxsize)

        self.log.info(f"Running {self.extract_job_type} in {extract_job_dir}")
        extract_command = [
            "relion_preprocess",
            "--i",
            refine_params.micrographs_file,
            "--reextract_data_star",
            str(project_dir / select_job_dir / "particles.star"),
            "--recenter",
            "--recenter_x",
            "0",
            "--recenter_y",
            "0",
            "--recenter_z",
            "0",
            "--part_star",
            str(extract_job_dir / "particles.star"),
            "--pick_star",
            str(extract_job_dir / "extractpick.star"),
            "--part_dir",
            str(extract_job_dir),
            "--extract",
            "--extract_size",
            str(refine_params.boxsize),
            "--norm",
            "--bg_radius",
            str(refine_params.bg_radius),
            "--white_dust",
            "-1",
            "--black_dust",
            "-1",
            "--invert_contrast",
            "--pipeline_control",
            f"{extract_job_dir}/",
        ]
        extract_result = subprocess.run(
            extract_command, cwd=str(project_dir), capture_output=True
        )

        # Register the Re-extraction job with the node creator
        self.log.info(f"Sending {self.extract_job_type} to node creator")
        node_creator_extract = {
            "job_type": self.extract_job_type,
            "input_file": f"{project_dir}/{select_job_dir}/particles.star:{refine_params.micrographs_file}",
            "output_file": f"{project_dir}/{extract_job_dir}/particles.star",
            "relion_options": dict(refine_params.relion_options),
            "command": " ".join(extract_command),
            "stdout": extract_result.stdout.decode("utf8", "replace"),
            "stderr": extract_result.stderr.decode("utf8", "replace"),
        }
        if extract_result.returncode:
            node_creator_extract["success"] = False
        else:
            node_creator_extract["success"] = True
        self.recwrap.send_to("node_creator", node_creator_extract)

        # End here if the command failed
        if extract_result.returncode:
            self.log.error(
                "Refinement re-extraction failed with exitcode "
                f"{extract_result.returncode}:\n"
                + extract_result.stderr.decode("utf8", "replace")
            )
            return False

        ###############################################################################
        # Create a reference for the refinement
        class_reference = (
            Path(refine_params.class3d_dir)
            / f"run_it{refine_params.nr_iter_3d:03}_class{refine_params.class_number:03}.mrc"
        )
        rescaled_class_reference = (
            project_dir
            / extract_job_dir
            / f"refinement_reference_class{refine_params.class_number:03}.mrc"
        )

        self.log.info("Running class reference rescaling")
        rescale_command = [
            "relion_image_handler",
            "--i",
            str(class_reference),
            "--o",
            str(rescaled_class_reference),
            "--angpix",
            str(refine_params.downscaled_pixel_size),
            "--rescale_angpix",
            str(refine_params.pixel_size),
            "--new_box",
            str(refine_params.boxsize),
        ]
        rescale_result = subprocess.run(
            rescale_command, cwd=str(project_dir), capture_output=True
        )

        # End here if the command failed
        if rescale_result.returncode:
            self.log.error(
                "Refinement reference scaling failed with exitcode "
                f"{rescale_result.returncode}:\n"
                + rescale_result.stderr.decode("utf8", "replace")
            )
            return False

        ###############################################################################
        # Set up the refinement job
        Path(refine_params.refine_job_dir).mkdir(parents=True, exist_ok=True)

        # Run Refine3D and confirm it ran successfully
        self.log.info(
            f"Running {self.refine_job_type} in {refine_params.refine_job_dir}"
        )
        refine_result, node_creator_refine = run_refine3d(
            refine_job_dir=Path(refine_params.refine_job_dir),
            particles_file=project_dir / extract_job_dir / "particles.star",
            class_reference=rescaled_class_reference,
            refine_params=refine_params,
        )

        # Register the Refine3D job with the node creator
        self.log.info(f"Sending {self.refine_job_type} to node creator")
        self.recwrap.send_to("node_creator", node_creator_refine)

        # End here if the command failed
        if refine_result.returncode:
            self.log.error(
                "Refinement Refine3D failed with exitcode "
                f"{refine_result.returncode}:\n"
                + refine_result.stderr.decode("utf8", "replace")
            )
            return False

        ###############################################################################
        # Do the mask creation
        mask_job_dir = Path(f"MaskCreate/job{job_num_refine + 1:03}")
        mask_job_dir.mkdir(parents=True, exist_ok=True)

        self.log.info(f"Running {self.mask_job_type} in {mask_job_dir}")
        mask_command = [
            "relion_mask_create",
            "--i",
            f"{refine_params.refine_job_dir}/run_class001.mrc",
            "--o",
            f"{mask_job_dir}/mask.mrc",
            "--lowpass",
            str(refine_params.mask_lowpass),
            "--ini_threshold",
            str(refine_params.mask_threshold),
            "--extend_inimask",
            str(refine_params.mask_extend),
            "--width_soft_edge",
            str(refine_params.mask_soft_edge),
            "--angpix",
            str(refine_params.pixel_size),
            "--j",
            str(refine_params.threads),
            "--pipeline_control",
            f"{mask_job_dir}/",
        ]
        mask_result = subprocess.run(
            mask_command, cwd=str(project_dir), capture_output=True
        )

        # Register the mask creation job with the node creator
        self.log.info(f"Sending {self.mask_job_type} to node creator")

        node_creator_mask = {
            "job_type": self.mask_job_type,
            "input_file": f"{refine_params.refine_job_dir}/run_class001.mrc",
            "output_file": f"{project_dir}/{mask_job_dir}/mask.mrc",
            "relion_options": dict(refine_params.relion_options),
            "command": " ".join(mask_command),
            "stdout": mask_result.stdout.decode("utf8", "replace"),
            "stderr": mask_result.stderr.decode("utf8", "replace"),
        }
        if mask_result.returncode:
            node_creator_mask["success"] = False
        else:
            node_creator_mask["success"] = True
        self.recwrap.send_to("node_creator", node_creator_mask)

        # End here if the command failed
        if mask_result.returncode:
            self.log.error(
                "Refinement mask creation failed with exitcode "
                f"{mask_result.returncode}:\n"
                + mask_result.stderr.decode("utf8", "replace")
            )
            return False

        ###############################################################################
        # Use PostProcessing to determine if this is a rerun
        postprocess_job_dir = Path(f"PostProcess/job{job_num_refine + 2:03}")
        if (postprocess_job_dir / "RELION_JOB_EXIT_SUCCESS").exists():
            job_is_rerun = True
        else:
            job_is_rerun = False
            postprocess_job_dir.mkdir(parents=True, exist_ok=True)

        # Do the post-processsing
        self.log.info(f"Running {self.postprocess_job_type} in {postprocess_job_dir}")
        postprocess_result, node_creator_postprocess = run_postprocessing(
            postprocess_job_dir=project_dir / postprocess_job_dir,
            refine_job_dir=Path(refine_params.refine_job_dir),
            mask_file=project_dir / mask_job_dir / "mask.mrc",
            refine_params=refine_params,
        )
        if not job_is_rerun:
            (postprocess_job_dir / "RELION_JOB_EXIT_SUCCESS").unlink()

        # Register the post-processing job with the node creator
        self.log.info(f"Sending {self.postprocess_job_type} to node creator")
        self.recwrap.send_to("node_creator", node_creator_postprocess)

        # End here if the command failed
        if postprocess_result.returncode:
            self.log.error(
                "Refinement post-process failed with exitcode "
                f"{postprocess_result.returncode}:\n"
                + postprocess_result.stderr.decode("utf8", "replace")
            )
            return False

        ###############################################################################
        # Get the statistics
        postprocess_lines = postprocess_result.stdout.decode("utf8", "replace").split(
            "\n"
        )
        final_bfactor = None
        final_resolution = None
        for line in postprocess_lines:
            if "+ apply b-factor of:" in line:
                final_bfactor = float(line.split()[-1])
            elif "+ FINAL RESOLUTION:" in line:
                final_resolution = float(line.split()[-1])

        self.log.info(
            f"Final results: bfactor {final_bfactor} and resolution {final_resolution} "
            f"for {number_of_particles} particles."
        )
        if not final_bfactor or not final_resolution:
            self.log.error(
                f"Unable to read bfactor and resolution for {refine_params.refine_job_dir}"
            )
            return False

        ###############################################################################
        # Send refinement job information to ispyb
        ispyb_parameters = []
        # Construct a bfactor group in the classification group table
        refined_grp_ispyb_parameters = {
            "ispyb_command": "buffer",
            "buffer_command": {"ispyb_command": "insert_particle_classification_group"},
            "type": "refine",
            "batch_number": "1",
            "number_of_particles_per_batch": number_of_particles,
            "number_of_classes_per_batch": "1",
            "symmetry": refine_params.symmetry,
            "particle_picker_id": refine_params.picker_id,
        }
        if job_is_rerun:
            # If this job overwrites another get the id for it
            refined_grp_ispyb_parameters["buffer_lookup"] = {
                "particle_classification_group_id": refine_params.refined_grp_uuid,
            }
        else:
            refined_grp_ispyb_parameters[
                "buffer_store"
            ] = refine_params.refined_grp_uuid
        ispyb_parameters.append(refined_grp_ispyb_parameters)

        # Send individual classes to ispyb
        class_star_file = cif.read_file(
            f"{refine_params.refine_job_dir}/run_model.star"
        )
        classes_block = class_star_file.find_block("model_classes")
        classes_loop = classes_block.find_loop("_rlnReferenceImage").get_loop()

        refined_ispyb_parameters = {
            "ispyb_command": "buffer",
            "buffer_lookup": {
                "particle_classification_group_id": refine_params.refined_grp_uuid
            },
            "buffer_command": {"ispyb_command": "insert_particle_classification"},
            "class_number": refine_params.class_number,
            "class_image_full_path": f"{project_dir}/{postprocess_job_dir}/postprocess.mrc",
            "particles_per_class": number_of_particles,
            "class_distribution": 1,
            "rotation_accuracy": classes_loop.val(0, 2),
            "translation_accuracy": classes_loop.val(0, 3),
            "estimated_resolution": final_resolution,
            "selected": "1",
        }
        if job_is_rerun:
            refined_ispyb_parameters["buffer_lookup"].update(
                {
                    "particle_classification_id": refine_params.refined_class_uuid,
                }
            )
        else:
            refined_ispyb_parameters["buffer_store"] = refine_params.refined_class_uuid

        # Add the resolution and fourier completeness if they are valid numbers
        estimated_resolution = float(classes_loop.val(0, 4))
        if np.isfinite(estimated_resolution):
            refined_ispyb_parameters["estimated_resolution"] = estimated_resolution
        else:
            refined_ispyb_parameters["estimated_resolution"] = 0.0
        fourier_completeness = float(classes_loop.val(0, 5))
        if np.isfinite(fourier_completeness):
            refined_ispyb_parameters[
                "overall_fourier_completeness"
            ] = fourier_completeness
        else:
            refined_ispyb_parameters["overall_fourier_completeness"] = 0.0
        ispyb_parameters.append(refined_ispyb_parameters)

        bfactor_ispyb_parameters = {
            "ispyb_command": "buffer",
            "buffer_lookup": {
                "particle_classification_id": refine_params.refined_class_uuid
            },
            "buffer_command": {"ispyb_command": "insert_bfactor_fit"},
            "resolution": final_resolution,
            "number_of_particles": number_of_particles,
            "particle_batch_size": number_of_particles,
        }
        ispyb_parameters.append(bfactor_ispyb_parameters)

        self.recwrap.send_to(
            "ispyb_connector",
            {
                "ispyb_command": "multipart_message",
                "ispyb_command_list": ispyb_parameters,
            },
        )

        # Tell Murfey the refinement has finished
        murfey_postprocess_params = {
            "register": "done_refinement",
            "project_dir": str(project_dir),
            "resolution": final_resolution,
            "batch_size": number_of_particles,
            "refined_class_uuid": refine_params.refined_class_uuid,
            "class_reference": str(rescaled_class_reference),
            "class_number": refine_params.class_number,
            "mask_file": f"{project_dir}/{mask_job_dir}/mask.mrc",
        }
        self.recwrap.send_to("murfey_feedback", murfey_postprocess_params)

        (postprocess_job_dir / "RELION_JOB_EXIT_SUCCESS").touch(exist_ok=True)
        self.log.info(
            f"Done refinement for {refine_params.class3d_dir} "
            f"with {number_of_particles} particles."
        )
        return True
from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path

import zocalo.wrapper
from pydantic import Field, ValidationError

from relion.zocalo.refine3d import (
    CommonRefineParameters,
    run_postprocessing,
    run_refine3d,
)

logger = logging.getLogger("relion.bfactor.wrapper")


class BFactorParameters(CommonRefineParameters):
    bfactor_directory: str = Field(..., min_length=1)
    class_reference: str = Field(..., min_length=1)
    class_number: int
    number_of_particles: int
    batch_size: int
    pixel_size: float
    mask_file: str
    mask_diameter: float
    refined_class_uuid: int


class BFactorWrapper(zocalo.wrapper.BaseWrapper):
    """
    A wrapper for the calculation of bfactors.
    """

    # Job names
    split_job_type = "relion.select.split"
    refine_job_type = "relion.refine3d"
    postprocess_job_type = "relion.postprocess"

    def run(self):
        """
        Run 3D refinement and postprocessing
        """
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params_dict = self.recwrap.recipe_step["job_parameters"]
        try:
            refine_params = BFactorParameters(**params_dict)
        except (ValidationError, TypeError) as e:
            self.log.warning(
                f"BFactor parameter validation failed for parameters: {params_dict} "
                f"with exception: {e}"
            )
            return False

        # Determine the directory to run in
        project_dir = Path(refine_params.class_reference).parent.parent.parent
        bfactor_dir = Path(refine_params.bfactor_directory)
        (bfactor_dir / "Import/job001").mkdir(parents=True, exist_ok=True)
        os.chdir(bfactor_dir)

        # Link the required files
        class_particles = bfactor_dir / "Import/job001/particles.star"
        class_particles.unlink(missing_ok=True)
        class_particles.symlink_to(
            project_dir
            / f"Select/Refine_class{refine_params.class_number}/particles.star"
        )

        class_reference = bfactor_dir / "Import/job001/refinement_ref.mrc"
        class_reference.unlink(missing_ok=True)
        class_reference.symlink_to(refine_params.class_reference)

        (bfactor_dir / "Extract").unlink(missing_ok=True)
        (bfactor_dir / "Extract").symlink_to(project_dir / "Extract")

        refine_mask_file = bfactor_dir / "Import/job001/mask.mrc"
        refine_mask_file.unlink(missing_ok=True)
        refine_mask_file.symlink_to(refine_params.mask_file)

        self.log.info(
            f"Running bfactor calculation for {refine_params.class_reference} "
            f"with {refine_params.number_of_particles} particles"
        )

        ###############################################################################
        # Split the particles file
        split_job_dir = Path("Select/job002")
        split_job_dir.mkdir(parents=True, exist_ok=True)
        split_command = [
            "relion_star_handler",
            "--i",
            str(class_particles),
            "--o",
            f"{split_job_dir}/particles.star",
            "--split",
            "--random_order",
            "--nr_split",
            "1",
            "--size_split",
            str(refine_params.number_of_particles),
            "--pipeline_control",
            f"{split_job_dir}/",
        ]
        split_result = subprocess.run(
            split_command, cwd=str(bfactor_dir), capture_output=True
        )

        # Register the Selection job with the node creator
        self.log.info(f"Sending {self.split_job_type} to node creator")
        refine_params.relion_options.batch_size = refine_params.number_of_particles
        node_creator_select = {
            "job_type": self.split_job_type,
            "input_file": str(class_particles),
            "output_file": f"{bfactor_dir}/{split_job_dir}/particles_split1.star",
            "relion_options": dict(refine_params.relion_options),
            "command": " ".join(split_command),
            "stdout": split_result.stdout.decode("utf8", "replace"),
            "stderr": split_result.stderr.decode("utf8", "replace"),
        }
        if split_result.returncode:
            node_creator_select["success"] = False
        else:
            node_creator_select["success"] = True
        self.recwrap.send_to("node_creator", node_creator_select)

        # End here if the command failed
        if split_result.returncode:
            self.log.error(
                "Refinement splitting failed with exitcode "
                f"{split_result.returncode}:\n"
                + split_result.stderr.decode("utf8", "replace")
            )
            return False

        ###############################################################################
        # Set up the refinement job
        refine_job_dir = Path("Refine3D/job003")
        refine_job_dir.mkdir(parents=True, exist_ok=True)

        # Run Refine3D and confirm it ran successfully
        refine_result, node_creator_refine = run_refine3d(
            working_dir=bfactor_dir,
            refine_job_dir=bfactor_dir / refine_job_dir,
            particles_file=f"{split_job_dir}/particles_split1.star",
            class_reference=class_reference,
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
        # Do the post-processsing
        postprocess_job_dir = Path("PostProcess/job004")
        postprocess_job_dir.mkdir(parents=True, exist_ok=True)

        postprocess_result, node_creator_postprocess = run_postprocessing(
            working_dir=bfactor_dir,
            postprocess_job_dir=f"{bfactor_dir}/{postprocess_job_dir}",
            refine_job_dir=bfactor_dir / refine_job_dir,
            mask_file=refine_mask_file,
            refine_params=refine_params,
        )

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
            f"for {refine_params.number_of_particles} particles."
        )
        if not final_bfactor or not final_resolution:
            self.log.error(f"Unable to read bfactor and resolution for {bfactor_dir}")
            return False

        ###############################################################################
        # Send refinement job information to ispyb
        ispyb_parameters = {
            "ispyb_command": "buffer",
            "buffer_lookup": {
                "particle_classification_id": refine_params.refined_class_uuid
            },
            "buffer_command": {"ispyb_command": "insert_bfactor_fit"},
            "resolution": final_resolution,
            "number_of_particles": refine_params.number_of_particles,
            "particle_batch_size": refine_params.batch_size,
        }
        self.recwrap.send_to("ispyb_connector", ispyb_parameters)

        # Tell Murfey the refinement has finished
        murfey_postprocess_params = {
            "register": "done_bfactor",
            "resolution": final_resolution,
            "number_of_particles": refine_params.number_of_particles,
            "refined_class_uuid": refine_params.refined_class_uuid,
        }
        self.recwrap.send_to("murfey_feedback", murfey_postprocess_params)

        self.log.info(
            f"Done bfactor for {refine_params.class_reference} "
            f"with {refine_params.number_of_particles} particles."
        )
        return True

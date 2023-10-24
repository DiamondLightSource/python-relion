from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path

import zocalo.wrapper
from pydantic import BaseModel, Field, ValidationError

from relion.zocalo.spa_relion_service_options import RelionServiceOptions

logger = logging.getLogger("relion.refine.wrapper")


class RefineParameters(BaseModel):
    bfactor_directory: str = Field(..., min_length=1)
    class_particles_file: str = Field(..., min_length=1)
    class_number: int
    particle_count: int
    pixel_size: float
    mask_file: str = None
    nr_iter_3d: int = 20
    mpi_run_command: str = "srun -n 5"
    dont_correct_greyscale: bool = True
    ini_high: float = 60.0
    dont_combine_weights_via_disc: bool = True
    nr_pool: int = 10
    pad: int = 2
    do_ctf: bool = True
    ctf_intact_first_peak: bool = False
    flatten_solvent: bool = True
    do_zero_mask: bool = True
    oversampling: int = 1
    healpix_order: int = 2
    local_healpix_order: int = 4
    low_resol_join_halves: int = 40
    offset_range: float = 5
    offset_step: float = 4
    symmetry: str = "C1"
    do_norm: bool = True
    do_scale: bool = True
    threads: int = 8
    gpus: str = "0:1:2:3"
    mask_lowpass: float = 15
    mask_threshold: float = 0.02
    mask_extend: int = 3
    mask_soft_edge: int = 3
    postprocess_lowres: float = 10
    program_id: int
    session_id: int
    relion_options: RelionServiceOptions


class RefineWrapper(zocalo.wrapper.BaseWrapper):
    """
    A wrapper for the Relion 3D refinement pipeline.
    """

    # Job names
    select_job_type = "relion.select.onvalue"
    split_job_type = "relion.select.split"
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
        project_dir = Path(refine_params.class_particles_file).parent.parent.parent
        bfactor_dir = Path(refine_params.bfactor_directory)
        try:
            (bfactor_dir / "Import/job001").mkdir(parents=True)
        except FileExistsError:
            self.log.warning(f"Refinement pipeline run {bfactor_dir} already exists")
            return False
        os.chdir(bfactor_dir)

        # Link the required files
        (bfactor_dir / "Import/job001/particles_data.star").symlink_to(
            refine_params.class_particles_file
        )
        class_reference = bfactor_dir / "Import/job001/refinement_ref.mrc"
        class_reference.symlink_to(
            Path(refine_params.class_particles_file).parent
            / f"run_it{refine_params.nr_iter_3d:03}_class{refine_params.class_number:03}.mrc"
        )
        (bfactor_dir / "Extract").symlink_to(project_dir / "Extract")
        refine_mask_file = bfactor_dir / "Import/job001/mask.mrc"

        ###############################################################################
        # Select the particles from the requested class
        select_job_dir = Path("Select/job002")
        select_job_dir.mkdir(parents=True)
        select_command = [
            "relion_star_handler",
            "--i",
            f"{bfactor_dir}/Import/job001/particles_data.star",
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
            select_command, cwd=str(bfactor_dir), capture_output=True
        )

        # Register the Selection job with the node creator
        self.log.info(f"Sending {self.select_job_type} to node creator")
        refine_params.relion_options.refine_class = refine_params.class_number
        node_creator_parameters = {
            "job_type": self.select_job_type,
            "input_file": f"{bfactor_dir}/Import/job001/particles_data.star",
            "output_file": f"{bfactor_dir}/{select_job_dir}/particles.star",
            "relion_options": dict(refine_params.relion_options),
            "command": " ".join(select_command),
            "stdout": select_result.stdout.decode("utf8", "replace"),
            "stderr": select_result.stderr.decode("utf8", "replace"),
        }
        if select_result.returncode:
            node_creator_parameters["success"] = False
        else:
            node_creator_parameters["success"] = True
        self.recwrap.send_to("node_creator", node_creator_parameters)

        # End here if the command failed
        if select_result.returncode:
            self.log.error(
                "Refinement selection failed with exitcode "
                f"{select_result.returncode}:\n"
                + select_result.stderr.decode("utf8", "replace")
            )
            return False

        ###############################################################################
        # Split the particles file
        split_job_dir = Path("Select/job003")
        split_job_dir.mkdir(parents=True)
        split_command = [
            "relion_star_handler",
            "--i",
            f"{select_job_dir}/particles.star",
            "--o",
            f"{split_job_dir}/particles.star",
            "--split",
            "--random_order",
            "--nr_split",
            "1",
            "--split_size",
            str(refine_params.particle_count),
            "--pipeline_control",
            f"{split_job_dir}/",
        ]
        split_result = subprocess.run(
            split_command, cwd=str(bfactor_dir), capture_output=True
        )

        # Register the Selection job with the node creator
        self.log.info(f"Sending {self.split_job_type} to node creator")
        refine_params.relion_options.batch_size = refine_params.particle_count
        node_creator_parameters = {
            "job_type": self.split_job_type,
            "input_file": f"{bfactor_dir}/{select_job_dir}/particles.star",
            "output_file": f"{bfactor_dir}/{split_job_dir}/particles_split1.star",
            "relion_options": dict(refine_params.relion_options),
            "command": " ".join(split_command),
            "stdout": split_result.stdout.decode("utf8", "replace"),
            "stderr": split_result.stderr.decode("utf8", "replace"),
        }
        if split_result.returncode:
            node_creator_parameters["success"] = False
        else:
            node_creator_parameters["success"] = True
        self.recwrap.send_to("node_creator", node_creator_parameters)

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
        refine_job_dir = Path("Refine3D/job004")
        refine_job_dir.mkdir(parents=True)

        refine_flags = {
            "dont_correct_greyscale": "--firstiter_cc",
            "ini_high": "--ini_high",
            "dont_combine_weights_via_disc": "--dont_combine_weights_via_disc",
            "nr_pool": "--pool",
            "pad": "--pad",
            "do_ctf": "--ctf",
            "ctf_intact_first_peak": "--ctf_intact_first_peak",
            "flatten_solvent": "--flatten_solvent",
            "do_zero_mask": "--zero_mask",
            "oversampling": "--oversampling",
            "healpix_order": "--healpix_order",
            "local_healpix_order": "--auto_local_healpix_order",
            "low_resol_join_halves": "--low_resol_join_halves",
            "offset_range": "--offset_range",
            "offset_step": "--offset_step",
            "symmetry": "--sym",
            "do_norm": "--norm",
            "do_scale": "--scale",
            "threads": "--j",
            "gpus": "--gpu",
        }

        refine_command = refine_params.mpi_run_command.split()
        refine_command.extend(
            [
                "relion_refine_mpi",
                "--i",
                f"{split_job_dir}/particles_split1.star",
                "--o",
                f"{refine_job_dir}/run",
                "--ref",
                str(class_reference),
                "--particle_diameter",
                f"{refine_params.relion_options.mask_diameter}",
                "--auto_refine",
                "--split_random_halves",
            ]
        )
        for k, v in refine_params.dict().items():
            if v and (k in refine_flags):
                if type(v) is bool:
                    refine_command.append(refine_flags[k])
                else:
                    refine_command.extend((refine_flags[k], str(v)))
        refine_command.extend(("--pipeline_control", f"{refine_job_dir}/"))

        # Run Refine3D and confirm it ran successfully
        self.log.info(" ".join(refine_command))
        refine_result = subprocess.run(
            refine_command, cwd=str(bfactor_dir), capture_output=True
        )

        # Register the Refine3D job with the node creator
        self.log.info(f"Sending {self.refine_job_type} to node creator")
        node_creator_parameters = {
            "job_type": self.refine_job_type,
            "input_file": f"{bfactor_dir}/{select_job_dir}/particles_split1.star:{class_reference}",
            "output_file": f"{bfactor_dir}/{refine_job_dir}/",
            "relion_options": dict(refine_params.relion_options),
            "command": " ".join(refine_command),
            "stdout": refine_result.stdout.decode("utf8", "replace"),
            "stderr": refine_result.stderr.decode("utf8", "replace"),
        }
        if refine_result.returncode:
            node_creator_parameters["success"] = False
        else:
            node_creator_parameters["success"] = True
        self.recwrap.send_to("node_creator", node_creator_parameters)

        # End here if the command failed
        if refine_result.returncode:
            self.log.error(
                "Refinement Refine3D failed with exitcode "
                f"{refine_result.returncode}:\n"
                + refine_result.stderr.decode("utf8", "replace")
            )
            return False

        ###############################################################################
        # Do the mask creation if one is not provided
        if refine_params.mask_file:
            refine_mask_file.symlink_to(refine_params.mask_file)
        else:
            mask_job_dir = Path("Mask/job005")
            mask_job_dir.mkdir(parents=True)
            mask_command = [
                "relion_mask_create",
                "--i",
                f"{refine_job_dir}/run_class001.mrc",
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
                mask_command, cwd=str(bfactor_dir), capture_output=True
            )

            # Register the mask creation job with the node creator
            self.log.info(f"Sending {self.mask_job_type} to node creator")
            refine_params.relion_options.angpix = refine_params.pixel_size
            node_creator_parameters = {
                "job_type": self.mask_job_type,
                "input_file": f"{bfactor_dir}/{refine_job_dir}/run_class001.mrc",
                "output_file": f"{bfactor_dir}/{mask_job_dir}/mask.mrc",
                "relion_options": dict(refine_params.relion_options),
                "command": " ".join(mask_command),
                "stdout": mask_result.stdout.decode("utf8", "replace"),
                "stderr": mask_result.stderr.decode("utf8", "replace"),
            }
            if mask_result.returncode:
                node_creator_parameters["success"] = False
            else:
                node_creator_parameters["success"] = True
            self.recwrap.send_to("node_creator", node_creator_parameters)

            # End here if the command failed
            if mask_result.returncode:
                self.log.error(
                    "Refinement mask creation failed with exitcode "
                    f"{mask_result.returncode}:\n"
                    + mask_result.stderr.decode("utf8", "replace")
                )
                return False

            # Link mask file to newly created mask
            refine_mask_file.symlink_to(f"{bfactor_dir}/{mask_job_dir}/mask.mrc")

            # Send the mask to murfey
            murfey_mask_params = {
                "register": "save_mask_file",
                "mask_file": f"{bfactor_dir}/{mask_job_dir}/mask.mrc",
                "program_id": refine_params.program_id,
                "session_id": refine_params.session_id,
            }
            self.recwrap.send_to("murfey_feedback", murfey_mask_params)

        ###############################################################################
        # Do the post-processsing
        postprocess_job_number = 5 if refine_params.mask_file else 6
        postprocess_job_dir = Path(f"PostProcess/job{postprocess_job_number:03}")
        postprocess_job_dir.mkdir(parents=True)
        postprocess_command = [
            "relion_postprocess",
            "--i",
            f"{refine_job_dir}/run_half1_class001_unfil.mrc",
            "--o",
            f"{postprocess_job_dir}/postprocess",
            "--mask",
            str(refine_mask_file),
            "--angpix",
            str(refine_params.pixel_size),
            "--auto_bfac",
            "--autob_lowres",
            str(refine_params.postprocess_lowres),
            "--pipeline_control",
            f"{postprocess_job_dir}/",
        ]
        postprocess_result = subprocess.run(
            postprocess_command, cwd=str(bfactor_dir), capture_output=True
        )

        # Register the post-processing job with the node creator
        self.log.info(f"Sending {self.postprocess_job_type} to node creator")
        node_creator_parameters = {
            "job_type": self.postprocess_job_type,
            "input_file": f"{bfactor_dir}/{refine_job_dir}/run_half1_class001_unfil.mrc:{refine_mask_file}",
            "output_file": f"{bfactor_dir}/{postprocess_job_dir}/postprocess.mrc",
            "relion_options": dict(refine_params.relion_options),
            "command": " ".join(postprocess_command),
            "stdout": postprocess_result.stdout.decode("utf8", "replace"),
            "stderr": postprocess_result.stderr.decode("utf8", "replace"),
        }
        if postprocess_result.returncode:
            node_creator_parameters["success"] = False
        else:
            node_creator_parameters["success"] = True
        self.recwrap.send_to("node_creator", node_creator_parameters)

        # End here if the command failed
        if postprocess_result.returncode:
            self.log.error(
                "Refinement post-process failed with exitcode "
                f"{postprocess_result.returncode}:\n"
                + postprocess_result.stderr.decode("utf8", "replace")
            )
            return False

        ###############################################################################
        # Get the statistics and tell Murfey the refinement has finished
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

        if not final_bfactor or not final_resolution:
            self.log.error(f"Unable to read bfactor and resolution for {bfactor_dir}")
            return False

        murfey_postprocess_params = {
            "register": "done_refinement",
            "particle_count": refine_params.particle_count,
            "bfactor": final_bfactor,
            "resolution": final_resolution,
            "program_id": refine_params.program_id,
            "session_id": refine_params.session_id,
        }
        self.recwrap.send_to("murfey_feedback", murfey_postprocess_params)

        self.log.info(f"Done refinement for {refine_params.class_particles_file}.")
        return True

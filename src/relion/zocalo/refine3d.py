from __future__ import annotations

import subprocess
from pathlib import Path

from pydantic import BaseModel

from relion.zocalo.spa_relion_service_options import RelionServiceOptions

refine_job_type = "relion.refine3d"
postprocess_job_type = "relion.postprocess"


class CommonRefineParameters(BaseModel):
    pixel_size: float
    mask_diameter: float
    mpi_run_command: str = "srun -n 5"
    dont_correct_greyscale: bool = True
    ini_high: float = 50.0
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
    offset_step: float = 2
    ignore_angles: bool = False
    resol_angles: bool = False
    symmetry: str = "C1"
    do_norm: bool = True
    do_scale: bool = True
    threads: int = 8
    gpus: str = "0:1:2:3"
    postprocess_lowres: float = 10
    relion_options: RelionServiceOptions


def run_refine3d(
    refine_job_dir: Path,
    particles_file: Path,
    class_reference: Path,
    refine_params: CommonRefineParameters,
):
    """Run a 3D Relion refinement job
    Parameters:
        refine_job_dir: Directory in which to run the job
        particles_file: Input particles star file to refine
        class_reference: Reference class mrc to use
        refine_params: Job parameters to send to Relion
    """
    refine_command = refine_params.mpi_run_command.split()
    refine_command.extend(
        [
            "relion_refine_mpi",
            "--i",
            str(particles_file),
            "--o",
            f"{refine_job_dir}/run",
            "--ref",
            str(class_reference),
            "--particle_diameter",
            f"{refine_params.mask_diameter}",
            "--auto_refine",
            "--split_random_halves",
        ]
    )

    # Add flags to the command based on the input parameters
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
        "ignore_angles": "--auto_ignore_angles",
        "resol_angles": "--auto_resol_angles",
        "symmetry": "--sym",
        "do_norm": "--norm",
        "do_scale": "--scale",
        "threads": "--j",
        "gpus": "--gpu",
    }
    for k, v in refine_params.dict().items():
        if v and (k in refine_flags):
            if type(v) is bool:
                refine_command.append(refine_flags[k])
            else:
                refine_command.extend((refine_flags[k], str(v)))
    refine_command.extend(("--pipeline_control", f"{refine_job_dir}/"))

    # Run Refine3D and confirm it ran successfully
    refine_result = subprocess.run(refine_command, capture_output=True)

    # Register the Refine3D job with the node creator
    node_creator_parameters = {
        "job_type": refine_job_type,
        "input_file": f"{particles_file}:{class_reference}",
        "output_file": f"{refine_job_dir}/",
        "relion_options": dict(refine_params.relion_options),
        "command": " ".join(refine_command),
        "stdout": refine_result.stdout.decode("utf8", "replace"),
        "stderr": refine_result.stderr.decode("utf8", "replace"),
    }
    if refine_result.returncode:
        node_creator_parameters["success"] = False
    else:
        node_creator_parameters["success"] = True

    return refine_result, node_creator_parameters


def run_postprocessing(
    postprocess_job_dir: Path,
    refine_job_dir: Path,
    mask_file: Path,
    refine_params: CommonRefineParameters,
):
    """Run Relion postprocessing on a refinement job
    Parameters:
        postprocess_job_dir: Directory in which to run the job
        refine_job_dir: Directory of the refinement job to run postprocessing on
        mask_file: Mask mrc file to apply
        refine_params: Job parameters to send to Relion
    """
    postprocess_command = [
        "relion_postprocess",
        "--i",
        f"{refine_job_dir}/run_half1_class001_unfil.mrc",
        "--o",
        f"{postprocess_job_dir}/postprocess",
        "--mask",
        str(mask_file),
        "--angpix",
        str(refine_params.pixel_size),
        "--auto_bfac",
        "--autob_lowres",
        str(refine_params.postprocess_lowres),
        "--pipeline_control",
        f"{postprocess_job_dir}/",
    ]
    postprocess_result = subprocess.run(postprocess_command, capture_output=True)

    # Register the post-processing job with the node creator
    node_creator_parameters = {
        "job_type": postprocess_job_type,
        "input_file": f"{refine_job_dir}/run_half1_class001_unfil.mrc:{mask_file}",
        "output_file": f"{postprocess_job_dir}/postprocess.mrc",
        "relion_options": dict(refine_params.relion_options),
        "command": " ".join(postprocess_command),
        "stdout": postprocess_result.stdout.decode("utf8", "replace"),
        "stderr": postprocess_result.stderr.decode("utf8", "replace"),
    }
    if postprocess_result.returncode:
        node_creator_parameters["success"] = False
    else:
        node_creator_parameters["success"] = True

    return postprocess_result, node_creator_parameters

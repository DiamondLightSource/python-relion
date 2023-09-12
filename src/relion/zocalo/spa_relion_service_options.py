from __future__ import annotations

import math
from pathlib import Path
from typing import Any, Dict

from pydantic import BaseModel, root_validator


def calculate_box_size(particle_size_pixels):
    # Use box 20% larger than particle and ensure size is even
    box_size_exact = 1.2 * particle_size_pixels
    box_size_int = int(math.ceil(box_size_exact))
    return box_size_int + box_size_int % 2


def calculate_downscaled_box_size(box_size_pix, angpix):
    for small_box_pix in (
        48,
        64,
        96,
        128,
        160,
        192,
        256,
        288,
        300,
        320,
        360,
        384,
        400,
        420,
        450,
        480,
        512,
        640,
        768,
        896,
        1024,
    ):
        # Don't go larger than the original box
        if small_box_pix > box_size_pix:
            return box_size_pix
        # If Nyquist freq. is better than 8.5 A, use this downscaled box, else step size
        small_box_angpix = angpix * box_size_pix / small_box_pix
        if small_box_angpix < 4.25:
            return small_box_pix
    # Fall back to a warning message
    return "Box size is too large!"


class RelionServiceOptions(BaseModel):
    """The parameters used by the Relion services"""

    """Parameters that Murfey will set"""
    # Pixel size in Angstroms in the input movies
    angpix: float = 0.885
    # Dose in electrons per squared Angstrom per frame
    dose_per_frame: float = 1.277
    # Gain-reference image in MRC format
    gain_ref: str = "Movies/gain.mrc"
    # Acceleration voltage (in kV)
    voltage: int = 300
    # Use binning=2 for super-resolution K2 movies
    motion_corr_binning: int = 1
    # eer format grouping
    eer_grouping: int = 20
    # Symmetry group
    symmetry: str = "C1"
    # Down-scale the particles upon extraction?
    downscale: bool = False
    # Run icebreaker?
    do_icebreaker_jobs: bool = True

    """Parameters used in internal calculations"""
    pixel_size_downscaled: float = 0
    # Diameter of particles picked by cryolo
    particle_diameter: float = 0
    # Box size of particles in the averaged micrographs (in pixels)
    boxsize: int = 256
    # Box size of the down-scaled particles (in pixels)
    small_boxsize: int = 64
    # Diameter of the mask used for 2D/3D classification (in Angstrom)
    mask_diameter: float = 190

    """Parameters we set differently from pipeliner defaults"""
    # Spherical aberration
    spher_aber: float = 2.7
    # Amplitude contrast (Q0)
    ampl_contrast: float = 0.1

    # Local motion-estimation patches for MotionCor2
    motioncor_patches_x: int = 5
    motioncor_patches_y: int = 5

    # Additional arguments for RELION's Motion Correction wrapper
    motioncor_other_args: str = "--do_at_most 200 --skip_logfile"
    # Threshold for cryolo autopicking
    cryolo_threshold: float = 0.15
    # Location of the cryolo specific files
    cryolo_config: str = "/dls_sw/apps/EM/crYOLO/phosaurus_models/config.json"
    cryolo_gmodel: str = (
        "/dls_sw/apps/EM/crYOLO/phosaurus_models/gmodel_phosnet_202005_N63_c17.h5"
    )

    # Fraction of classes to attempt to remove using the RELION 2D class ranker
    class2d_fraction_of_classes_to_remove: float = 0.9
    # 2D classification particle batch size
    batch_size: int = 50000
    # Maximum batch size for the single batch of 3D classification
    class3d_max_size: int = 200000
    # Initial models to generate
    inimodel_nr_classes: int = 4
    # Initial lowpass filter on 3D reference
    class3d_ini_lowpass: int = 40

    # Classification batches and iteration counts
    class2d_nr_classes: int = 50
    class2d_nr_iter: int = 25
    class3d_nr_classes: int = 4
    class3d_nr_iter: int = 25

    class Config:
        validate_assignment = True

    @root_validator(skip_on_failure=True)
    def if_particle_diameter_compute_box_sizes(cls, values):
        if values.get("particle_diameter"):
            values["mask_diameter"] = 1.1 * values["particle_diameter"]
            values["boxsize"] = calculate_box_size(
                values["particle_diameter"] / values["angpix"]
            )
            values["small_boxsize"] = calculate_downscaled_box_size(
                values["boxsize"], values["angpix"]
            )
        return values


def generate_service_options(
    relion_options: RelionServiceOptions, submission_type: str
) -> dict:
    job_options: Dict[str, Any] = {}
    queue_options = {
        "do_queue": "No",
        "qsubscript": "run_as_service",
    }

    job_options["relion.import.movies"] = {
        "angpix": relion_options.angpix,
        "kV": relion_options.voltage,
    }

    job_options["relion.motioncorr.own"] = {
        "dose_per_frame": relion_options.dose_per_frame,
        "fn_gain_ref": relion_options.gain_ref
        if Path(relion_options.gain_ref).exists()
        else "",
        "eer_grouping": relion_options.eer_grouping,
        "patch_x": relion_options.motioncor_patches_x,
        "patch_y": relion_options.motioncor_patches_y,
        "bin_factor": relion_options.motion_corr_binning,
        "other_args": f"{relion_options.motioncor_other_args}",
        "nr_mpi": 4,
        "nr_threads": 10,
    }

    job_options["relion.motioncorr.motioncor2"] = {
        **job_options["relion.motioncorr.own"],
        "fn_motioncor2_exe": "MotionCor2",
        "gpu_ids": "0:1:2:3",
    }

    job_options["icebreaker.micrograph_analysis.micrographs"] = {"nr_threads": "10"}

    job_options["icebreaker.micrograph_analysis.enhancecontrast"] = {"nr_threads": "10"}

    job_options["icebreaker.micrograph_analysis.summary"] = {"nr_threads": "10"}

    job_options["icebreaker.micrograph_analysis.particles"] = {"nr_threads": "10"}

    job_options["relion.ctffind.ctffind4"] = {"nr_mpi": 40}

    job_options["cryolo.autopick"] = {
        "model_path": relion_options.cryolo_gmodel,
        "config_file": relion_options.cryolo_config,
        "box_size": relion_options.boxsize,
        "confidence_threshold": relion_options.cryolo_threshold,
        "gpus": "0 1 2 3",
    }

    job_options["relion.extract"] = {
        "bg_diameter": -1,
        "extract_size": relion_options.boxsize,
        "do_rescale": relion_options.downscale,
        "rescale": relion_options.small_boxsize,
        "nr_mpi": 40,
    }

    job_options["relion.select.split"] = {
        "split_size": relion_options.batch_size,
    }

    job_options["relion.class2d.em"] = {
        "nr_classes": relion_options.class2d_nr_classes,
        "nr_iter_em": relion_options.class2d_nr_iter,
        "do_preread_images": True,
        "particle_diameter": relion_options.mask_diameter,
        "use_gpu": True,
        "gpu_ids": "",
        "nr_mpi": 5,
        "nr_threads": 8,
    }

    job_options["relion.class2d.vdam"] = {
        "nr_classes": relion_options.class2d_nr_classes,
        "nr_iter_grad": relion_options.class2d_nr_iter,
        "do_preread_images": True,
        "particle_diameter": relion_options.mask_diameter,
        "use_gpu": True,
        "gpu_ids": "0,1,2,3",
        "nr_mpi": 1,
        "nr_threads": 12,
    }

    job_options["relion.select.class2dauto"] = {
        "python_exe": "/dls_sw/apps/EM/relion/4.0/conda/bin/python",
        "rank_threshold": 0.5,
        "other_args": "--select_min_nr_particles 500",
    }

    job_options["combine_star_files_job"] = {
        "do_split": True,
        "split_size": relion_options.batch_size,
    }

    job_options["relion.initialmodel"] = {
        "nr_classes": relion_options.inimodel_nr_classes,
        "sym_name": relion_options.symmetry,
        "particle_diameter": relion_options.mask_diameter,
        "do_preread_images": True,
        "nr_pool": 10,
        "use_gpu": True,
        "gpu_ids": "0,1,2,3",
        "nr_threads": 12,
    }

    job_options["relion.class3d"] = {
        "nr_classes": relion_options.class3d_nr_classes,
        "nr_iter": relion_options.class3d_nr_iter,
        "sym_name": relion_options.symmetry,
        "ini_high": relion_options.class3d_ini_lowpass,
        "particle_diameter": relion_options.mask_diameter,
        "do_preread_images": True,
        "use_gpu": True,
        "gpu_ids": "",
        "nr_mpi": 5,
        "nr_threads": 8,
    }

    if submission_type not in ["relion.import.movies", "combine_star_files_job"]:
        job_options[submission_type].update(queue_options)
    return job_options[submission_type]


service_options = RelionServiceOptions()
service_values = {
    service_options.do_icebreaker_jobs,
    service_options.cryolo_threshold,
    service_options.pixel_size_downscaled,
    service_options.angpix,
    service_options.voltage,
    service_options.spher_aber,
    service_options.ampl_contrast,
    service_options.batch_size,
    service_options.class2d_fraction_of_classes_to_remove,
}

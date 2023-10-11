from __future__ import annotations

import re
from pathlib import Path
from typing import Callable, Dict

from gemmi import cif

from relion.zocalo.spa_relion_service_options import RelionServiceOptions

NODE_PARTICLESDATA = "ParticlesData"


def get_optics_table(
    relion_options: RelionServiceOptions, particle: bool = False, im_size: int = 0
):
    """
    Create the optics table for micrograph or particle star files.
    Particle files contain additional rows describing the extracted images.
    """
    output_cif = cif.Document()
    data_optics = output_cif.add_new_block("optics")

    new_angpix = (
        str(relion_options.pixel_size_downscaled)
        if relion_options.pixel_size_downscaled
        else str(relion_options.angpix)
    )

    optics_columns = [
        "OpticsGroupName",
        "OpticsGroup",
        "MicrographOriginalPixelSize",
        "Voltage",
        "SphericalAberration",
        "AmplitudeContrast",
    ]
    if particle:
        optics_columns.extend(
            [
                "ImagePixelSize",
                "ImageSize",
                "ImageDimensionality",
                "CtfDataAreCtfPremultiplied",
            ]
        )
    else:
        optics_columns.append("MicrographPixelSize")

    optics_values = [
        "opticsGroup1",
        "1",
        str(relion_options.angpix),
        str(relion_options.voltage),
        str(relion_options.spher_aber),
        str(relion_options.ampl_contrast),
    ]
    if particle:
        optics_values.extend([new_angpix, str(im_size), "2", "0"])
    else:
        optics_values.append(new_angpix)

    optics_loop = data_optics.init_loop("_rln", optics_columns)
    optics_loop.add_row(optics_values)
    return output_cif


def _import_output_files(
    job_dir: Path,
    input_file: Path,
    output_file: Path,
    relion_options: RelionServiceOptions,
    results: dict,
):
    """Import jobs save a list of all micrographs"""
    star_file = job_dir / "movies.star"

    # Read and append to the existing output file, or otherwise create one
    if not star_file.exists():
        output_cif = get_optics_table(relion_options)

        data_movies = output_cif.add_new_block("movies")
        movies_loop = data_movies.init_loop(
            "_rln", ["MicrographMovieName", "OpticsGroup"]
        )
    else:
        output_cif = cif.read_file(str(star_file))
        data_movies = output_cif.find_block("movies")
        movies_loop = data_movies.find_loop("_rlnMicrographMovieName").get_loop()

    movies_loop.add_row([str(output_file), "1"])
    output_cif.write_file(str(star_file), style=cif.Style.Simple)


def _motioncorr_output_files(
    job_dir: Path,
    input_file: Path,
    output_file: Path,
    relion_options: RelionServiceOptions,
    results: dict,
):
    """Motion correction saves a list of micrographs and their motion"""
    star_file = job_dir / "corrected_micrographs.star"

    # Read and append to the existing output file, or otherwise create one
    if not star_file.exists():
        output_cif = get_optics_table(relion_options)

        data_movies = output_cif.add_new_block("micrographs")
        movies_loop = data_movies.init_loop(
            "_rln",
            [
                "MicrographName",
                "MicrographMetadata",
                "OpticsGroup",
                "AccumMotionTotal",
                "AccumMotionEarly",
                "AccumMotionLate",
            ],
        )
    else:
        output_cif = cif.read_file(str(star_file))
        data_movies = output_cif.find_block("micrographs")
        movies_loop = data_movies.find_loop("_rlnMicrographName").get_loop()

    movies_loop.add_row(
        [
            str(output_file),
            str(output_file.with_suffix(".star")),
            "1",
            str(results["total_motion"]),
            str(results["early_motion"]),
            str(results["late_motion"]),
        ]
    )
    output_cif.write_file(str(star_file), style=cif.Style.Simple)

    # Logfile is expected but will not be made
    (star_file.parent / "logfile.pdf").touch()


def _ctffind_output_files(
    job_dir: Path,
    input_file: Path,
    output_file: Path,
    relion_options: RelionServiceOptions,
    results: dict,
):
    """Ctf estimation saves a list of micrographs and their ctf parameters"""
    star_file = job_dir / "micrographs_ctf.star"

    # Read and append to the existing output file, or otherwise create one
    if not star_file.exists():
        output_cif = get_optics_table(relion_options)

        data_movies = output_cif.add_new_block("micrographs")
        movies_loop = data_movies.init_loop(
            "_rln",
            [
                "MicrographName",
                "OpticsGroup",
                "CtfImage",
                "DefocusU",
                "DefocusV",
                "CtfAstigmatism",
                "DefocusAngle",
                "CtfFigureOfMerit",
                "CtfMaxResolution",
            ],
        )
    else:
        output_cif = cif.read_file(str(star_file))
        data_movies = output_cif.find_block("micrographs")
        movies_loop = data_movies.find_loop("_rlnMicrographName").get_loop()

    # Results needed in the star file are stored in a txt file with the output
    with open(output_file.with_suffix(".txt"), "r") as f:
        ctf_results = f.readlines()[-1].split()

    movies_loop.add_row(
        [
            str(input_file),
            "1",
            str(output_file.with_suffix(".ctf")) + ":mrc",
            ctf_results[1],
            ctf_results[2],
            str(abs(float(ctf_results[1]) - float(ctf_results[2]))),
            ctf_results[3],
            ctf_results[5],
            ctf_results[6],
        ]
    )
    output_cif.write_file(str(star_file), style=cif.Style.Simple)

    # Logfile is expected but will not be made
    (star_file.parent / "logfile.pdf").touch()


def _icebreaker_output_files(
    job_dir: Path,
    input_file: Path,
    output_file: Path,
    relion_options: RelionServiceOptions,
    results: dict,
):

    if results["icebreaker_type"] != "particles":
        # Micrograph icebreaker jobs need a file listing the completed micrographs
        with open(job_dir / "done_mics.txt", "a+") as f:
            f.write(input_file.name + "\n")

    if results["icebreaker_type"] == "micrographs":
        # Micrograph jobs save a list of micrographs and their motion
        star_file = job_dir / "grouped_micrographs.star"
        file_to_add = (
            str(
                Path(re.sub(".+/job[0-9]{3}", str(output_file), str(input_file))).parent
                / input_file.stem
            )
            + "_grouped.mrc"
        )
    elif results["icebreaker_type"] == "enhancecontrast":
        # Contrast enhancement jobs save a list of micrographs and their motion
        star_file = job_dir / "flattened_micrographs.star"
        file_to_add = (
            str(
                Path(re.sub(".+/job[0-9]{3}", str(output_file), str(input_file))).parent
                / input_file.stem
            )
            + "_flattened.mrc"
        )
    elif results["icebreaker_type"] == "summary":
        csv_file = job_dir / "five_figs_test.csv"
        if not csv_file.exists():
            with open(csv_file, "w") as f:
                f.write("path,min,q1,q2=median,q3,max\n")
        with open(csv_file, "a+") as f:
            f.write(f"{input_file}," + ",".join(results["summary"]) + "\n")
        return
    else:
        # Nothing to do for particles job
        return

    # Read and append to the existing output file, or otherwise create one
    if not star_file.exists():
        output_cif = cif.Document()

        data_movies = output_cif.add_new_block("micrographs")
        movies_loop = data_movies.init_loop(
            "_rln",
            [
                "MicrographName",
                "MicrographMetadata",
                "OpticsGroup",
                "AccumMotionTotal",
                "AccumMotionEarly",
                "AccumMotionLate",
            ],
        )
    else:
        output_cif = cif.read_file(str(star_file))
        data_movies = output_cif.find_block("micrographs")
        movies_loop = data_movies.find_loop("_rlnMicrographName").get_loop()

    movies_loop.add_row(
        [
            file_to_add,
            str(input_file.with_suffix(".star")),
            "1",
            str(results["total_motion"]),
            str(results["early_motion"]),
            str(results["late_motion"]),
        ]
    )
    output_cif.write_file(str(star_file), style=cif.Style.Simple)


def _cryolo_output_files(
    job_dir: Path,
    input_file: Path,
    output_file: Path,
    relion_options: RelionServiceOptions,
    results: dict,
):
    """Cryolo jobs save a list of micrographs and files with particle coordinates"""
    star_file = job_dir / "autopick.star"

    # Read and append to the existing output file, or otherwise create one
    if not star_file.exists():
        output_cif = cif.Document()

        data_movies = output_cif.add_new_block("coordinate_files")
        movies_loop = data_movies.init_loop(
            "_rln",
            [
                "MicrographName",
                "MicrographCoordinates",
            ],
        )
    else:
        output_cif = cif.read_file(str(star_file))
        data_movies = output_cif.find_block("coordinate_files")
        movies_loop = data_movies.find_loop("_rlnMicrographName").get_loop()

    movies_loop.add_row([str(input_file), str(output_file)])
    output_cif.write_file(str(star_file), style=cif.Style.Simple)


def _extract_output_files(
    job_dir: Path,
    input_file: Path,
    output_file: Path,
    relion_options: RelionServiceOptions,
    results: dict,
):
    """Extract jobs save a list of particle coordinates"""
    star_file = job_dir / "particles.star"

    # Read and append to the existing output file, or otherwise create one
    if not star_file.exists():
        output_cif = get_optics_table(
            relion_options, particle=True, im_size=results["box_size"]
        )

        particles_cif = cif.read_file(str(output_file))
        data_particles = particles_cif.find_block("particles")
        output_cif.add_copied_block(data_particles)
        output_cif.write_file(str(star_file), style=cif.Style.Simple)

    else:
        with open(output_file, "r") as added_cif:
            added_lines = added_cif.readlines()

        with open(star_file, "a") as output_cif:
            for new_row in added_lines:
                if new_row[:1].isdigit():
                    output_cif.write(new_row)


def _select_output_files(
    job_dir: Path,
    input_file: Path,
    output_file: Path,
    relion_options: RelionServiceOptions,
    results: dict,
):
    """Select jobs need no further files, but have extra nodes to add"""
    # Find and add all the output files to the node list
    split_count = int(str(output_file).split("split")[1].split(".")[0])
    split_files = {}
    for split_file in range(split_count, 0, -1):
        split_name = f"particles_split{split_file}.star"
        split_files[split_name] = [NODE_PARTICLESDATA, ["relion"]]
    return split_files


def _relion_no_extra_files(
    job_dir: Path,
    input_file: Path,
    output_file: Path,
    relion_options: RelionServiceOptions,
    results: dict,
):
    """Jobs run through relion do not need any extra files written"""
    return


_output_files: Dict[str, Callable] = {
    "relion.import.movies": _import_output_files,
    "relion.motioncorr.motioncor2": _motioncorr_output_files,
    "icebreaker.micrograph_analysis.micrographs": _icebreaker_output_files,
    "icebreaker.micrograph_analysis.enhancecontrast": _icebreaker_output_files,
    "icebreaker.micrograph_analysis.summary": _icebreaker_output_files,
    "relion.ctffind.ctffind4": _ctffind_output_files,
    "cryolo.autopick": _cryolo_output_files,
    "relion.extract": _extract_output_files,
    "relion.select.split": _select_output_files,
    "icebreaker.micrograph_analysis.particles": _icebreaker_output_files,
    "relion.class2d.em": _relion_no_extra_files,
    "relion.select.class2dauto": _relion_no_extra_files,
    "combine_star_files_job": _relion_no_extra_files,
    "relion.initialmodel": _relion_no_extra_files,
    "relion.class3d": _relion_no_extra_files,
}


def create_output_files(
    job_type: str,
    job_dir: Path,
    input_file: Path,
    output_file: Path,
    relion_options: RelionServiceOptions,
    results: dict,
):
    return _output_files[job_type](
        job_dir, input_file, output_file, relion_options, results
    )

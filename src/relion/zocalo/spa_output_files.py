from __future__ import annotations

import re
from pathlib import Path
from typing import Callable, Dict

from gemmi import cif


def _import_output_files(
    job_dir: Path,
    input_file: Path,
    output_file: Path,
    relion_it_options: dict,
    results: dict,
):
    star_file = job_dir / "movies.star"

    # Read the existing output file, or otherwise create one
    if not star_file.exists():
        output_cif = cif.Document()

        data_optics = output_cif.add_new_block("optics")
        optics_loop = data_optics.init_loop(
            "_rln",
            [
                "OpticsGroupName",
                "OpticsGroup",
                "MicrographOriginalPixelSize",
                "Voltage",
                "SphericalAberration",
                "AmplitudeContrast",
            ],
        )
        optics_loop.add_row(
            [
                "opticsGroup1",
                "1",
                str(relion_it_options["angpix"]),
                str(relion_it_options["voltage"]),
                str(relion_it_options["Cs"]),
                str(relion_it_options["ampl_contrast"]),
            ]
        )

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
    relion_it_options: dict,
    results: dict,
):
    star_file = job_dir / "corrected_micrographs.star"

    # Read the existing output file, or otherwise create one
    if not star_file.exists():
        output_cif = cif.Document()

        data_optics = output_cif.add_new_block("optics")
        optics_loop = data_optics.init_loop(
            "_rln",
            [
                "OpticsGroupName",
                "OpticsGroup",
                "MicrographOriginalPixelSize",
                "Voltage",
                "SphericalAberration",
                "AmplitudeContrast",
                "MicrographPixelSize",
            ],
        )
        optics_loop.add_row(
            [
                "opticsGroup1",
                "1",
                str(relion_it_options["angpix"]),
                str(relion_it_options["voltage"]),
                str(relion_it_options["Cs"]),
                str(relion_it_options["ampl_contrast"]),
                str(relion_it_options["angpix"]),
            ]
        )

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
            results["total_motion"],
            "0.0",
            results["total_motion"],
        ]
    )
    output_cif.write_file(str(star_file), style=cif.Style.Simple)

    # Logfile is expected but will not be made
    (star_file.parent / "logfile.pdf").touch()


def _ctffind_output_files(
    job_dir: Path,
    input_file: Path,
    output_file: Path,
    relion_it_options: dict,
    results: dict,
):
    star_file = job_dir / "micrographs_ctf.star"

    # Read the existing output file, or otherwise create one
    if not star_file.exists():
        output_cif = cif.Document()

        data_optics = output_cif.add_new_block("optics")
        optics_loop = data_optics.init_loop(
            "_rln",
            [
                "OpticsGroupName",
                "OpticsGroup",
                "MicrographOriginalPixelSize",
                "Voltage",
                "SphericalAberration",
                "AmplitudeContrast",
                "MicrographPixelSize",
            ],
        )
        optics_loop.add_row(
            [
                "opticsGroup1",
                "1",
                str(relion_it_options["angpix"]),
                str(relion_it_options["voltage"]),
                str(relion_it_options["Cs"]),
                str(relion_it_options["ampl_contrast"]),
                str(relion_it_options["angpix"]),
            ]
        )

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
            str(output_file.with_suffix(".star")) + ":mrc",
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
    relion_it_options: dict,
    results: dict,
):
    if results["icebreaker_type"] == "micrographs":
        star_file = job_dir / "grouped_micrographs.star"
        file_to_add = (
            str(
                Path(re.sub(".+/job[0-9]{3}", str(output_file), str(input_file))).parent
                / input_file.stem
            )
            + "_grouped.mrc"
        )
    elif results["icebreaker_type"] == "enhancecontrast":
        star_file = job_dir / "flattened_micrographs.star"
        file_to_add = (
            str(
                Path(re.sub(".+/job[0-9]{3}", str(output_file), str(input_file))).parent
                / input_file.stem
            )
            + "_flattened.mrc"
        )
    else:
        return

    # Read the existing output file, or otherwise create one
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
            results["total_motion"],
            "0.0",
            results["total_motion"],
        ]
    )
    output_cif.write_file(str(star_file), style=cif.Style.Simple)


def _cryolo_output_files(
    job_dir: Path,
    input_file: Path,
    output_file: Path,
    relion_it_options: dict,
    results: dict,
):
    star_file = job_dir / "autopick.star"

    # Read the existing output file, or otherwise create one
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

    # run.out is expected but will not be made
    (star_file.parent / "run.out").touch()


_output_files: Dict[str, Callable] = {
    "relion.import.movies": _import_output_files,
    "relion.motioncorr.motioncor2": _motioncorr_output_files,
    # "relion.motioncorr.own": _from_import,
    "icebreaker.micrograph_analysis.micrographs": _icebreaker_output_files,
    "icebreaker.micrograph_analysis.enhancecontrast": _icebreaker_output_files,
    "icebreaker.micrograph_analysis.summary": _icebreaker_output_files,
    "relion.ctffind.ctffind4": _ctffind_output_files,
    "cryolo.autopick": _cryolo_output_files,
    # "relion.ctffind.gctf": _from_motioncorr,
    # "relion.autopick.log": _from_ctf,
    # "relion.autopick.ref3d": _from_ctf,
    # "cryolo.autopick": partial(_from_ctf, in_key="input_file"),
    # "relion.extract": _extract,
    # "relion.select.split": _select,
}


def create_output_files(
    job_type: str,
    job_dir: Path,
    input_file: Path,
    output_file: Path,
    relion_it_options: dict,
    results: dict,
):
    _output_files[job_type](
        job_dir, input_file, output_file, relion_it_options, results
    )

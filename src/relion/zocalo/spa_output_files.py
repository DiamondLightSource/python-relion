from __future__ import annotations

from pathlib import Path
from typing import Callable, Dict

from gemmi import cif


def _import_output_files(
    job_dir: Path,
    input_file: Path,
    output_file: Path,
    options: dict,
):
    star_file = job_dir / "movies.star"

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
                str(options["angpix"]),
                str(options["voltage"]),
                str(options["Cs"]),
                str(options["ampl_contrast"]),
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
    options: dict,
):
    star_file = job_dir / "corrected_micrographs.star"

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
                str(options["angpix"]),
                str(options["voltage"]),
                str(options["Cs"]),
                str(options["ampl_contrast"]),
                str(options["angpix"]),
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
            "0.0",
            "0.0",
            "0.0",
        ]
    )
    output_cif.write_file(str(star_file), style=cif.Style.Simple)

    # Logfile is expected but will not be made
    (star_file.parent / "logfile.pdf").touch()


def _ctffind_output_files(
    job_dir: Path,
    input_file: Path,
    output_file: Path,
    options: dict,
):
    star_file = job_dir / "micrographs_ctf.star"

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
                str(options["angpix"]),
                str(options["voltage"]),
                str(options["Cs"]),
                str(options["ampl_contrast"]),
                str(options["angpix"]),
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
            ctf_results[4],
            ctf_results[5],
        ]
    )
    output_cif.write_file(str(star_file), style=cif.Style.Simple)

    # Logfile is expected but will not be made
    (star_file.parent / "logfile.pdf").touch()


_output_files: Dict[str, Callable] = {
    "relion.import.movies": _import_output_files,
    "relion.motioncorr.motioncor2": _motioncorr_output_files,
    # "relion.motioncorr.own": _from_import,
    # "icebreaker.micrograph_analysis.micrographs": partial(
    #    _from_motioncorr, in_key="in_mics"
    # ),
    # "icebreaker.micrograph_analysis.enhancecontrast": partial(
    #    _from_motioncorr, in_key="in_mics"
    # ),
    # "icebreaker.micrograph_analysis.summary": partial(
    #    _from_ib,
    # ),
    "relion.ctffind.ctffind4": _ctffind_output_files,
    # "relion.ctffind.gctf": _from_motioncorr,
    # "relion.autopick.log": _from_ctf,
    # "relion.autopick.ref3d": _from_ctf,
    # "cryolo.autopick": partial(_from_ctf, in_key="input_file"),
    # "relion.extract": _extract,
    # "relion.select.split": _select,
}


def create_output_files(
    job_type: str, job_dir: Path, input_file: Path, output_file: Path, options: dict
):
    _output_files[job_type](job_dir, input_file, output_file, options)

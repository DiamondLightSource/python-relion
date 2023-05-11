from __future__ import annotations

from pathlib import Path
from typing import Callable, Dict

from gemmi import cif


def _import_output_files(job_dir: Path, file_to_add: Path, options: dict):
    output_file = job_dir / "movies.star"

    if not output_file.exists():
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
        output_cif = cif.read_file(str(output_file))
        data_movies = output_cif.find_block("movies")
        movies_loop = data_movies.find_loop("_rlnMicrographMovieName").get_loop()

    movies_loop.add_row([str(file_to_add), "1"])
    output_cif.write_file(str(output_file), style=cif.Style.Simple)


def _motioncorr_output_files(job_dir: Path, file_to_add: Path, options: dict):
    output_file = job_dir / "corrected_micrographs.star"

    if not output_file.exists():
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
        output_cif = cif.read_file(str(output_file))
        data_movies = output_cif.find_block("micrographs")
        movies_loop = data_movies.find_loop("_rlnMicrographName").get_loop()

    movies_loop.add_row(
        [
            str(file_to_add),
            str(file_to_add.with_suffix(".star")),
            "1",
            "0.0",
            "0.0",
            "0.0",
        ]
    )
    output_cif.write_file(str(output_file), style=cif.Style.Simple)


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
    # "relion.ctffind.ctffind4": _from_motioncorr,
    # "relion.ctffind.gctf": _from_motioncorr,
    # "relion.autopick.log": _from_ctf,
    # "relion.autopick.ref3d": _from_ctf,
    # "cryolo.autopick": partial(_from_ctf, in_key="input_file"),
    # "relion.extract": _extract,
    # "relion.select.split": _select,
}


def create_output_files(job_type: str, job_dir: str, file_to_add: Path, options: dict):
    _output_files[job_type](Path(job_dir), file_to_add, options)

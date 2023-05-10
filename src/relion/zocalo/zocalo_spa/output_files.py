from __future__ import annotations

from pathlib import Path
from typing import Callable, Dict

from gemmi import cif

from relion.cryolo_relion_it.cryolo_relion_it import RelionItOptions


def _import_output_files(job_dir: Path, file_to_add: Path, options: RelionItOptions):
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
                "AmplitudeContrast",
            ],
        )
        optics_loop.add_row(
            [
                "opticsGroup1",
                "1",
                str(options.angpix),
                str(options.voltage),
                str(options.ampl_contrast),
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


_output_files: Dict[str, Callable] = {
    "relion.import.movies": _import_output_files,
    # "relion.motioncorr.motioncor2": _from_import,
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


def create_output_files(
    job_type: str, job_dir: Path, file_to_add: Path, options: RelionItOptions
):
    _output_files[job_type](job_dir, file_to_add, options)

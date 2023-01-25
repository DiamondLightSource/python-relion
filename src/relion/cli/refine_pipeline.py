from __future__ import annotations

import argparse

from relion.pipeline.refine import RefinePipelineRunner


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--dir",
        help="Working directory where Relion will run",
        dest="working_directory",
        default=".",
    )
    parser.add_argument(
        "-f",
        "--particles-file",
        help="Star file containing particles to be imported",
        action="append",
        dest="particles_files",
    )
    parser.add_argument(
        "-m",
        "--ref-model",
        help="Reference model",
        dest="ref_model",
    )
    parser.add_argument(
        "--mask",
        help="Path to mask for post processing job",
        dest="mask",
        default="",
    )
    parser.add_argument(
        "--particle-diameter",
        help="Particle diameter in Angstroms",
        dest="particle_diameter",
        default=170,
    )
    parser.add_argument(
        "--extract-size",
        help="Extraction box size",
        dest="extract_size",
        default=256,
    )
    parser.add_argument(
        "--micrographs-star",
        help="Path to micrographs star file",
        dest="micrographs_star",
        default="CtfFind/job003/micrographs_ctf.star",
    )
    parser.add_argument(
        "--autob-highres",
        help="High resolution limit for b-factor calculation",
        dest="autob_highres",
        default=4.75,
    )
    parser.add_argument(
        "--ini-high",
        help="Low pass filter for 3D refinement",
        dest="ini_high",
        default=60,
    )
    parser.add_argument(
        "--sym",
        help="Symmetry for 3D refinement",
        dest="sym",
        default="C1",
    )
    args = parser.parse_args()

    for pf in args.particles_files:
        runner = RefinePipelineRunner(
            args.working_directory,
            pf,
            args.ref_model,
            mask=args.mask,
            autob_highres=args.autob_highres,
            extract_size=args.extract_size,
            symmetry=args.sym,
            ini_high=args.ini_high,
        )
        runner(micrographs_star=args.micrographs_star)

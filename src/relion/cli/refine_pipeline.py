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
        dest="particles_file",
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
    args = parser.parse_args()

    runner = RefinePipelineRunner(
        args.working_directory, args.particles_file, args.ref_model, mask=args.mask
    )
    runner()

from __future__ import annotations

import argparse
from math import ceil
from pathlib import Path

import pandas as pd
import starfile


def combine_star_files(folder_to_process: Path, output_dir: Path):
    """
    Combines all particle star files from a given folder into
    a single file.
    """
    final_star_file = {}
    number_of_star_files = 0
    for split_file in list(folder_to_process.glob("particles_split*.star")):
        number_of_star_files += 1
        star_dictionary = starfile.read(split_file)

        data_optics = star_dictionary["optics"]
        data_particles = star_dictionary["particles"]

        try:
            # check that the files have the same optics tables
            if final_star_file["optics"].ne(data_optics).any().any():
                raise IndexError(
                    "Cannot combine star files with different optics tables."
                )
        except KeyError:
            # if this is the first file, construct a new table
            final_star_file["optics"] = data_optics

        try:
            # combine the particle tables
            final_star_file["particles"] = pd.concat(
                (final_star_file["particles"], data_particles)
            )
        except KeyError:
            # if this is the first file, construct a new table
            final_star_file["particles"] = data_particles

    starfile.write(final_star_file, output_dir / "particles_all.star", overwrite=True)

    print(f"Combined {number_of_star_files} files into particles_all.star")


def split_star_file(
    file_to_process: Path,
    output_dir: Path,
    number_of_splits: int or None = None,
    split_size: int or None = None,
):
    """
    Splits a star file into subfiles.
    The number of subfiles can be given with number_of_splits
    or is determined by split_size, the number of particles for in each file.
    """
    star_dictionary = starfile.read(file_to_process)

    data_optics = star_dictionary["optics"]
    data_particles = star_dictionary["particles"]
    number_of_particles = len(data_particles)

    # determine the number of files and size of the splits
    if number_of_splits:
        if split_size:
            print(
                "Warning: "
                "Both number_of_splits and split_size have been given, "
                "using number_of_splits.",
            )
        split_size = ceil(number_of_particles / number_of_splits)
    elif split_size:
        number_of_splits = ceil(number_of_particles / split_size)
    else:
        raise KeyError("Either number_of_splits or split_size must be given.")

    # create dictionaries of particle data and write these to the new files
    for split in range(number_of_splits):
        particles_to_use = data_particles[split_size * split : split_size * (split + 1)]
        dictionary_to_write = {"optics": data_optics, "particles": particles_to_use}
        starfile.write(
            dictionary_to_write,
            output_dir / f"particles_split{split+1}.star",
            overwrite=True,
        )

    print(
        f"Split {number_of_particles} particles into "
        f"{number_of_splits} files with {split_size} particles in each."
    )


def create_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "folder_to_process",
        type=Path,
        help="Folder to combine particle star files from.",
    )
    parser.add_argument(
        "--output_dir",
        dest="output_dir",
        type=Path,
        help="Folder to save the new star files to.",
    )

    parser.add_argument(
        "--split",
        dest="do_split",
        action="store_true",
        default=False,
        help="Whether to split the particle star files again.",
    )
    parser.add_argument(
        "--n_files",
        dest="n_files",
        type=int,
        default=None,
        help="Number of files to split the particles into.",
    )
    parser.add_argument(
        "--split_size",
        dest="split_size",
        type=int,
        default=None,
        help="Number of particles to write per file.",
    )

    return parser


def main():
    arg_parser = create_parser()
    run_args = vars(arg_parser.parse_args())

    combine_star_files(run_args["folder_to_process"], run_args["output_dir"])

    if run_args["do_split"]:
        split_star_file(
            run_args["output_dir"] / "particles_all.star",
            run_args["output_dir"],
            run_args["n_files"],
            run_args["split_size"],
        )

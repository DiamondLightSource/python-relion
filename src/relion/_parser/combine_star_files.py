from __future__ import annotations

import argparse
from math import ceil
from pathlib import Path
from typing import List

import pandas as pd
import starfile


def combine_star_files(files_to_process: List[Path], output_dir: Path):
    """Combines any number of particle star files into a single file.

    Parameters:
    files_to_process: A list of the particle star files to combine
    output_dir: The directory in which to save the combined "particles_all.star" file
    """
    final_star_file = {}
    number_of_star_files = 0
    for split_file in files_to_process:
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
    """Splits a star file into subfiles.

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
            output_dir / f".particles_split{split+1}_tmp.star",
            overwrite=True,
        )
        (output_dir / f".particles_split{split+1}_tmp.star").rename(
            output_dir / f"particles_split{split+1}.star"
        )

    print(
        f"Split {number_of_particles} particles into "
        f"{number_of_splits} files with {split_size} particles in each."
    )


def create_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "files_to_process",
        type=Path,
        nargs="+",
        help="The star files from which to combine particles.",
    )
    parser.add_argument(
        "--output_dir",
        dest="output_dir",
        type=Path,
        help="Folder in which to save the new star files.",
    )

    parser.add_argument(
        "--split",
        dest="do_split",
        action="store_true",
        default=False,
        help="Whether to split the particles again into new star files.",
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

    combine_star_files(run_args["files_to_process"], run_args["output_dir"])

    if run_args["do_split"]:
        split_star_file(
            run_args["output_dir"] / "particles_all.star",
            run_args["output_dir"],
            run_args["n_files"],
            run_args["split_size"],
        )

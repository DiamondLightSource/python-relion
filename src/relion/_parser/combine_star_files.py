from __future__ import annotations

import argparse
from math import ceil
from pathlib import Path
from typing import List

import starfile


def write_empty_particles_file(file_to_write, optics_dataframe, particles_dataframe):
    """Write a particles star file with no particles, ready for appending to"""
    with open(file_to_write, "w") as optics_file:
        optics_file.write("data_optics\n\nloop_\n")
        for optics_loop_tag in optics_dataframe.keys():
            optics_file.write(f"_{optics_loop_tag}\n")
        optics_file.write(" ".join(optics_dataframe.to_numpy(dtype=str)[0]))
        optics_file.write("\n\n\n")

        optics_file.write("data_particles\n\nloop_\n")
        for particles_loop_tag in particles_dataframe.keys():
            optics_file.write(f"_{particles_loop_tag}\n")


def combine_star_files(files_to_process: List[Path], output_dir: Path):
    """Combines any number of particle star files into a single file.

    Parameters:
    files_to_process: A list of the particle star files to combine
    output_dir: The directory in which to save the combined "particles_all.star" file
    """
    total_particles = 0

    # Never read particles_all.star first as it will be big
    if files_to_process[0].name == "particles_all.star":
        files_to_process.append(files_to_process[0])
        files_to_process = files_to_process[1:]

    # Make a temporary star file to get the table headings from
    reference_optics = None
    with open(files_to_process[0], "r") as full_starfile, open(
        output_dir / ".particles_tmp.star", "w"
    ) as tmp_starfile:
        for line_counter in range(50):
            line = full_starfile.readline()
            if line.startswith("opticsGroup"):
                reference_optics = line.split()
            if not line:
                break
            tmp_starfile.write(line)

    star_dictionary = starfile.read(output_dir / ".particles_tmp.star")
    (output_dir / ".particles_tmp.star").unlink()

    if not reference_optics:
        raise IndexError(f"Cannot find optics group in {files_to_process[0]}")

    write_empty_particles_file(
        output_dir / ".particles_all_tmp.star",
        star_dictionary["optics"],
        star_dictionary["particles"],
    )

    number_of_star_files = 0
    # Add the remaining files using append mode for speed and memory efficiency
    for split_file in files_to_process:
        # Check that the files have the same optics tables
        with open(split_file, "r") as added_starfile:
            while True:
                optics_line = added_starfile.readline()
                if not optics_line:
                    raise IndexError(f"Cannot find optics group in {split_file}")
                if optics_line.startswith("opticsGroup"):
                    new_optics = optics_line.split()
                    break

        if len(new_optics) != len(reference_optics):
            raise IndexError(
                "Cannot combine star files with different length optics tables."
            )
        for optics_label in range(len(reference_optics)):
            ref_value = reference_optics[optics_label]
            new_value = new_optics[optics_label]
            if ref_value[0].isdigit() and new_value[0].isdigit():
                ref_value = float(ref_value)
                new_value = float(new_value)
            if ref_value != new_value:
                print(ref_value, new_value)
                raise IndexError(
                    "Cannot combine star files with different values in optics tables."
                )

        # Add the particles lines to the final star file
        file_particles_count = 0
        with open(split_file, "r") as added_starfile, open(
            output_dir / ".particles_all_tmp.star", "a"
        ) as particles_file:
            while True:
                particle_line = added_starfile.readline()
                if not particle_line:
                    break
                particle_split_line = particle_line.split()
                if len(particle_split_line) > 0 and particle_split_line[0][0].isdigit():
                    file_particles_count += 1
                    total_particles += 1
                    particles_file.write(particle_line)

        print(f"Adding {split_file} with {file_particles_count} particles")
        number_of_star_files += 1

    (output_dir / ".particles_all_tmp.star").rename(output_dir / "particles_all.star")
    print(
        f"Combined {number_of_star_files} files into particles_all.star "
        f"with {total_particles} particles"
    )


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

    # Make a temporary star file to get the table headings from
    with open(file_to_process, "r") as full_starfile, open(
        output_dir / ".particles_tmp.star", "w"
    ) as tmp_starfile:
        for line_counter in range(50):
            line = full_starfile.readline()
            if not line:
                break
            tmp_starfile.write(line)

    star_dictionary = starfile.read(output_dir / ".particles_tmp.star")
    (output_dir / ".particles_tmp.star").unlink()

    # Find the number of lines in the full file
    starfile_starter_lines = line_counter - star_dictionary["particles"].shape[0]
    count = 0
    with open(file_to_process, "r") as full_starfile:
        for count, line in enumerate(full_starfile):
            pass
    number_of_particles = count + 1 - starfile_starter_lines

    # Determine the number of files and size of the splits
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

    with open(file_to_process, "r") as full_starfile:
        # Read in the full file line by line, removing the header lines first
        for start_line in range(starfile_starter_lines):
            full_starfile.readline()

        for split in range(number_of_splits):
            # Give each new file the header information
            write_empty_particles_file(
                output_dir / f".particles_split{split+1}_tmp.star",
                star_dictionary["optics"],
                star_dictionary["particles"],
            )

            # Write particles to the split files by reading in lines from the full file
            with open(
                output_dir / f".particles_split{split+1}_tmp.star", "a"
            ) as split_file:
                for count in range(split_size):
                    particle_line = full_starfile.readline()
                    if not particle_line:
                        break
                    split_file.write(particle_line)

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

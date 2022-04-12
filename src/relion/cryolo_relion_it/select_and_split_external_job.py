#!/usr/bin/env python3
"""
External job to select the micrographs for a chosen class and then split
into two random halves
"""

from __future__ import annotations

import argparse
import os
import os.path
import subprocess

RELION_JOB_FAILURE_FILENAME = "RELION_JOB_EXIT_FAILURE"
RELION_JOB_SUCCESS_FILENAME = "RELION_JOB_EXIT_SUCCESS"


def run_job(project_dir, job_dir, in_dir, starin, starout, args_list):
    parser = argparse.ArgumentParser()
    parser.add_argument("--class_number", dest="class_number")
    parser.add_argument(
        "--j", dest="threads", help="Number of threads to run (ignored)"
    )
    args = parser.parse_args(args_list)
    if not starout.endswith(".star"):
        raise ValueError(
            "Output file for external SelectAndSplit job must end with .star"
        )

    command = [
        "relion_star_handler",
        "--i",
        f"{starin}",
        "--o",
        f"{os.path.join(job_dir, starout)}",
        "--select",
        "rlnClassNumber",
        f"{args.class_number}",
    ]

    subprocess.run(command, check=True)

    command = [
        "relion_star_handler",
        "--i",
        f"{os.path.join(job_dir, starout)}",
        "--o",
        f"{os.path.join(job_dir, starout)}",
        "--split",
        "--random_order",
        "--nr_split",
        "2",
    ]

    subprocess.run(command, check=True)


def main():
    """Change to the job working directory, then call run_job()"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--in_dir", dest="in_dir", help="Input directory name")
    parser.add_argument(
        "--o", "--out_dir", dest="out_dir", help="Output directory name"
    )
    parser.add_argument("--in_mics", dest="starin", help="Input star file name")
    parser.add_argument("--outfile", dest="starout", help="Output star file name")
    # parser.add_argument("--of", help="Output star file name")
    parser.add_argument(
        "--pipeline_control", help="Directory for pipeline control files"
    )
    known_args, other_args = parser.parse_known_args()
    project_dir = os.getcwd()
    os.makedirs(known_args.out_dir, exist_ok=True)
    os.chdir(known_args.out_dir)
    if os.path.isfile(RELION_JOB_FAILURE_FILENAME):
        print(
            " select_and_split_external_job: Removing previous failure indicator file"
        )
        os.remove(RELION_JOB_FAILURE_FILENAME)
    if os.path.isfile(RELION_JOB_SUCCESS_FILENAME):
        print(
            " select_and_split_external_job: Removing previous success indicator file"
        )
        os.remove(RELION_JOB_SUCCESS_FILENAME)
    try:
        os.chdir("../..")
        run_job(
            project_dir,
            known_args.out_dir,
            known_args.in_dir,
            known_args.starin,
            known_args.starout,
            other_args,
        )
    except Exception:
        os.chdir(known_args.out_dir)
        open(RELION_JOB_FAILURE_FILENAME, "w").close()
        raise
    else:
        os.chdir(known_args.out_dir)
        open(RELION_JOB_SUCCESS_FILENAME, "w").close()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
External job for making a mask de novo with a soft edge
"""

import argparse
import os
import os.path
import subprocess


RELION_JOB_FAILURE_FILENAME = "RELION_JOB_EXIT_FAILURE"
RELION_JOB_SUCCESS_FILENAME = "RELION_JOB_EXIT_SUCCESS"


def run_job(project_dir, job_dir, args_list):
    parser = argparse.ArgumentParser()
    parser.add_argument("--box_size", dest="box_size")
    parser.add_argument("--angpix", dest="angpix")
    parser.add_argument("--outer_radius", dest="outer_radius")
    parser.add_argument(
        "--j", dest="threads", help="Number of threads to run (ignored)"
    )
    args = parser.parse_args(args_list)

    os.chdir(job_dir)

    # need to remove the hard coding of outer_radius and width_soft_edge

    command = [
        "relion_mask_create",
        "--denovo",
        "true",
        "--box_size",
        f"{args.box_size}",
        "--angpix",
        f"{args.angpix}",
        "--outer_radius",
        f"{args.outer_radius}",
        "--width_soft_edge",
        "5",
    ]

    subprocess.run(command, check=True)


def main():
    """Change to the job working directory, then call run_job()"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--out_dir", dest="out_dir", help="Output directory name")
    parser.add_argument(
        "--pipeline_control", help="Directory for pipeline control files"
    )
    known_args, other_args = parser.parse_known_args()
    project_dir = os.getcwd()
    os.makedirs(known_args.out_dir, exist_ok=True)
    os.chdir(known_args.out_dir)
    if os.path.isfile(RELION_JOB_FAILURE_FILENAME):
        print(" mask_soft_edge_external_job: Removing previous failure indicator file")
        os.remove(RELION_JOB_FAILURE_FILENAME)
    if os.path.isfile(RELION_JOB_SUCCESS_FILENAME):
        print(" mask_soft_edge_external_job: Removing previous success indicator file")
        os.remove(RELION_JOB_SUCCESS_FILENAME)
    try:
        os.chdir("../..")
        run_job(project_dir, known_args.out_dir, other_args)
    except Exception:
        if os.getcwd() == project_dir:
            os.chdir(known_args.out_dir)
        open(RELION_JOB_FAILURE_FILENAME, "w").close()
        raise
    else:
        if os.getcwd() == project_dir:
            os.chdir(known_args.out_dir)
        open(RELION_JOB_SUCCESS_FILENAME, "w").close()


if __name__ == "__main__":
    main()

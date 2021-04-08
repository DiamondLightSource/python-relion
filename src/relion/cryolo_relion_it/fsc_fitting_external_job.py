#!/usr/bin/env python3
"""
External job for fitting FSC curves and finding the value at which they cross 0.5
"""

import argparse
import json
import os
import os.path

import gemmi


RELION_JOB_FAILURE_FILENAME = "RELION_JOB_EXIT_FAILURE"
RELION_JOB_SUCCESS_FILENAME = "RELION_JOB_EXIT_SUCCESS"


def run_job(project_dir, out_dir, fscs_files, args_list):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--j", dest="threads", help="Number of threads to run (ignored)"
    )

    resolutions = []

    for i, starin in enumerate(fscs_files):
        if not starin.endswith(".star"):
            raise ValueError("Input files containing FSC curves must end with .star")
        fsc_in = gemmi.cif.read_file(os.path.join(project_dir, starin))
        data_as_dict = json.loads(fsc_in.as_json())["fsc"]
        invres = [1 / x for x in data_as_dict["_rlnangstromresolution"]]
        fsc = data_as_dict["_rlnfouriershellcorrelationcorrected"]
        res = lin_interp(invres, fsc)
        resolutions.append(res)

    class_index = resolutions.index(min(resolutions))

    return class_index


def lin_interp(invres, fsc):
    cpoints = crossing_points(invres, fsc)
    last_crossing_point = cpoints[-1]
    slope = (last_crossing_point[1][1] - last_crossing_point[0][1]) / (
        last_crossing_point[1][0] - last_crossing_point[0][0]
    )
    constant = last_crossing_point[0][1] - slope * last_crossing_point[0][0]
    return (0.5 - constant) / slope


def crossing_points(invres, fsc):
    points = []
    for i in range(1, len(invres)):
        if fsc[i] <= 0.5 and fsc[i - 1] >= 0.5:
            points.append(((invres[i - 1], fsc[i - 1]), (invres[i], fsc[i])))
    return points


def main():
    """Change to the job working directory, then call run_job()"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out_dir", dest="out_dir", help="Directory for the FSC fitting External job"
    )
    parser.add_argument(
        "--i",
        nargs="+",
        dest="fscs",
        help="Input star file names containing FSC curves for comparison",
    )
    parser.add_argument(
        "--pipeline_control", help="Directory for pipeline control files"
    )
    known_args, other_args = parser.parse_known_args()
    project_dir = os.getcwd()
    os.makedirs(known_args.out_dir, exist_ok=True)
    os.chdir(known_args.out_dir)
    if os.path.isfile(RELION_JOB_FAILURE_FILENAME):
        print(" fsc_fitting_external_job: Removing previous failure indicator file")
        os.remove(RELION_JOB_FAILURE_FILENAME)
    if os.path.isfile(RELION_JOB_SUCCESS_FILENAME):
        print(" fsc_fitting_external_job: Removing previous success indicator file")
        os.remove(RELION_JOB_SUCCESS_FILENAME)
    try:
        os.chdir("../..")
        class_index = run_job(
            project_dir, known_args.out_dir, known_args.fscs, other_args
        )
        with open(
            os.path.join(project_dir, known_args.out_dir, "BestClass.txt"), "w"
        ) as f:
            f.write(str(class_index))
    except Exception:
        os.chdir(known_args.out_dir)
        open(RELION_JOB_FAILURE_FILENAME, "w").close()
        raise
    else:
        os.chdir(known_args.out_dir)
        open(RELION_JOB_SUCCESS_FILENAME, "w").close()


if __name__ == "__main__":
    main()

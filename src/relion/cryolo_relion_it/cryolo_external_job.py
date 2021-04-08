#!/usr/bin/env python
"""
External job for calling cryolo within Relion 3.1
in_mics are the micrographs to be picked on
in_model is the model to be used, empty will use general model!

Run in main Relion project directory
external_cryolo.py --o $PATH_WHERE_TO_STORE --in_mics $PATH_TO_MCORR/CTF_STARFILE --box_size $BOX_SIZE --threshold 0.3 (optional: --in_model $PATH_TO_MODEL)
eg:
cryolo_external_job.py --o "External/crYOLO_AutoPick" --in_mics "CtfFind/job004/micrographs_ctf.star" --box_size 300 --threshold 0.3
"""

import argparse
import datetime
import json
import os
import os.path
import platform
import shutil
import subprocess
import sys
import pathlib

import gemmi


RELION_JOB_FAILURE_FILENAME = "RELION_JOB_EXIT_FAILURE"
RELION_JOB_SUCCESS_FILENAME = "RELION_JOB_EXIT_SUCCESS"


def run_job(project_dir, job_dir, args_list):
    parser = argparse.ArgumentParser()
    parser.add_argument("--in_mics", help="Input micrographs STAR file")
    parser.add_argument(
        "--j", dest="threads", help="Number of threads to run (ignored)"
    )
    parser.add_argument("--box_size", help="Size of box (~ particle size)")
    parser.add_argument("--threshold", help="Threshold for picking (default = 0.3)")
    parser.add_argument("--in_model", help="model from previous job")
    parser.add_argument("--gmodel", help="cryolo general model")
    parser.add_argument("--config", help="cryolo config")
    parser.add_argument("--gpu", help='GPUs to use (e.g. "0 1 2 3")')
    args = parser.parse_args(args_list)
    thresh = args.threshold
    box_size = args.box_size
    gen_model = args.gmodel
    conf_file = args.config
    gpus = args.gpu

    # Use general model by default if in_model not given or doesn't exist
    model = gen_model
    if args.in_model is not None:
        model_path = os.path.join(project_dir, args.in_model)
        if os.path.isfile(model_path):
            model = model_path
        else:
            print(f" cryolo_external_job: Cannot find fine tuned model {model_path}")

    # Making a cryolo config file with the correct box size
    with open(conf_file, "r") as json_file:
        data = json.load(json_file)
        data["model"]["anchors"] = [int(box_size), int(box_size)]
    with open("config.json", "w") as outfile:
        json.dump(data, outfile)

    # Reading the micrographs star file from relion
    in_doc = gemmi.cif.read_file(os.path.join(project_dir, args.in_mics))
    data_as_dict = json.loads(in_doc.as_json())["micrographs"]

    try:
        os.mkdir("cryolo_input")
    except FileExistsError:
        # Not crucial so if fails due to any reason just carry on
        try:
            with open(
                "done_mics.txt", "a+"
            ) as f:  # Done mics is to ensure that cryolo doesn't pick from already done mics
                for micrograph in os.listdir("cryolo_input"):
                    f.write(micrograph + "\n")
        except Exception:
            pass
        shutil.rmtree("cryolo_input")
        os.mkdir("cryolo_input")

    # Arranging files for cryolo to predict particles from
    # Not crucial so if fails due to any reason just carry on
    try:
        with open("done_mics.txt", "r") as f:
            done_mics = f.read().splitlines()
    except Exception:
        done_mics = []

    # Count new micrographs because cryolo fails if its input directory is empty
    mic_count = 0
    for micrograph in data_as_dict["_rlnmicrographname"]:
        if os.path.split(micrograph)[-1] not in done_mics:
            os.link(
                os.path.join(project_dir, micrograph),
                os.path.join(
                    project_dir, job_dir, "cryolo_input", os.path.split(micrograph)[-1]
                ),
            )
            mic_count += 1

    if mic_count > 0:
        print(" cryolo_external_job: Running from model {}".format(model))
        cryolo_command = [
            "cryolo_predict.py",
            "--conf",
            "config.json",
            "-i",
            f"{pathlib.PurePosixPath(project_dir) / job_dir / 'cryolo_input'}",
            "-o",
            f"{pathlib.PurePosixPath(project_dir) / job_dir / 'gen_pick'}",
            "--weights",
            model,
            "--gpu",
            gpus,
            "--threshold",
            thresh,
        ]
        subprocess.run(cryolo_command, check=True)
    else:
        print(" cryolo_external_job: No new micrographs, not running crYOLO")

    os.makedirs("picked_stars", exist_ok=True)

    # Arranging files for Relion to use
    for picked in os.listdir(os.path.join(project_dir, job_dir, "gen_pick", "STAR")):
        new_name = os.path.splitext(picked)[0] + "_autopick" + ".star"
        try:
            os.link(
                os.path.join(project_dir, job_dir, "gen_pick", "STAR", picked),
                os.path.join(project_dir, job_dir, "picked_stars", new_name),
            )
        except FileExistsError:
            pass

    # Writing a star file for Relion
    with open("coords_suffix_autopick.star", "w") as part_file:
        part_file.write(args.in_mics)

    # Required star file
    out_doc = gemmi.cif.Document()
    output_nodes_block = out_doc.add_new_block("output_nodes")
    loop = output_nodes_block.init_loop(
        "", ["_rlnPipeLineNodeName", "_rlnPipeLineNodeType"]
    )
    loop.add_row([os.path.join(job_dir, "coords_suffix_autopick.star"), "2"])
    out_doc.write_file("RELION_OUTPUT_NODES.star")
    ctf_star = os.path.join(project_dir, args.in_mics)
    correct_paths(ctf_star)


def correct_paths(ctf_star):
    in_doc = gemmi.cif.read_file(ctf_star)
    data_as_dict = json.loads(in_doc.as_json())["micrographs"]

    for i in range(len(data_as_dict["_rlnmicrographname"])):
        name = data_as_dict["_rlnmicrographname"][i]
        dirs, mic_file = os.path.split(name)
        full_dir = ""
        for d in dirs.split("/")[2:]:
            full_dir = os.path.join(full_dir, d)
        os.makedirs(full_dir, exist_ok=True)
        picked_star = os.path.splitext(mic_file)[0] + "_autopick.star"
        try:
            shutil.move(
                os.path.join("picked_stars", picked_star),
                os.path.join(full_dir, picked_star),
            )
        except FileNotFoundError:
            print(f" cryolo_external_job: cryolo found no particles in {picked_star}")


def main():
    """Change to the job working directory, then call run_job()"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--o", dest="out_dir", help="Output directory name")
    parser.add_argument(
        "--pipeline_control", help="Directory for pipeline control files"
    )
    known_args, other_args = parser.parse_known_args()
    project_dir = os.getcwd()
    job_dir = known_args.out_dir

    # pipeline_control_dir is probably the same as job_dir, and we should use job_dir
    # if pipeline_control_dir is not given. Make the path absolute (by joining it onto
    # project_dir) so success and failure indicator files are always written in the
    # right place even if the working directory is changed.
    pipeline_control_dir = (
        os.path.join(project_dir, known_args.pipeline_control)
        if known_args.pipeline_control is not None
        else os.path.join(project_dir, job_dir)
    )
    # Ensure job_dir and pipeline_control_dir (probably the same) both exist
    os.makedirs(job_dir, exist_ok=True)
    os.makedirs(pipeline_control_dir, exist_ok=True)

    # Prepare indicator filenames for use after the job has finished
    failure_filename = os.path.join(pipeline_control_dir, RELION_JOB_FAILURE_FILENAME)
    success_filename = os.path.join(pipeline_control_dir, RELION_JOB_SUCCESS_FILENAME)

    # Print current time and host to stdout and stderr
    # This helps with debugging when the job is run repeatedly
    time = datetime.datetime.now()
    host = platform.node()
    message = f"{time}: cryolo_external_job.py running on {host}"
    print(message)
    print(message, file=sys.stderr)

    # Change to the job directory to actually run the job
    os.chdir(job_dir)
    try:
        run_job(project_dir, job_dir, other_args)
    except Exception:
        open(failure_filename, "w").close()
        raise
    else:
        open(success_filename, "w").close()


if __name__ == "__main__":
    main()

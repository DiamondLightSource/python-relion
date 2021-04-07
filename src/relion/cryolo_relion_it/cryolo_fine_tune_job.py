#!/usr/bin/env python
"""
External job for calling cryolo fine tune within Relion 3.1
in_parts is from a subset selection job.
cryolo_fine_tune_job.py --o 'External/crYOLO_FineTune' --in_parts 'Select/job005/particles.star' --box_size 300
"""

import argparse
import json
import os
import os.path
import shutil
import subprocess
import time

import gemmi


RELION_JOB_FAILURE_FILENAME = "RELION_JOB_EXIT_FAILURE"
RELION_JOB_SUCCESS_FILENAME = "RELION_JOB_EXIT_SUCCESS"

CRYOLO_FINETUNE_JOB_DIR = "External/crYOLO_FineTune"


def run_job(project_dir, job_dir, args_list):
    parser = argparse.ArgumentParser()
    parser.add_argument("--in_parts", help="Input micrographs STAR file")
    parser.add_argument(
        "--j", dest="threads", help="Number of threads to run (ignored)"
    )
    parser.add_argument("--box_size", help="Size of box (~ particle size)")
    parser.add_argument("--gmodel", help="cryolo general model")
    parser.add_argument("--config", help="cryolo config")
    args = parser.parse_args(args_list)
    box_size = args.box_size
    weights = args.gmodel
    conf_file = args.config

    # Making a cryolo config file with the correct box size and model location
    with open(conf_file, "r") as json_file:
        data = json.load(json_file)
        data["model"]["anchors"] = [int(box_size), int(box_size)]
        data["train"]["pretrained_weights"] = weights
    with open("config.json", "w") as outfile:
        json.dump(data, outfile)

    # Reading particle star file from relion
    count = 0
    particle_file = os.path.join(project_dir, args.in_parts)
    while not os.path.exists(particle_file):
        count += 1
        if count > 60:
            print(
                f" cryolo_fine_tune_job: giving up after waiting for over {count * 10} seconds for particle file {particle_file} to appear"
            )
            raise AssertionError("Timeout waiting for input file")
        if count % 6 == 0:
            print(
                f" cryolo_fine_tune_job: still waiting for particle file {particle_file} to appear after {count * 10} seconds"
            )
        time.sleep(10)
    in_doc = gemmi.cif.read_file(particle_file)
    data_as_dict = json.loads(in_doc.as_json())["micrographs"]

    try:
        os.mkdir("train_annotation")
    except FileExistsError:
        shutil.rmtree("train_annotation")
        os.mkdir("train_annotation")
    try:
        os.mkdir("train_image")
    except FileExistsError:
        shutil.rmtree("train_image")
        os.mkdir("train_image")

    # Arranging files for cryolo to train from
    for micro in range(len(data_as_dict["_rlnmicrographname"])):
        try:
            os.link(
                os.path.join(project_dir, data_as_dict["_rlnmicrographname"][micro]),
                os.path.join(
                    project_dir,
                    job_dir,
                    "train_image",
                    os.path.split(data_as_dict["_rlnmicrographname"][micro])[-1],
                ),
            )
        except FileExistsError:
            pass

        box_name = (
            os.path.splitext(
                os.path.split(data_as_dict["_rlnmicrographname"][micro])[-1]
            )[0]
            + ".box"
        )

        individual_files = open(os.path.join("train_annotation", box_name), "a+")
        individual_files.write(
            f"{data_as_dict['_rlncoordinatex'][micro] - int(box_size)/2}\t"
        )
        individual_files.write(
            f"{data_as_dict['_rlncoordinatey'][micro] - int(box_size)/2}\t"
        )
        individual_files.write(f"{box_size}\t")
        individual_files.write(f"{box_size}\n")
        individual_files.close()

    # Running cryolo
    subprocess.run(
        [
            "cryolo_train.py",
            "--conf",
            "config.json",
            "--warmup",
            "0",
            "--gpu",
            "0",
            "--fine_tune",
        ]
    )

    # Writing a star file (This one is meaningless for now)
    with open("_manualpick.star", "w") as part_doc:
        part_doc.write(os.path.join(project_dir, args.in_parts))

    # Required star file
    out_doc = gemmi.cif.Document()
    output_nodes_block = out_doc.add_new_block("output_nodes")
    loop = output_nodes_block.init_loop(
        "", ["_rlnPipeLineNodeName", "_rlnPipeLineNodeType"]
    )
    loop.add_row([os.path.join(job_dir, "_manualpick.star"), "2"])
    out_doc.write_file("RELION_OUTPUT_NODES.star")
    print(" cryolo_fine_tune_job: crYOLO Finished Fine-Tuning")


def main():
    """Change to the job working directory, then call run_job()"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--o", dest="out_dir", help="Output directory name")
    known_args, other_args = parser.parse_known_args()
    project_dir = os.getcwd()
    os.makedirs(known_args.out_dir, exist_ok=True)
    os.chdir(known_args.out_dir)
    if os.path.isfile(RELION_JOB_FAILURE_FILENAME):
        print(" cryolo_fine_tune_job: Removing previous failure indicator file")
        os.remove(RELION_JOB_FAILURE_FILENAME)
    if os.path.isfile(RELION_JOB_SUCCESS_FILENAME):
        print(" cryolo_fine_tune_job: Removing previous success indicator file")
        os.remove(RELION_JOB_SUCCESS_FILENAME)
    try:
        run_job(project_dir, known_args.out_dir, other_args)
    except Exception:
        open(RELION_JOB_FAILURE_FILENAME, "w").close()
        raise
    else:
        open(RELION_JOB_SUCCESS_FILENAME, "w").close()


def run_cryolo_job(job_dir, command_list, pipeline_opts, wait_for_completion=True):
    """Run a cryolo job (submitting to the queue if requested) and optionally wait for completion"""
    success_file = os.path.join(job_dir, RELION_JOB_SUCCESS_FILENAME)
    failure_file = os.path.join(job_dir, RELION_JOB_FAILURE_FILENAME)
    if os.path.isfile(failure_file):
        print(f" cryolo_pipeline: Removing previous job failure file {failure_file}")
        os.remove(failure_file)
    if os.path.isfile(success_file):
        print(f" cryolo_pipeline: Removing previous job success file {success_file}")
        os.remove(success_file)

    if pipeline_opts.cryolo_submit_to_queue:
        # TODO: remove -o and -e arguments after switch to Relion 3.1. This syntax is specific to
        # qsub but necessary for now to ensure stdout and stderr files go in the job directory
        # rather than the project directory. In Relion 3.1 the normal Relion qsub template can be
        # used which will put the files in the right place automatically.
        submit_command = [
            pipeline_opts.queue_submit_command,
            "-o",
            os.path.join(job_dir, "cryolo_job.out"),  # Temporary!
            "-e",
            os.path.join(job_dir, "cryolo_job.err"),  # Temporary!
            pipeline_opts.cryolo_queue_submission_template,
        ]
        submit_command.extend(command_list)
        print(
            " cryolo_pipeline: running cryolo command: {}".format(
                " ".join(submit_command)
            )
        )
        subprocess.Popen(submit_command)
    else:
        print(
            " cryolo_pipeline: running cryolo command: {}".format(
                " ".join(command_list)
            )
        )
        subprocess.Popen(command_list)

    if wait_for_completion:
        count = 0
        while not (os.path.isfile(failure_file) or os.path.isfile(success_file)):
            count += 1
            if count % 6 == 0:
                print(
                    f" cryolo_pipeline: Still waiting for cryolo job to finish after {count * 10} seconds"
                )
            time.sleep(10)


if __name__ == "__main__":
    main()

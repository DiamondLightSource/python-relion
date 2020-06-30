#!/usr/bin/env python
"""
This is the cryolo preprocessing pipeline run from relion_it script. This script first executes the relion_it pipeline up to picking - cryolo then runs through cryolo_external_job.py and its output is used by relion extraction. All executions of this script after the first run in the background and in parallel to the relion_it script.

As with the relion_it script, if RUNNING_RELION_IT is deleted then this script will stop.
"""
import os
import time
import argparse
import ast
import sys
import subprocess
import shutil

import cryolo_relion_it
import cryolo_external_job


CRYOLO_PIPELINE_OPTIONS_FILE = "cryolo_pipeline_options.py"
CRYOLO_PICK_JOB_DIR = "External/crYOLO_AutoPick"
CRYOLO_FINETUNE_JOB_DIR = "External/crYOLO_FineTune"


def main():
    # When this script is run in the background a few arguments and options need to be parsed
    opts = cryolo_relion_it.RelionItOptions()
    opts.update_from_file(CRYOLO_PIPELINE_OPTIONS_FILE)

    parser = argparse.ArgumentParser()
    parser.add_argument("--runjobs")
    parser.add_argument("--motioncorr_job")
    parser.add_argument("--ctffind_job")
    parser.add_argument("--ipass")
    parser.add_argument("--manpick_job")
    args = parser.parse_args()
    runjobs = ast.literal_eval(args.runjobs)
    motioncorr_job = args.motioncorr_job
    ctffind_job = args.ctffind_job
    ipass = int(args.ipass)
    manpick_job = args.manpick_job

    queue_options = [
        "Submit to queue? == Yes",
        "Queue name:  == {}".format(opts.queue_name),
        "Queue submit command: == {}".format(opts.queue_submit_command),
        "Standard submission script: == {}".format(opts.queue_submission_template),
        "Minimum dedicated cores per node: == {}".format(opts.queue_minimum_dedicated),
    ]

    RunJobsCry(
        opts.preprocess_repeat_times,
        runjobs,
        motioncorr_job,
        ctffind_job,
        opts,
        ipass,
        queue_options,
        manpick_job,
    )


def RunJobsCry(
    num_repeats,
    runjobs,
    motioncorr_job,
    ctffind_job,
    opts,
    ipass,
    queue_options,
    manpick_job,
):
    """
    Very similar to relion_it preprocessing pipeline with the autopicker. Extract and select jobs are identical.
    """
    # Constants
    PIPELINE_STAR = "default_pipeline.star"
    RUNNING_FILE = "RUNNING_RELION_IT"
    SECONDPASS_REF3D_FILE = "RELION_IT_2NDPASS_3DREF"
    SETUP_CHECK_FILE = "RELION_IT_SUBMITTED_JOBS"
    PREPROCESS_SCHEDULE_PASS1 = "PREPROCESS"
    PREPROCESS_SCHEDULE_PASS2 = "PREPROCESS_PASS2"

    # Ensure return variables are initialised
    split_job = None
    manpick_job = None

    for i in range(0, num_repeats):
        if not os.path.isfile(RUNNING_FILE):
            print(
                f" cryolo_pipeline: {RUNNING_FILE} file no longer exists, exiting now ..."
            )
            exit(0)
        preprocess_schedule_name = "BEFORE_CRYOLO"
        # Running jobs up until picking
        cryolo_relion_it.RunJobs(runjobs, 1, 1, preprocess_schedule_name)
        cryolo_relion_it.WaitForJob(motioncorr_job, 15)
        cryolo_relion_it.WaitForJob(ctffind_job, 15)
        cryolo_command = [
            cryolo_external_job.__file__,
            "--in_mics",
            os.path.join(ctffind_job, "micrographs_ctf.star"),
            "--o",
            CRYOLO_PICK_JOB_DIR,
            "--box_size",
            str(int(opts.extract_boxsize / opts.motioncor_binning)),
            "--threshold",
            str(opts.cryolo_threshold),
            "--gmodel",
            str(opts.cryolo_gmodel),
            "--config",
            str(opts.cryolo_config),
            "--gpu",
            f'"{opts.cryolo_pick_gpus}"',
        ]

        if os.path.isfile(
            os.path.join(
                CRYOLO_FINETUNE_JOB_DIR, cryolo_external_job.RELION_JOB_SUCCESS_FILENAME
            )
        ):
            cryolo_command.extend(
                ["--in_model", os.path.join(CRYOLO_FINETUNE_JOB_DIR, "model.h5")]
            )

        run_cryolo_job(
            CRYOLO_PICK_JOB_DIR, cryolo_command, opts, wait_for_completion=True
        )

        if not os.path.isfile(RUNNING_FILE):
            print(
                f" cryolo_pipeline: {RUNNING_FILE} file no longer exists, exiting now ..."
            )
            exit(0)

        #### Set up manual pick job
        if num_repeats == 1:
            # In order to visualise cry picked particles
            manpick_options = [
                "Input micrographs: == {}micrographs_ctf.star".format(ctffind_job),
                "Particle diameter (A): == {}".format(opts.autopick_LoG_diam_min),
            ]
            manualpick_job_name = "crYOLO_AutoPick"
            manualpick_alias = "crYOLO_AutoPick"
            manpick_job, already_had_it = cryolo_relion_it.addJob(
                "ManualPick",
                manualpick_job_name,
                SETUP_CHECK_FILE,
                manpick_options,
                alias=manualpick_alias,
            )
            cryolo_relion_it.RunJobs([manpick_job], 1, 1, "ManualPick")

        # wait for Manpick to make movies directory tree
        wait_count = 0
        # movies_dir to make sure if they named 'Movies' file differently it wont fail
        movies_dir = opts.import_images.split("/")[0]
        while not os.path.exists(manpick_job):
            if wait_count > 15:
                # but dont wait too long as not too important
                break
            time.sleep(2)
            wait_count += 1

        if wait_count <= 15:
            try:
                shutil.rmtree(os.path.join(manpick_job, movies_dir))
                # Multiple reasons this could fail... Not crucial
            except:
                pass
            shutil.copytree(
                os.path.join(CRYOLO_PICK_JOB_DIR, movies_dir),
                os.path.join(manpick_job, movies_dir),
            )

        #### Set up the Extract job
        bin_corrected_box_exact = int(opts.extract_boxsize / opts.motioncor_binning)
        bin_corrected_box_even = bin_corrected_box_exact + bin_corrected_box_exact % 2
        extract_options = [
            "Input coordinates:  == {}_manualpick.star".format(
                CRYOLO_PICK_JOB_DIR + "/"
            ),
            "micrograph STAR file:  == {}micrographs_ctf.star".format(ctffind_job),
            "Diameter background circle (pix):  == {}".format(opts.extract_bg_diameter),
            "Particle box size (pix): == {}".format(bin_corrected_box_even),
            "Number of MPI procs: == {}".format(opts.extract_mpi),
        ]

        if ipass == 0:
            if opts.extract_downscale:
                extract_options.append("Rescale particles? == Yes")
                extract_options.append(
                    "Re-scaled size (pixels):  == {}".format(opts.extract_small_boxsize)
                )
        else:
            if opts.extract2_downscale:
                extract_options.append("Rescale particles? == Yes")
                extract_options.append(
                    "Re-scaled size (pixels):  == {}".format(
                        opts.extract2_small_boxsize
                    )
                )

        if opts.extract_submit_to_queue:
            extract_options.extend(queue_options)

        if ipass == 0:
            extract_job_name = "extract_job"
            extract_alias = "pass 1"
        else:
            extract_job_name = "extract2_job"
            extract_alias = "pass 2"

        extract_job, already_had_it = cryolo_relion_it.addJob(
            "Extract",
            extract_job_name,
            SETUP_CHECK_FILE,
            extract_options,
            alias=extract_alias,
        )
        secondjobs = [extract_job]

        if (ipass == 0 and (opts.do_class2d or opts.do_class3d)) or (
            ipass == 1 and (opts.do_class2d_pass2 or opts.do_class3d_pass2)
        ):
            #### Set up the Select job to split the particle STAR file into batches
            split_options = [
                "OR select from particles.star: == {}particles.star".format(
                    extract_job
                ),
                "OR: split into subsets? == Yes",
                "OR: number of subsets:  == -1",
            ]

            if ipass == 0:
                split_job_name = "split_job"
                split_options.append("Subset size:  == {}".format(opts.batch_size))
                split_alias = "into {}".format(opts.batch_size)
            else:
                split_job_name = "split2_job"
                split_options.append(
                    "Subset size:  == {}".format(opts.batch_size_pass2)
                )
                split_alias = "into {}".format(opts.batch_size_pass2)

            split_job, already_had_it = cryolo_relion_it.addJob(
                "Select",
                split_job_name,
                SETUP_CHECK_FILE,
                split_options,
                alias=split_alias,
            )

            # Now start running stuff
            secondjobs.append(split_job)
            # Now execute the entire preprocessing pipeliner
            if ipass == 0:
                preprocess_schedule_name = PREPROCESS_SCHEDULE_PASS1
            else:
                preprocess_schedule_name = PREPROCESS_SCHEDULE_PASS2
            cryolo_relion_it.RunJobs(secondjobs, 1, 1, preprocess_schedule_name)
    if num_repeats == 1:
        return split_job, manpick_job


def run_cryolo_job(job_dir, command_list, pipeline_opts, wait_for_completion=True):
    """Run a cryolo job (submitting to the queue if requested) and optionally wait for completion"""
    success_file = os.path.join(
        job_dir, cryolo_external_job.RELION_JOB_SUCCESS_FILENAME
    )
    failure_file = os.path.join(
        job_dir, cryolo_external_job.RELION_JOB_FAILURE_FILENAME
    )
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
                    " cryolo_pipeline: Still waiting for cryolo job to finish after {count * 10} seconds"
                )
            time.sleep(10)


if __name__ == "__main__":
    main()

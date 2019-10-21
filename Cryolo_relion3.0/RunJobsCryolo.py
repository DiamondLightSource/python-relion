#!/dls_sw/apps/python/anaconda/4.6.14/64/envs/cryolo/bin/python
"""
This is the cryolo preprocessing pipeline run from relion_it script. This script first executes the relion_it pipeline up to picking - cryolo then runs through external_cryolo_3.py and its output is used by relion extraction. All executions of this script after the first run in the background and in parallel to the relion_it script.

As with the relion_it script, if RUNNING_RELION_IT is deleted then this script will stop.
"""
import os
import time
import argparse
import ast
import sys
import runpy
import subprocess
import shutil

import relion_it_editted

cryolo_relion_directory = "/dls_sw/apps/EM/relion_cryolo/CryoloRelion-master/"
# cryolo_relion_directory = '/home/yig62234/Documents/pythonEM/Cryolo_relion3.0'


def main():
    # When this script is run in the background a few arguments and options need to be parsed
    OPTIONS_FILE = "relion_it_options.py"
    opts = relion_it_editted.RelionItOptions()

    parser = argparse.ArgumentParser()
    parser.add_argument("--num_repeats")
    parser.add_argument("--runjobs")
    parser.add_argument("--motioncorr_job")
    parser.add_argument("--ctffind_job")
    parser.add_argument("--ipass")
    parser.add_argument("--user_opt_files")
    parser.add_argument("--gui")
    parser.add_argument("--manpick_job")
    args = parser.parse_args()
    num_repeats = int(args.num_repeats)
    runjobs = ast.literal_eval(args.runjobs)
    motioncorr_job = args.motioncorr_job
    ctffind_job = args.ctffind_job
    ipass = int(args.ipass)
    gui = int(args.gui)
    manpick_job = args.manpick_job

    option_files = ast.literal_eval(args.user_opt_files)
    if gui == 1:
        option_files.append(OPTIONS_FILE)

    for user_opt_file in option_files:
        user_opts = runpy.run_path(user_opt_file)
        opts.update_from(user_opts)

    queue_options = [
        "Submit to queue? == Yes",
        "Queue name:  == {}".format(opts.queue_name),
        "Queue submit command: == {}".format(opts.queue_submit_command),
        "Standard submission script: == {}".format(opts.queue_submission_template),
        "Minimum dedicated cores per node: == {}".format(opts.queue_minimum_dedicated),
    ]

    RunJobsCry(
        num_repeats,
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

    # print ' RELION_IT: submitted',preprocess_schedule_name,'pipeliner with', opts.preprocess_repeat_times,'repeats of the preprocessing jobs'
    # print ' RELION_IT: this pipeliner will run in the background of your shell. You can stop it by deleting the file RUNNING_PIPELINER_'+preprocess_schedule_name

    for i in range(0, num_repeats):
        if not os.path.exists("RUNNING_RELION_IT"):
            print("Exiting cryolo pipeline")
            exit
        preprocess_schedule_name = "BEFORE_CRYOLO"
        # Running jobs up until picking
        relion_it_editted.RunJobs(runjobs, 1, 1, preprocess_schedule_name)
        relion_it_editted.WaitForJob(motioncorr_job, 15)
        relion_it_editted.WaitForJob(ctffind_job, 15)
        cryolo_options = [
            "--in_mics {}".format(os.path.join(ctffind_job + "micrographs_ctf.star")),
            "--o {}".format("External"),
            "--box_size {}".format(opts.extract_boxsize),
            "--threshold {}".format(opts.cryolo_threshold),
        ]
        option_string = ""
        for cry_option in cryolo_options:
            option_string += cry_option
            option_string += " "
        if os.path.exists("ExternalFine/DONE"):
            option_string += "--in_model 'ExternalFine/model.h5'"
        command = (
            os.path.join(cryolo_relion_directory, "external_cryolo_3.py")
            + " "
            + option_string
        )
        print(" RELION_IT: RUNNING {}".format(command))
        os.system(command)

        if not os.path.exists("RUNNING_RELION_IT"):
            print("Exiting cryolo pipeline")
            exit

        #### Set up manual pick job
        if num_repeats == 1:
            # In order to visualise cry picked particles
            manpick_options = [
                "Input micrographs: == {}micrographs_ctf.star".format(ctffind_job)
            ]
            manualpick_job_name = "cryolo_picks"
            manualpick_alias = "cryolo_picks"
            manpick_job, already_had_it = relion_it_editted.addJob(
                "ManualPick",
                manualpick_job_name,
                SETUP_CHECK_FILE,
                manpick_options,
                alias=manualpick_alias,
            )
            relion_it_editted.RunJobs([manpick_job], 1, 1, "ManualPick")

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
            except:
                pass
            shutil.copytree(
                os.path.join("External", movies_dir),
                os.path.join(manpick_job, movies_dir),
            )

        #### Set up the Extract job
        extract_options = [
            "Input coordinates:  == {}_manualpick.star".format("External/"),
            "micrograph STAR file:  == {}micrographs_ctf.star".format(ctffind_job),
            "Diameter background circle (pix):  == {}".format(opts.extract_bg_diameter),
            "Particle box size (pix): == {}".format(
                opts.extract_boxsize / opts.motioncor_binning
            ),
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

        extract_job, already_had_it = relion_it_editted.addJob(
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

            split_job, already_had_it = relion_it_editted.addJob(
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
            relion_it_editted.RunJobs(secondjobs, 1, 1, preprocess_schedule_name)
    if num_repeats == 1:
        return split_job, manpick_job


if __name__ == "__main__":
    main()

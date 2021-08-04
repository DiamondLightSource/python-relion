import datetime
import enum
import functools
import logging
import os
import pathlib
import threading
import time
from pprint import pprint

import zocalo.util.symlink
import zocalo.wrapper

import relion
from relion.cryolo_relion_it import cryolo_relion_it, dls_options, icebreaker_histogram
from relion.cryolo_relion_it.cryolo_relion_it import RelionItOptions
from relion.dbmodel.modeltables import (
    CryoemInitialModelTable,
    CTFTable,
    MotionCorrectionTable,
    ParticleClassificationGroupTable,
    ParticleClassificationTable,
    ParticlePickerTable,
)

logger = logging.getLogger("relion.zocalo.wrapper")

RelionStatus = enum.Enum("RelionStatus", "RUNNING SUCCESS FAILURE")

# to test:
# - create an empty directory
# - run following command to generate a recipe-wrapper:
#   dlstbx.find_in_ispyb -p 6844019 -f /dls_sw/apps/zocalo/live/recipes/ispyb-relion.json --recipe-pointer=2 --out=rw && replace "/dls/m12/data/2021/cm28212-1/processed" "$(pwd)/processed" "/dls/m12/data/2021/cm28212-1/tmp" "$(pwd)/tmp" '"$ispyb_autoprocprogram_id"' "83862530" < rw > rw-local
# - run the wrapper:
#   dlstbx.wrap --recipewrapper=rw-local --wrap=relion --offline -v


class RelionWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        # Enable log messages for relion.*
        logging.getLogger("relion").setLevel(logging.INFO)

        # Report python-relion package version
        self.status_thread.set_static_status_field("python-relion", relion.__version__)

        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        self.params = self.recwrap.recipe_step["job_parameters"]
        self.working_directory = pathlib.Path(self.params["working_directory"])
        self.results_directory = pathlib.Path(self.params["results_directory"])

        # Here we cheat. Ultimately we want to run the processing inside the
        # 'working' directory, and then copy relevant output into the 'results'
        # directory as we go along. In the first instance we will just run Relion
        # inside the 'results' directory, and ignore the 'working' directory
        # completely.
        self.working_directory = self.results_directory

        # Create working and results directory
        self.working_directory.mkdir(parents=True, exist_ok=True)
        self.results_directory.mkdir(parents=True, exist_ok=True)
        if self.params.get("create_symlink"):
            # Create symbolic link above directories
            # We want users to go to the most recent execution, as they can stop
            # and restart processing from SynchWeb. Thus we overwrite symlinks.
            zocalo.util.symlink.create_parent_symlink(
                self.working_directory,
                self.params["create_symlink"],
                overwrite_symlink=True,
            )
            zocalo.util.symlink.create_parent_symlink(
                self.results_directory,
                self.params["create_symlink"],
                overwrite_symlink=True,
            )

        # Relion needs to have a 'Movies' link inside the working directory
        # pointing to the image files
        movielink = "Movies"
        os.symlink(self.params["image_directory"], self.working_directory / movielink)
        self.params["ispyb_parameters"]["import_images"] = os.path.join(
            movielink,
            pathlib.Path(self.params["ispyb_parameters"]["import_images"]).relative_to(
                self.params["image_directory"]
            ),
        )

        # Debug output
        pprint(self.params)

        # Select specific Cryolo version if so desired
        if self.params.get("cryolo_version"):
            os.environ["CRYOLO_VERSION"] = self.params["cryolo_version"]
            logger.info("Selected cryolo version %s", self.params["cryolo_version"])

        # Initialise number of imported files to 0
        imported_files = []
        mc_job_time_all_processed = None

        # Start Relion
        self._relion_subthread = threading.Thread(
            target=self.start_relion, name="relion_subprocess_runner", daemon=True
        )
        self._relion_subthread.start()

        relion_started = time.time()

        preprocess_check = self.results_directory / "RUNNING_PIPELINER_PREPROCESS"
        all_process_check = self.results_directory / "RUNNING_RELION_IT"

        relion_prj = relion.Project(
            self.working_directory,
            message_constructors={
                "ispyb": construct_message,
                "images": images_msgs,
            },
        )

        while not relion_prj.origin_present() or not preprocess_check.is_file():
            time.sleep(0.5)
            if time.time() - relion_started > 10 * 60:
                break

        relion_prj.load()

        preproc_recently_run = False
        processing_ended = False
        should_send_icebreaker = True
        icebreaker_particles_star_file_found = False

        while (
            self._relion_subthread.is_alive() or preprocess_check.is_file()
        ) and False not in [n.environment["status"] for n in relion_prj if n._out]:
            time.sleep(1)

            # logger.info("Looking for results")

            ispyb_command_list = []
            images_command_list = []

            if pathlib.Path(self.params["stop_file"]).is_file():
                relion_prj.load()
                for job_path in relion_prj._job_nodes:
                    (
                        self.results_directory
                        / job_path.name
                        / "RELION_JOB_EXIT_ABORTED"
                    ).touch()
                for p in self.results_directory.glob("RUNNING_*"):
                    p.unlink()

            relion_prj.load()

            # Should only return results that have not previously been sent

            # for fr in relion_prj.results.fresh:
            #    curr_res = ispyb_results(fr.stage_object, fr.job_name, self.opts)
            #    ispyb_command_list.extend(curr_res)
            #    images_command_list.extend(images_msgs(fr.stage_object, fr.job_name))
            #    if curr_res:
            #        logger.info(f"Fresh results found for {fr.job_name}")

            # if ispyb_command_list:
            #    logger.info(
            #        "Sending commands like this: %s", str(ispyb_command_list[0])
            #    )
            #    self.recwrap.send_to(
            #        "ispyb", {"ispyb_command_list": ispyb_command_list}
            #    )
            #    logger.info("Sent %d commands to ISPyB", len(ispyb_command_list))

            # Should only return results that have not previously been sent

            for job_msg in relion_prj.messages:
                if job_msg.get("ispyb") and job_msg["ispyb"]:
                    logger.info(
                        f"Found results that look like this: {job_msg['ispyb'][0]}"
                    )
                    ispyb_command_list.extend(job_msg["ispyb"])
                if job_msg.get("images") and job_msg["images"]:
                    images_command_list.extend(job_msg["images"])

            if ispyb_command_list:
                logger.info(
                    "Sending commands like this: %s", str(ispyb_command_list[0])
                )
                self.recwrap.send_to(
                    "ispyb", {"ispyb_command_list": ispyb_command_list}
                )
                logger.info("Sent %d commands to ISPyB", len(ispyb_command_list))

            for imgcmd in images_command_list:
                self.recwrap.send_to("images", imgcmd)

            ### Extract and send Icebreaker results as histograms if the Icebreaker grouping job has run
            if not self.opts.stop_after_ctf_estimation and (
                self.opts.do_class2d or self.opts.do_class3d
            ):
                attachment_list = []
                try:
                    pdf_file_path = icebreaker_histogram.create_pdf_histogram(
                        self.working_directory
                    )
                    json_file_path = icebreaker_histogram.create_json_histogram(
                        self.working_directory
                    )
                    if should_send_icebreaker and pdf_file_path and json_file_path:
                        attachment_list.append(
                            ispyb_attachment(json_file_path, "Graph")
                        )
                        attachment_list.append(ispyb_attachment(pdf_file_path, "Graph"))
                        logger.info(f"Sending ISPyB attachments {attachment_list}")
                        self.recwrap.send_to(
                            "ispyb", {"ispyb_command_list": attachment_list}
                        )
                        should_send_icebreaker = False
                        icebreaker_particles_star_file_found = True
                except (FileNotFoundError, OSError, RuntimeError, ValueError):
                    logger.error("Error creating Icebreaker histogram.")

            # if Relion has been running too long stop loop of preprocessing jobs
            try:
                most_recent_movie = max(
                    p.stat().st_mtime
                    for p in pathlib.Path(self.params["image_directory"]).glob("**/*")
                )
            # if a file vanishes for some reason make sure that there is no crash and no exit
            except FileNotFoundError:
                most_recent_movie = time.time()

            # check if all imported files have been motion corrected
            # if they have then get the time stamp of the motion correction job
            # so that it can be checked all preprocessing jobs have run after it
            # only do this if the number of imported files has changed
            new_imported_files = relion_prj.get_imported()
            if new_imported_files != imported_files or not mc_job_time_all_processed:
                imported_files = new_imported_files
                mc_job_time_all_processed = self.check_processing_of_imports(
                    relion_prj, imported_files
                )

            if len(relion_prj._job_nodes) != 0 and len(relion_prj.preprocess) != 0:
                for job in relion_prj.preprocess:
                    job_end_time = job.environment["end_time_stamp"]
                    if job_end_time is None:
                        break
                    if (
                        datetime.datetime.timestamp(job_end_time) < most_recent_movie
                        or job.environment["job_count"] < 1
                    ):
                        break
                    # don't need to check if Import job has run recently for this bit
                    if mc_job_time_all_processed and "Import" not in job.name:
                        if (
                            datetime.datetime.timestamp(job_end_time)
                            < mc_job_time_all_processed
                        ):
                            preproc_recently_run = False
                            break
                else:
                    preproc_recently_run = True

            currtime = time.time()
            if (
                currtime - most_recent_movie
                > int(self.params["latest_movie_timeout"]) * 60
                and currtime - relion_started > 10 * 60
                and preprocess_check.is_file()
                and preproc_recently_run
                and mc_job_time_all_processed
            ):
                logger.info(
                    f"Ending preprocessing: current time {currtime}, most recent movie {most_recent_movie}, time at which all micrographs were processed {mc_job_time_all_processed}"
                )
                preprocess_check.unlink()

            processing_ended = self.check_whether_ended(relion_prj)
            if (
                currtime - most_recent_movie
                > int(self.params["latest_movie_timeout"]) * 60
                and currtime - relion_started > 10 * 60
                and all_process_check.is_file()
                and processing_ended
                and mc_job_time_all_processed
            ):
                logger.info(
                    f"Ending all processing: current time {currtime}, most recent movie {most_recent_movie}"
                )
                all_process_check.unlink()

        if not icebreaker_particles_star_file_found:
            logger.warning("No particles.star file found for Icebreaker grouping.")
        logger.info("Done.")
        success = False not in [n.environment["status"] for n in relion_prj if n._out]

        if preprocess_check.is_file():
            preprocess_check.unlink()

        if all_process_check.is_file():
            all_process_check.unlink()

        return success

    @staticmethod
    def check_processing_of_imports(relion_prj, imported):
        try:
            checked_key = "job002"
            checks = [False for _ in range(len(imported))]
            for i, f in enumerate(imported):
                keys = [
                    (j.environment["job"], j)
                    for j in relion_prj._jobtype_nodes
                    if j.name == "MotionCorr"
                ]
                for key, job in keys:
                    if any(
                        f.split(".")[0] in p.micrograph_name.split(".")[0]
                        for p in job.environment["result"][key]
                    ):
                        checks[i] = True
                        checked_key = key
                        break
            if all(checks):
                completion_time = relion_prj._job_nodes.get_by_name(
                    "MotionCorr/" + checked_key
                ).environment["end_time_stamp"]
                if completion_time:
                    return datetime.datetime.timestamp(completion_time)
            return
        except (KeyError, AttributeError, RuntimeError, FileNotFoundError) as e:
            logger.debug(
                f"Exception encountered while checking whether imported files have been processed: {e}",
                exc_info=True,
            )
            return

    def start_relion(self):
        print("Running RELION wrapper - stdout")
        logger.info("Running RELION wrapper - logger.info")

        for k, v in self.params["ispyb_parameters"].items():
            if v.isnumeric():
                self.params["ispyb_parameters"][k] = int(v)
            elif v.lower() == "true":
                self.params["ispyb_parameters"][k] = True
            elif v.lower() == "false":
                self.params["ispyb_parameters"][k] = False
            else:
                try:
                    self.params["ispyb_parameters"][k] = float(v)
                except ValueError:
                    pass
        pprint(self.params["ispyb_parameters"])

        # Prepare options object
        opts = RelionItOptions()
        opts.update_from(vars(dls_options))
        opts.update_from(self.params["ispyb_parameters"])

        self.opts = opts

        # Write options to disk for a record of parameters used
        options_file = self.working_directory / cryolo_relion_it.OPTIONS_FILE
        logger.info(f"Writing all options to {options_file}")
        if os.path.isfile(options_file):
            logger.info(
                f"File {options_file} already exists; renaming old copy to {options_file}~"
            )
            os.rename(options_file, f"{options_file}~")
        with open(options_file, "w") as optfile:
            opts.print_options(optfile)

        success = False
        oldpwd = os.getcwd()
        try:
            os.chdir(self.working_directory)
            cryolo_relion_it.run_pipeline(opts)
            success = True
        except Exception as ex:
            logger.error(ex)
        finally:
            os.chdir(oldpwd)

        logger.info("Done.")
        return success

    def check_whether_ended(self, proj):
        if len(proj._job_nodes) == 0 or None in [j.environment["status"] for j in proj]:
            return False
        check_time = time.time()
        return all(
            check_time - datetime.datetime.timestamp(j.environment["end_time_stamp"])
            > 10 * 60
            for j in proj
        )

    def create_synchweb_stop_file(self):
        pathlib.Path(self.params["stop_file"]).touch()

    def get_status(self, job_path):
        relion_stop_files = [
            "RELION_JOB_EXIT_SUCCESS",
            "RELION_EXIT_FAILURE",
            "RELION_JOB_ABORT_NOW",
            "RELION_EXIT_ABORTED",
        ]
        # synchweb_stop_files = [synchweb stop files list]
        # job_finished_files = [relion job finished files]
        for item in relion_stop_files:
            if (job_path / item).is_file():  # or synchweb_stop_file exists:
                return RelionStatus.SUCCESS
            else:
                return RelionStatus.RUNNING
            # if job_finished_file exists:


@functools.singledispatch
def images_msgs(table, primary_key, **kwargs):
    logger.debug(f"{table!r} does not have associated images")
    return []


@images_msgs.register(MotionCorrectionTable)
def _(table: MotionCorrectionTable, primary_key: int, **kwargs):
    return {
        "file": table.get_row_by_primary_key(primary_key)[
            "micrograph_snapshot_full_path"
        ].replace(".jpeg", ".mrc")
    }


@images_msgs.register(CTFTable)
def _(table: CTFTable, primary_key: int, **kwargs):
    return {
        "file": table.get_row_by_primary_key(primary_key)[
            "fft_theoretical_full_path"
        ].replace(".jpeg", ".ctf")
    }


@images_msgs.register(ParticleClassificationGroupTable)
def _(table: ParticleClassificationGroupTable, primary_key: int, **kwargs):
    image_path = table.get_row_by_primary_key(primary_key)["class_images_stack"]
    if not image_path:
        return {}
    return {"file": image_path, "all_frames": "true"}


@functools.singledispatch
def ispyb_results(
    relion_stage_object, job_string: str, relion_options: RelionItOptions
):
    """
    A function that takes Relion stage objects and job names (together
    representing a single job directory) and translates them into ISPyB
    service commands.
    """
    raise ValueError(f"{relion_stage_object!r} is not a known Relion object")


@ispyb_results.register(relion.InitialModel)
def _(
    stage_object: relion.InitialModel, job_string: str, relion_options: RelionItOptions
):
    return []


@ispyb_results.register(relion.CTFFind)
def _(stage_object: relion.CTFFind, job_string: str, relion_options: RelionItOptions):
    logger.info("Generating ISPyB commands for %s ", job_string)
    ispyb_command_list = []
    for ctf_micrograph in stage_object[job_string]:
        ispyb_command_list.append(
            {
                "ispyb_command": "insert_ctf",
                "micrograph_name": ctf_micrograph.micrograph_name,
                "astigmatism": ctf_micrograph.astigmatism,
                "astigmatism_angle": ctf_micrograph.defocus_angle,
                "max_estimated_resolution": ctf_micrograph.max_resolution,
                "estimated_defocus": (
                    float(ctf_micrograph.defocus_u) + float(ctf_micrograph.defocus_v)
                )
                / 2,
                "cc_value": ctf_micrograph.fig_of_merit,
                "amplitude_contrast": ctf_micrograph.amp_contrast,
                "fft_theoretical_full_path": ctf_micrograph.diagnostic_plot_path,
                "box_size_x": relion_options.ctffind_boxsize,
                "box_size_y": relion_options.ctffind_boxsize,
                "min_resolution": relion_options.ctffind_minres,
                "max_resolution": relion_options.ctffind_maxres,
                "min_defocus": relion_options.ctffind_defocus_min,
                "max_defocus": relion_options.ctffind_defocus_max,
                "defocus_step_size": relion_options.ctffind_defocus_step,
            }
        )
    return ispyb_command_list


@ispyb_results.register(relion.MotionCorr)
def _(
    stage_object: relion.MotionCorr, job_string: str, relion_options: RelionItOptions
):
    logger.info("Generating ISPyB commands for %s ", job_string)
    ispyb_command_list = []
    for motion_corr_micrograph in stage_object[job_string]:
        number_of_frames = len(motion_corr_micrograph.drift_data)
        ispyb_command_list.append(
            {
                "ispyb_command": "insert_motion_correction",
                "micrograph_name": motion_corr_micrograph.micrograph_name,
                "total_motion": motion_corr_micrograph.total_motion,
                "early_motion": motion_corr_micrograph.early_motion,
                "late_motion": motion_corr_micrograph.late_motion,
                "average_motion_per_frame": (float(motion_corr_micrograph.total_motion))
                / number_of_frames,
                "dose_per_frame": relion_options.motioncor_doseperframe,
                "patches_used_x": relion_options.motioncor_patches_x,
                "patches_used_y": relion_options.motioncor_patches_y,
                "image_number": motion_corr_micrograph.micrograph_number,
                "micrograph_snapshot_full_path": motion_corr_micrograph.micrograph_snapshot_full_path,
                "drift_frames": [
                    (frame.frame, frame.deltaX, frame.deltaY)
                    for frame in motion_corr_micrograph.drift_data
                ],
            }
        )
    return ispyb_command_list


@ispyb_results.register(relion.AutoPick)
def _(stage_object: relion.AutoPick, job_string: str, relion_options: RelionItOptions):
    ispyb_command_list = [
        {
            "ispyb_command": "insert_particle_picker",
            "number_of_particles": mic.number_of_particles,
            "particle_diameter": relion_options.autopick_LoG_diam_max
            / 10,  # units are nm not Angstrom in the DB
            "micrograph_name": mic.micrograph_full_path,
        }
        for mic in stage_object[job_string]
    ]
    return ispyb_command_list


@ispyb_results.register(relion.Cryolo)
def _(stage_object: relion.Cryolo, job_string: str, relion_options: RelionItOptions):
    ispyb_command_list = [
        {
            "ispyb_command": "insert_particle_picker",
            "number_of_particles": mic.number_of_particles,
            "particle_diameter": int(
                relion_options.extract_boxsize
                * relion_options.angpix
                / relion_options.motioncor_binning
            )
            / 10,
            "particle_picking_template": relion_options.cryolo_gmodel,
            "micrograph_name": mic.micrograph_full_path,
        }
        for mic in stage_object[job_string]
    ]
    return ispyb_command_list


@ispyb_results.register(relion.Class2D)
def _(stage_object: relion.Class2D, job_string: str, relion_options: RelionItOptions):
    ispyb_command_list = []
    sorted_jobs = sorted(
        [st for st in stage_object.keys()], key=lambda st: int(st.replace("job", ""))
    )
    batch_number = sorted_jobs.index(job_string) + 1
    for class_2d in stage_object[job_string]:
        ispyb_command_list.append(
            {
                "ispyb_command": "insert_class2d",
                "number_of_particles_per_batch": relion_options.batch_size,
                "number_of_classes_per_batch": relion_options.class2d_nr_classes,
                "type": "2D",
                "symmetry": relion_options.symmetry,
                "class_number": class_2d.particle_sum[0],
                "particles_per_class": class_2d.particle_sum[1],
                "rotation_accuracy": class_2d.accuracy_rotations,
                "translation_accuracy": class_2d.accuracy_translations_angst,
                "estimated_resolution": class_2d.estimated_resolution,
                "overall_fourier_completeness": class_2d.overall_fourier_completeness,
                "batch_number": batch_number,
            }
        )
    return ispyb_command_list


@ispyb_results.register(relion.Class3D)
def _(stage_object: relion.Class3D, job_string: str, relion_options: RelionItOptions):
    ispyb_command_list = []
    sorted_jobs = sorted(
        [st for st in stage_object.keys()], key=lambda st: int(st.replace("job", ""))
    )
    batch_number = sorted_jobs.index(job_string) + 1
    for class_3d in stage_object[job_string]:
        ispyb_command_list.append(
            {
                "ispyb_command": "insert_class3d",
                "number_of_particles_per_batch": relion_options.batch_size,
                "number_of_classes_per_batch": relion_options.class3d_nr_classes,
                "type": "3D",
                "symmetry": relion_options.symmetry,
                "class_number": class_3d.particle_sum[0],
                "particles_per_class": class_3d.particle_sum[1],
                "rotation_accuracy": class_3d.accuracy_rotations,
                "translation_accuracy": class_3d.accuracy_translations_angst,
                "estimated_resolution": class_3d.estimated_resolution,
                "overall_fourier_completeness": class_3d.overall_fourier_completeness,
                "batch_number": batch_number,
                "init_model_number_of_particles": class_3d.initial_model_num_particles,
                "init_model_resolution": relion_options.inimodel_resol_final,
            }
        )
    return ispyb_command_list


@functools.singledispatch
def construct_message(table, primary_key, resend=False):
    raise ValueError(f"{table!r} is not a known Table")


@construct_message.register(MotionCorrectionTable)
def _(table: MotionCorrectionTable, primary_key: int, resend: bool = False):
    row = table.get_row_by_primary_key(primary_key)
    buffered = ["motion_correction_id"]
    buffer_store = row["motion_correction_id"]
    results = {
        "ispyb_command": "buffer",
        "buffer_command": {
            "ispyb_command": "insert_motion_correction_buffer",
            **{k: v for k, v in row.items() if k not in buffered},
        },
        "buffer_store": buffer_store,
    }
    return results


@construct_message.register(CTFTable)
def _(table: CTFTable, primary_key: int, resend: bool = False):
    row = table.get_row_by_primary_key(primary_key)
    buffered = ["motion_correction_id", "ctf_id"]
    buffer_store = row["ctf_id"]
    buffer_lookup = row["motion_correction_id"]
    if resend:
        results = {
            "ispyb_command": "buffer",
            "buffer_lookup": {
                "motion_correction_id": buffer_lookup,
                "ctf_id": buffer_store,
            },
            "buffer_command": {
                "ispyb_command": "insert_ctf_buffer",
                **{k: v for k, v in row.items() if k not in buffered},
            },
        }
    else:
        results = {
            "ispyb_command": "buffer",
            "buffer_lookup": {
                "motion_correction_id": buffer_lookup,
            },
            "buffer_command": {
                "ispyb_command": "insert_ctf_buffer",
                **{k: v for k, v in row.items() if k not in buffered},
            },
            "buffer_store": buffer_store,
        }
    return results


@construct_message.register(ParticlePickerTable)
def _(table: ParticlePickerTable, primary_key: int, resend: bool = False):
    row = table.get_row_by_primary_key(primary_key)
    buffered = ["first_motion_correction_id", "particle_picker_id"]
    buffer_store = row["particle_picker_id"]
    buffer_lookup = row["first_motion_correction_id"]
    if resend:
        results = {
            "ispyb_command": "buffer",
            "buffer_lookup": {
                "motion_correction_id": buffer_lookup,
                "particle_picker_id": buffer_store,
            },
            "buffer_command": {
                "ispyb_command": "insert_particle_picker_buffer",
                **{k: v for k, v in row.items() if k not in buffered},
            },
        }
    else:
        results = {
            "ispyb_command": "buffer",
            "buffer_lookup": {
                "motion_correction_id": buffer_lookup,
            },
            "buffer_command": {
                "ispyb_command": "insert_particle_picker_buffer",
                **{k: v for k, v in row.items() if k not in buffered},
            },
            "buffer_store": buffer_store,
        }
    return results


@construct_message.register(ParticleClassificationGroupTable)
def _(table: ParticleClassificationGroupTable, primary_key: int, resend: bool = False):
    row = table.get_row_by_primary_key(primary_key)
    buffered = ["particle_picker_id", "particle_classification_group_id"]
    buffer_store = row["particle_classification_group_id"]
    buffer_lookup = row["particle_picker_id"]
    if resend:
        results = {
            "ispyb_command": "buffer",
            "buffer_lookup": {
                "particle_picker_id": buffer_lookup,
                "particle_classification_group_id": buffer_store,
            },
            "buffer_command": {
                "ispyb_command": "insert_particle_classification_group",
                **{k: v for k, v in row.items() if k not in buffered},
            },
        }
    else:
        results = {
            "ispyb_command": "buffer",
            "buffer_lookup": {
                "particle_picker_id": buffer_lookup,
            },
            "buffer_command": {
                "ispyb_command": "insert_particle_classification_group",
                **{k: v for k, v in row.items() if k not in buffered},
            },
            "buffer_store": buffer_store,
        }
    return results


@construct_message.register(ParticleClassificationTable)
def _(table: ParticleClassificationTable, primary_key: int, resend: bool = False):
    row = table.get_row_by_primary_key(primary_key)
    buffered = ["particle_classification_group_id", "particle_classification_id"]
    buffer_store = row["particle_classification_id"]
    buffer_lookup = row["particle_classification_group_id"]
    if resend:
        results = {
            "ispyb_command": "buffer",
            "buffer_lookup": {
                "particle_classification_group_id": buffer_lookup,
                "particle_classification_id": buffer_store,
            },
            "buffer_command": {
                "ispyb_command": "insert_particle_classification",
                **{k: v for k, v in row.items() if k not in buffered},
            },
        }
    else:
        results = {
            "ispyb_command": "buffer",
            "buffer_lookup": {
                "particle_classification_group_id": buffer_lookup,
            },
            "buffer_command": {
                "ispyb_command": "insert_particle_classification",
                **{k: v for k, v in row.items() if k not in buffered},
            },
            "buffer_store": buffer_store,
        }
    return results


@construct_message.register(CryoemInitialModelTable)
def _(table: CryoemInitialModelTable, primary_key: int, resend: bool = False):
    row = table.get_row_by_primary_key(primary_key)
    class_ids = row["particle_classification_id"]
    if not isinstance(class_ids, list):
        class_ids = [class_ids]
    results = []
    for class_id in class_ids:
        buffered = ["particle_classification_id", "cryoem_initial_model_id"]
        this_result = {
            "ispyb_command": "buffer",
            "buffer_lookup": {
                "particle_classification_id": class_id,
            },
            "buffer_command": {
                "ispyb_command": "insert_cryoem_initial_model",
                **{k: v for k, v in row.items() if k not in buffered},
            },
        }
        results.append(this_result)
    return results


def ispyb_attachment(attachment_path_object, file_type):
    return {
        "ispyb_command": "add_program_attachment",
        "file_name": os.fspath(attachment_path_object.name),
        "file_path": os.fspath(attachment_path_object.parent),
        "file_type": file_type,
    }

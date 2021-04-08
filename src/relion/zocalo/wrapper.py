import enum
import functools
import logging
import os
import pathlib
import procrunner
import relion
import threading
import time
import zocalo.util.symlink
import zocalo.wrapper
from pprint import pprint
from relion.cryolo_relion_it import cryolo_relion_it, dls_options

logger = logging.getLogger("dlstbx.wrap.relion")

RelionStatus = enum.Enum("RelionStatus", "RUNNING SUCCESS FAILURE")

# to test:
# - create an empty directory
# - run following command to generate a recipe-wrapper:
#   dlstbx.find_in_ispyb -p 6844019 -f ~/zocalo/recipes/ispyb-relion.json --recipe-pointer=2 --out=rw ; replace "/dls/m12/data/2021/cm28212-1/processed" "$(pwd)/processed" "/dls/m12/data/2021/cm28212-1/tmp" "$(pwd)/tmp" < rw > rw-local
# - run the wrapper:
#   dlstbx.wrap --recipewrapper=rw-local --wrap=relion --offline -v


class RelionWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
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
            movielink, self.params["file_template"]
        )

        # Debug output
        pprint(self.params)

        # Start Relion
        self._relion_subthread = threading.Thread(
            target=self.start_relion, name="relion_subprocess_runner", daemon=True
        )
        self._relion_subthread.start()

        relion_prj = relion.Project(self.working_directory)
        while self._relion_subthread.is_alive():
            time.sleep(1)
            logger.info("Looking for results")

            ispyb_command_list = []
            try:
                motion_corr_status = self.get_job_status_dictionary(
                    relion_prj.motioncorrection
                )
                for item in motion_corr_status:
                    if all(
                        x == RelionStatus.SUCCESS for x in motion_corr_status.values()
                    ):
                        logger.info("Motion Correction finished")
                        # break
                    # elif
                    if motion_corr_status[item] == RelionStatus.SUCCESS:
                        ispyb_command_list.extend(ispyb_results(item[0], item[1]))
            except FileNotFoundError:
                logger.info("No files found for Motion Correction")

            try:
                ctf_status = self.get_job_status_dictionary(relion_prj.ctffind)
                for item in ctf_status:
                    if all(x == RelionStatus.SUCCESS for x in ctf_status.values()):
                        logger.info("CTFFind finished")
                        # break
                    # elif
                    if ctf_status[item] == RelionStatus.SUCCESS:
                        ispyb_command_list.extend(ispyb_results(item[0], item[1]))
            except FileNotFoundError:
                logger.info("No files found for CTFFind")

            if ispyb_command_list:
                logger.info(
                    "Sending commands like this: %s", str(ispyb_command_list[0])
                )
                self.recwrap.send_to(
                    "ispyb", {"ispyb_command_list": ispyb_command_list}
                )
                logger.info("Sent %d commands to ISPyB", len(ispyb_command_list))

        logger.info("Done.")
        success = True
        return success

    def start_pseudo_relion(self):
        logger.info("Starting 'Relion'")
        result = procrunner.run(
            (
                "/bin/bash",
                "/home/wra62962/python-relion/relion/mimic-script",
            ),
            timeout=self.params.get("timeout"),
            working_directory=self.working_directory,
            environment_override={"PYTHONIOENCODING": "UTF-8"},
        )
        logger.info("command: %s", " ".join(result["command"]))
        logger.info("exitcode: %s", result["exitcode"])
        logger.debug(result["stdout"])
        logger.debug(result["stderr"])

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
        opts = cryolo_relion_it.RelionItOptions()
        opts.update_from(vars(dls_options))
        opts.update_from(self.params["ispyb_parameters"])

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
        try:
            cryolo_relion_it.run_pipeline(opts)
            success = True
        except Exception as ex:
            logger.error(ex)

        logger.info("Done.")
        return success

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

    def get_job_status_dictionary(self, stage_object):
        dictionary = {}
        job_set = {(stage_object, job) for job in stage_object}
        for x in job_set:
            path = stage_object._basepath / x[1]
            status = self.get_status(path)
            dictionary[x] = status
        return dictionary


@functools.singledispatch
def ispyb_results(relion_stage_object, job_string: str):
    """
    A function that takes Relion stage objects and job names (together
    representing a single job directory) and translates them into ISPyB
    service commands.
    """
    raise ValueError(f"{relion_stage_object!r} is not a known Relion object")


@ispyb_results.register(relion.CTFFind)
def _(stage_object: relion.CTFFind, job_string: str):
    logger.info("Generating ISPyB commands for %s ", job_string)
    ispyb_command_list = []
    for ctf_micrograph in stage_object[job_string]:
        ispyb_command_list.append(
            {
                "ispyb_command": "insert_ctf",
                "astigmatism": ctf_micrograph.astigmatism,
                "astigmatism_angle": ctf_micrograph.defocus_angle,
                "max_estimated_resolution": ctf_micrograph.max_resolution,
                "estimated_defocus": (
                    float(ctf_micrograph.defocus_u) + float(ctf_micrograph.defocus_v)
                )
                / 2,
                "cc_value": ctf_micrograph.fig_of_merit,
            }
        )
    return ispyb_command_list


@ispyb_results.register(relion.MotionCorr)
def _(stage_object: relion.MotionCorr, job_string: str):
    logger.info("Generating ISPyB commands for %s ", job_string)
    ispyb_command_list = []
    for motion_corr_micrograph in stage_object[job_string]:
        ispyb_command_list.append(
            {
                "ispyb_command": "insert_motion_correction",
                "micrograph_name": motion_corr_micrograph.micrograph_name,
                "total_motion": motion_corr_micrograph.total_motion,
                "early_motion": motion_corr_micrograph.early_motion,
                "late_motion": motion_corr_micrograph.late_motion,
                "average_motion_per_frame": (
                    float(motion_corr_micrograph.total_motion)
                ),  # / number of frames
            }
        )
    return ispyb_command_list

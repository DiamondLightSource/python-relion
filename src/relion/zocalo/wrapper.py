import logging
import os
import pathlib

# import dlstbx.util.symlink
import relion
import enum
import zocalo.wrapper
from pprint import pprint
import time
import subprocess

# import functools

logger = logging.getLogger("dlstbx.wrap.relion")

RelionStatus = enum.Enum("RelionStatus", "RUNNING EXIT_SUCCESS FAILURE")
RELION_RUNNING = True


class RelionWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        # self.working_directory = pathlib.Path(params["working_directory"])
        # self.working_directory = pathlib.Path(
        #    "/dls/science/groups/scisoft/DIALS/dials_data/relion_tutorial_data"
        # )
        self.working_directory = pathlib.Path("/home/slg25752/temp/relion")
        self.results_directory = pathlib.Path(params["results_directory"])
        # create working directory
        # self.working_directory.mkdir(parents=True, exist_ok=True)
        # if params.get("create_symlink"):
        # Create symbolic link above working directory
        #    dlstbx.util.symlink.create_parent_symlink(
        #        str(self.working_directory), params["create_symlink"]
        #    )

        # Create a symbolic link in the working directory to the image directory
        movielink = "Movies"
        # os.symlink(params["image_directory"], self.working_directory / movielink)
        params["ispyb_parameters"]["import_images"] = os.path.join(
            movielink, params["file_template"]
        )
        pprint(params["ispyb_parameters"])

        # construct relion command line
        # command = ["relion", params["screen-selection"]]

        # run relion
        # result = procrunner.run(
        #    command,
        #    timeout=params.get("timeout"),
        #    working_directory=working_directory.strpath,
        #    environment_override={"PYTHONIOENCODING": "UTF-8"},
        # )
        # logger.info("command: %s", " ".join(result["command"]))
        # logger.info("exitcode: %s", result["exitcode"])
        # logger.debug(result["stdout"])
        # logger.debug(result["stderr"])
        # success = result["exitcode"] == 0
        success = True

        # copy output files to result directory
        # self.results_directory.mkdir(parents=True, exist_ok=True)

        # if params.get("create_symlink"):
        # Create symbolic link above results directory
        #    dlstbx.util.symlink.create_parent_symlink(
        #       str(self.results_directory), params["create_symlink"]
        #    )

        relion_process = subprocess.Popen(["/home/slg25752/relion/mimic-relion-script"])
        self.start_relion()

        global RELION_RUNNING
        while RELION_RUNNING:

            time.sleep(10)

            relion_object = relion.Project(self.working_directory)
            logger.info("Looking for results")

            try:
                motion_corr_status = self.get_job_status_dictionary(
                    relion_object.motioncorrection
                )
                for item in motion_corr_status:
                    if all(
                        x == RelionStatus.EXIT_SUCCESS
                        for x in motion_corr_status.values()
                    ):
                        logger.info("Motion Correction finished")
                        # break
                    # elif
                    if motion_corr_status[item] == RelionStatus.EXIT_SUCCESS:
                        logger.info("Sending %s results for %s", item[0], item[1])
                        self.send_results(item[0], item[1])
                        # self.send_motioncorrection_results_to_ispyb(item[0], item[1])
            except FileNotFoundError:
                logger.info("No files found for Motion Correction")

            try:
                ctf_status = self.get_job_status_dictionary(relion_object.ctffind)
                for item in ctf_status:
                    if all(x == RelionStatus.EXIT_SUCCESS for x in ctf_status.values()):
                        logger.info("CTFFind finished")
                        # break
                    # elif
                    if ctf_status[item] == RelionStatus.EXIT_SUCCESS:
                        logger.info("Sending %s results for %s", item[0], item[1])
                        self.send_results(item[0], item[1])
                        # self.send_ctffind_results_to_ispyb(item[0], item[1])
            except FileNotFoundError:
                logger.info("No files found for CTFFind")

            poll = relion_process.poll()  # None value -> process is still running
            print("POLL RELION_RUNNING:", poll)
            if poll is not None:
                print("Relion stopped")
                RELION_RUNNING = False

            time.sleep(10)

        logger.info("Done.")
        return success

    def start_relion(self):
        pass

    def create_synchweb_stop_file(self, path_to_desired_file_location):
        pathlib.Path(path_to_desired_file_location / "stopfile.txt").touch()

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
                return RelionStatus.EXIT_SUCCESS
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

    def send_ctffind_results_to_ispyb(self, stage_object, job_string):
        logger.info("Sending results to ISPyB for %s ", job_string)
        ispyb_command_list = []
        for ctf_micrograph in stage_object[job_string]:
            ispyb_command_list.append(
                {
                    "ispyb_command": "insert_ctf",
                    "astigmatism": ctf_micrograph.astigmatism,
                    "astigmatism_angle": ctf_micrograph.defocus_angle,
                    "max_estimated_resolution": ctf_micrograph.max_resolution,
                    "estimated_defocus": (
                        float(ctf_micrograph.defocus_u)
                        + float(ctf_micrograph.defocus_v)
                    )
                    / 2,
                    "cc_value": ctf_micrograph.fig_of_merit,
                }
            )
        logger.info("Sending commands like this: %s", str(ispyb_command_list[0]))
        self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_command_list})
        logger.info("Sent %d commands to ISPyB", len(ispyb_command_list))

    def send_motioncorrection_results_to_ispyb(self, stage_object, job_string):
        logger.info("Sending results to ISPyB for %s ", job_string)
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
        logger.info("Sending commands like this: %s", str(ispyb_command_list[0]))
        self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_command_list})
        logger.info("Sent %d commands to ISPyB", len(ispyb_command_list))

    # @functools.singledispatch
    def send_results(self, relion_stage_object, job_string):
        """
        A generic send function that takes a Relion
        object and uses the corresponding send_results function.
        """
        if isinstance(relion_stage_object, relion.CTFFind):
            self.send_ctffind_results_to_ispyb(relion_stage_object, job_string)

        elif isinstance(relion_stage_object, relion.MotionCorr):
            self.send_motioncorrection_results_to_ispyb(relion_stage_object, job_string)

        else:
            raise ValueError(f"{relion_stage_object!r} is not a known Relion object")


# @send_results.register(relion.CTFFind)
# def _(relionobject: relion.CTFFind, job_string):
#    RelionWrapper.send_ctffind_results_to_ispyb(relion.CTFFind, job_string)


# @send_results.register(relion.MotionCorr)
# def _(relionobject: relion.MotionCorr, job_string):
#    RelionWrapper.send_motioncorrection_results_to_ispyb(relion.MotionCorr, job_string)

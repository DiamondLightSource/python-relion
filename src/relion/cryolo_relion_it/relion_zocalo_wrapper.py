import logging
import os
import pathlib
from pprint import pprint

from . import util_symlink
import zocalo.wrapper

logger = logging.getLogger("relion_yolo_it.relion_zocalo_wrapper")


class RelionWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        # TEMP test output and logging
        print("Running RELION wrapper - stdout")
        logger.info("Running RELION wrapper - logger.info")

        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        working_directory = pathlib.Path(params["working_directory"])
        results_directory = pathlib.Path(params["results_directory"])

        # create working directory
        working_directory.mkdir(parents=True, exist_ok=True)
        if params.get("create_symlink"):
            # Create symbolic link above working directory
            util_symlink.create_parent_symlink(
                str(working_directory), params["create_symlink"]
            )

        # Create a symbolic link in the working directory to the image directory
        movielink = "Movies"
        movielink_path = working_directory / movielink
        movielink_target = params["image_directory"]
        if os.path.islink(movielink_path):
            current_target = os.readlink(movielink_path)
            if current_target == movielink_target:
                logger.info(f"Using existing link {movielink_path} -> {current_target}")
            else:
                raise ValueError(
                    f"Trying to create link {movielink_path} -> {movielink_target} but a link already exists pointing to {current_target}"
                )
        else:
            logger.info(f"Creating link {movielink_path} -> {movielink_target}")
            os.symlink(movielink_target, movielink_path)

        params["ispyb_parameters"]["import_images"] = os.path.join(
            movielink, params["file_template"]
        )
        for k, v in params["ispyb_parameters"].items():
            if v.isnumeric():
                params["ispyb_parameters"][k] = int(v)
            elif v.lower() == "true":
                params["ispyb_parameters"][k] = True
            elif v.lower() == "false":
                params["ispyb_parameters"][k] = False
            else:
                try:
                    params["ispyb_parameters"][k] = float(v)
                except ValueError:
                    pass
        pprint(params["ispyb_parameters"])

        options_file = working_directory / "processing_options.py"
        logger.info(f"Writing options to {options_file}")
        with open(options_file, "w") as opts_file:
            for key, value in params["ispyb_parameters"].items():
                print(f"{key} = {value !r}", file=opts_file)

        # TODO: find a better way to configure these values
        relion_pipeline_python = "/dls_sw/apps/EM/relion_cryolo/relion-yolo-it-dev-env/bin/wrappers/conda/python"
        relion_pipeline_home = pathlib.Path(
            "/dls_sw/apps/EM/relion_cryolo/python-relion-yolo-it_relion3.1_dev/relion_yolo_it"
        )

        # Find relion_it.py script and standard DLS options
        relion_it = relion_pipeline_home / "cryolo_relion_it.py"
        dls_options = relion_pipeline_home / "dls_options.py"

        # construct relion command line
        relion_command = [relion_pipeline_python, relion_it, dls_options, options_file]

        # TEMP make a shell script to set up the necessary environment and run relion_it
        commands = [
            "#!/bin/bash",
            "source /etc/profile.d/modules.sh",
            "module load hamilton",
            "module load EM/yolo_relion_it/relion_3.1.1_cryolo_1.7.6",
            " ".join(["exec"] + [str(item) for item in relion_command]),
        ]
        script_file = working_directory / "run_script.sh"
        logger.info(f"Writing job commands to {script_file}")
        script_file.write_text("\n".join(commands))

        # run relion
        # (environment_override is necessary because the libtbx dispatcher sets LD_LIBRARY_PATH and PYTHONPATH)
        result = procrunner.run(
            ["bash", script_file],
            working_directory=working_directory,
            environment_override={
                "LD_LIBRARY_PATH": "",
                "_LMFILES_": "",
                "LOADEDMODULES": "",
                "PYTHONPATH": "",
                "PYTHONUNBUFFERED": "1",
            },
        )
        logger.info("command: %s", " ".join(result["command"]))
        logger.info("exitcode: %s", result["exitcode"])
        logger.debug(result["stdout"])
        logger.debug(result["stderr"])
        success = result["exitcode"] == 0

        # copy output files to result directory
        # results_directory.mkdir(parents=True, exist_ok=True)

        # if params.get("create_symlink"):
        #     # Create symbolic link above results directory
        #     util_symlink.create_parent_symlink(
        #         str(results_directory), params["create_symlink"]
        #     )

        logger.info("Done.")

        return success

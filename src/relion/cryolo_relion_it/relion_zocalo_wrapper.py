import logging
import os
import pathlib
from pprint import pprint

from . import cryolo_relion_it, dls_options, util_symlink
import zocalo.wrapper

logger = logging.getLogger("relion.cryolo_relion_it.relion_zocalo_wrapper")


class RelionWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        # TEMP test output and logging
        print("Running RELION wrapper - stdout")
        logger.info("Running RELION wrapper - logger.info")

        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["job_parameters"]
        working_directory = pathlib.Path(params["working_directory"])
        # results_directory = pathlib.Path(params["results_directory"])

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

        # Prepare options object
        opts = cryolo_relion_it.RelionItOptions()
        opts.update_from(vars(dls_options))
        opts.update_from(params["ispyb_parameters"])

        # Write options to disk for a record of parameters used
        options_file = working_directory / cryolo_relion_it.OPTIONS_FILE
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

        # copy output files to result directory
        # results_directory.mkdir(parents=True, exist_ok=True)

        # if params.get("create_symlink"):
        #     # Create symbolic link above results directory
        #     util_symlink.create_parent_symlink(
        #         str(results_directory), params["create_symlink"]
        #     )

        logger.info("Done.")

        return success

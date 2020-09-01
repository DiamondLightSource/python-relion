import logging
import pathlib
import relion
import zocalo.wrapper

logger = logging.getLogger("relion.zocalo.wrapper")


class RelionWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params = self.recwrap.recipe_step["job_parameters"]
        self.working_directory = pathlib.Path(params["working_directory"])
        self.send_results_to_ispyb()
        logger.info("Done.")
        return True

    def send_results_to_ispyb(self):
        logger.info("Reading Relion results")
        project = relion.Project(self.working_directory)
        ispyb_command_list = []
        for job in project.ctffind.values():
            for ctf_micrograph in job:
                ispyb_command_list.append(
                    {
                        "ispyb_command": "insert_ctf",
                        "max_resolution": ctf_micrograph.max_resolution,
                        "astigmatism": ctf_micrograph.astigmatism,
                        "astigmatism_angle": ctf_micrograph.astigmatism_angle,
                        "estimated_resolution": ctf_micrograph.estimated_resolution,
                        "estimated_defocus": ctf_micrograph.estimated_defocus,
                        "cc_value": ctf_micrograph.cc_value,
                    }
                )

        logger.info("Sending %s", str(ispyb_command_list))
        self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_command_list})
        logger.info("Sent %d commands to ISPyB", len(ispyb_command_list))

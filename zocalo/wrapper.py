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

        for job in project.motioncorrection.values():
            for motion_corr_micrograph in job:
                ispyb_command_list.append(
                    {
                        "ispyb_command": "insert_motion_corr",
                        "micrograph_name": motion_corr_micrograph.micrograph_name,
                        "total_motion": motion_corr_micrograph.accum_motion_total,
                        "early_motion": motion_corr_micrograph.accum_motion_early,
                        "late_motion": motion_corr_micrograph.accum_motion_late,
                        "average_motion_per_frame": (
                            float(motion_corr_micrograph.accum_motion_total)
                        ),  # / number of frames
                    }
                )

        for job in project.class2D.values():
            for class_entry in job:
                ispyb_command_list.append(
                    {
                        "ispyb_command": "insert_class2d",
                        "reference_image": class_entry.reference_image
                        # fields not yet in ISPyB
                    }
                )

        for job in project.class3D.values():
            for class_entry in job:
                ispyb_command_list.append(
                    {
                        "ispyb_command": "insert_class2d",
                        "reference_image": class_entry.reference_image
                        # fields not yet in ISPyB
                    }
                )

        logger.info("Sending %s", str(ispyb_command_list))
        self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_command_list})
        logger.info("Sent %d commands to ISPyB", len(ispyb_command_list))

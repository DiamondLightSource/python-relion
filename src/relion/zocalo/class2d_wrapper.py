from __future__ import annotations

import enum
import logging
import os
from pathlib import Path

import procrunner
import zocalo.wrapper

logger = logging.getLogger("relion.class2d.wrapper")

RelionStatus = enum.Enum("RelionStatus", "RUNNING SUCCESS FAILURE")

"""Message structure:
particles_file: Select/job009/particles_split1.star
class2d_dir: project_dir/Class2D/job010
particle_diameter: 256
nr_iter: 20
nr_classes: 50
relion_it_options: dict
"""

job_type = "relion.class2d.em"


class Class2DWrapper(zocalo.wrapper.BaseWrapper):
    def parse_class2d_output(self, line: str):
        """
        Read the output logs of relion 2D classification
        """
        if not line:
            return

        if line.startswith("CurrentResolution="):
            line_split = line.split()
            self.resolution -= int(line_split[1])

    def run(self):
        self.log.info("Running Class2D")
        self.resolution = -1

        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        class2d_params = self.recwrap.recipe_step["parameters"]

        # Make the job directory and move to the project directory
        job_dir = Path(class2d_params["class2d_dir"])
        job_dir.mkdir(parents=True, exist_ok=True)
        project_dir = job_dir.parent.parent
        os.chdir(project_dir)

        self.log.info(f"Changed to {project_dir}")

        class2d_command = [
            "relion_refine",
            "--i",
            str(Path(class2d_params["particles_file"]).relative_to(project_dir)),
            "--o",
            f"{job_dir.relative_to(project_dir)}/",
            "--dont_combine_weights_via_disc",
            "--preread_images",
            "--pool",
            "10",
            "--pad",
            "2",
            "--ctf",
            "--iter",
            class2d_params["nr_iter"],
            "--tau2_fudge",
            "2",
            "--particle_diameter",
            class2d_params["particle_diameter"],
            "--K",
            class2d_params["nr_classes"],
            "--flatten_solvent",
            "--zero_mask",
            "--center_classes",
            "--oversampling",
            "1",
            "--psi_step",
            "12.0",
            "--offset_range",
            "5",
            "--offset_step",
            "2.0",
            "--norm",
            "--scale",
            "--pipeline_control",
            f"{job_dir.relative_to(project_dir)}/",
        ]

        self.log.info(" ".join(class2d_command))

        # Run cryolo and confirm it ran successfully
        result = procrunner.run(
            command=class2d_command,
            callback_stdout=self.parse_class2d_output,
            working_directory=f"{project_dir}/",
        )
        if result.returncode:
            self.log.error(
                f"Relion Class2D failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            return

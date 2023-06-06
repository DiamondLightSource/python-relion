from __future__ import annotations

import enum
import logging
import os
from pathlib import Path
from typing import Optional

import procrunner
import zocalo.wrapper
from pydantic import BaseModel, Field
from pydantic.error_wrappers import ValidationError

logger = logging.getLogger("relion.class2d.wrapper")

RelionStatus = enum.Enum("RelionStatus", "RUNNING SUCCESS FAILURE")

job_type = "relion.class2d.em"


class Class2DParameters(BaseModel):
    particles_file: str = Field(..., min_length=1)
    class2d_dir: str = Field(..., min_length=1)
    particle_diameter: int
    nr_iter: int = 20
    nr_classes: int = 50
    mc_uuid: int
    relion_it_options: Optional[dict] = None


class Class2DWrapper(zocalo.wrapper.BaseWrapper):
    """
    A wrapper for the Relion 2D classification job.
    """

    # Values to extract for ISPyB
    resolution = -1

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
        """
        Run the 2D classification and register results
        """
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params_dict = self.recwrap.recipe_step["parameters"]
        try:
            class2d_params = Class2DParameters(**params_dict)
        except (ValidationError, TypeError):
            self.log.warning(
                f"Class2D parameter validation failed for parameters: {params_dict}."
            )
            return False

        # Make the job directory and move to the project directory
        job_dir = Path(class2d_params.class2d_dir)
        job_dir.mkdir(parents=True, exist_ok=True)
        project_dir = job_dir.parent.parent
        os.chdir(project_dir)

        particles_file = str(
            Path(class2d_params.particles_file).relative_to(project_dir)
        )
        self.log.info(f"Running Class2D for {particles_file}")

        class2d_command = [
            "relion_refine",
            "--i",
            particles_file,
            "--o",
            f"{job_dir.relative_to(project_dir)}/run",
            "--dont_combine_weights_via_disc",
            "--preread_images",
            "--pool",
            "10",
            "--pad",
            "2",
            "--ctf",
            "--iter",
            str(class2d_params.nr_iter),
            "--tau2_fudge",
            "2",
            "--particle_diameter",
            str(class2d_params.particle_diameter),
            "--K",
            str(class2d_params.nr_classes),
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

        # Run cryolo and confirm it ran successfully
        result = procrunner.run(
            command=class2d_command,
            callback_stdout=self.parse_class2d_output,
            working_directory=str(project_dir),
        )
        if result.returncode:
            self.log.error(
                f"Relion Class2D failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            return False

        node_creator_parameters = {
            "job_type": "relion.class2d.em",
            "input_file": class2d_params.particles_file,
            "output_file": class2d_params.class2d_dir,
            "relion_it_options": class2d_params.relion_it_options,
        }
        self.recwrap.send_to("spa.node_creator", node_creator_parameters)

        ispyb_insert = {"command": "classification"}
        self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_insert})

        return True

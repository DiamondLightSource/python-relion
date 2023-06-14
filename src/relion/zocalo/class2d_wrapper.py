from __future__ import annotations

import enum
import logging
import os
import re
from pathlib import Path
from typing import Optional

import procrunner
import zocalo.wrapper
from pydantic import BaseModel, Field
from pydantic.error_wrappers import ValidationError

logger = logging.getLogger("relion.class2d.wrapper")

RelionStatus = enum.Enum("RelionStatus", "RUNNING SUCCESS FAILURE")


class Class2DParameters(BaseModel):
    particles_file: str = Field(..., min_length=1)
    class2d_dir: str = Field(..., min_length=1)
    batch_is_complete: bool
    particle_diameter: float
    do_vdam = False
    dont_combine_weights_via_disc: bool = True
    preread_images: bool = True
    scratch_dir: str = None
    nr_pool: int = 10
    pad: int = 2
    skip_gridding: bool = False
    do_ctf: bool = True
    ctf_intact_first_peak: bool = False
    nr_iter: int = 20
    tau_fudge: float = 2
    nr_classes: int = 50
    flatten_solvent: bool = True
    do_zero_mask: bool = True
    highres_limit: float = None
    centre_classes: bool = True
    oversampling: int = 1
    skip_align: bool = False
    psi_step: float = 12.0
    offset_range: float = 5
    offset_step: float = 1
    allow_coarser: bool = False
    do_norm: bool = True
    do_scale: bool = True
    threads: int = 4
    gpus: str = "0"
    relion_it_options: Optional[dict] = None
    combine_star_job_number: int
    autoselect_min_score: int = 0


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
            self.resolution = int(line_split[1])

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

        if class2d_params.do_vdam:
            job_type = "relion.class2d.vdam"
        else:
            job_type = "relion.class2d.em"

        # Make the job directory and move to the project directory
        job_dir = Path(class2d_params.class2d_dir)
        job_dir.mkdir(parents=True, exist_ok=True)
        project_dir = job_dir.parent.parent
        os.chdir(project_dir)
        job_num = int(
            re.search("/job[0-9]{3}", str(class2d_params.class2d_dir))[0][4:7]
        )

        particles_file = str(
            Path(class2d_params.particles_file).relative_to(project_dir)
        )
        self.log.info(f"Running Class2D for {particles_file}")

        class2d_flags = {
            "dont_combine_weights_via_disc": "--dont_combine_weights_via_disc",
            "preread_images": "--preread_images",
            "scratch_dir": "--scratch_dir",
            "nr_pool": "--pool",
            "pad": "--pad",
            "skip_gridding": "--skip_gridding",
            "do_ctf": "--ctf",
            "ctf_intact_first_peak": "--ctf_intact_first_peak",
            "nr_iter": "--iter",
            "tau_fudge": "--tau2_fudge",
            "particle_diameter": "--particle_diameter",
            "nr_classes": "--K",
            "flatten_solvent": "--flatten_solvent",
            "do_zero_mask": "--zero_mask",
            "highres_limit": "--strict_highres_exp",
            "centre_classes": "--center_classes",
            "oversampling": "--oversampling",
            "skip_align": "--skip_align",
            "psi_step": "--psi_step",
            "offset_range": "--offset_range",
            "offset_step": "--offset_step",
            "allow_coarser": "--allow_coarser_sampling",
            "do_norm": "--norm",
            "do_scale": "--scale",
            "threads": "--j",
            "gpus": "--gpu",
        }

        # Create the classification command
        class2d_command = [
            "relion_refine",
            "--i",
            particles_file,
            "--o",
            f"{job_dir.relative_to(project_dir)}/run",
        ]
        for k, v in class2d_params.dict().items():
            if v and (k in class2d_flags):
                if type(v) is tuple:
                    class2d_command.extend(
                        (class2d_flags[k], " ".join(str(_) for _ in v))
                    )
                elif type(v) is bool:
                    class2d_command.append(class2d_flags[k])
                else:
                    class2d_command.extend((class2d_flags[k], str(v)))
        class2d_command.extend(
            ("--pipeline_control", f"{job_dir.relative_to(project_dir)}/")
        )
        with open(Path(class2d_params.class2d_dir) / "note.txt", "w") as f:
            f.write(" ".join(class2d_command))

        # Run Class2D and confirm it ran successfully
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

        # Register the Class2D job with the node creator
        self.log.info(f"Sending {job_type} to node creator")
        node_creator_parameters = {
            "job_type": job_type,
            "input_file": class2d_params.particles_file,
            "output_file": class2d_params.class2d_dir,
            "relion_it_options": class2d_params.relion_it_options,
        }
        self.recwrap.send_to("spa.node_creator", node_creator_parameters)

        # Send results to ispyb
        ispyb_parameters = {
            "type": "2D",
        }
        ispyb_parameters.update(
            {
                "ispyb_command": "buffer",
                "buffer_command": {
                    "ispyb_command": "insert_particle_classification_group"
                },
            }
        )
        self.log.info(f"Sending to ispyb {ispyb_parameters}")
        self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_parameters})

        if class2d_params.batch_is_complete:
            # Create an icebreaker job
            if class2d_params.relion_it_options["do_icebreaker_group"]:
                self.log.info("Sending to icebreaker particle analysis")
                icebreaker_params = {
                    "icebreaker_type": "particles",
                    "input_micrographs": (
                        f"{project_dir}/IceBreaker/job003/grouped_micrographs.star"
                    ),
                    "input_particles": class2d_params.particles_file,
                    "output_path": f"{project_dir}/IceBreaker/job{job_num + 1:03}/",
                    "relion_it_options": class2d_params.relion_it_options,
                }
                self.recwrap.send_to("icebreaker", icebreaker_params)

            # Create a 2D autoselection job
            self.log.info("Sending to class selection")
            autoselect_parameters = {
                "input_file": f"{class2d_params.class2d_dir}/run_it{class2d_params.nr_iter:03}_optimiser.star",
                "combine_star_job_number": class2d_params.combine_star_job_number,
                "min_score": class2d_params.autoselect_min_score,
                "particle_diameter": class2d_params.particle_diameter,
                "relion_it_options": class2d_params.relion_it_options,
            }
            self.recwrap.send_to("select.classes", autoselect_parameters)

        self.log.info(f"Done {job_type} for {class2d_params.particles_file}.")
        return True

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

logger = logging.getLogger("relion.class3d.wrapper")

RelionStatus = enum.Enum("RelionStatus", "RUNNING SUCCESS FAILURE")

job_type = "relion.class3d"


class Class3DParameters(BaseModel):
    particles_file: str = Field(..., min_length=1)
    class3d_dir: str = Field(..., min_length=1)
    particle_diameter: float
    do_initial_model: bool = False
    initial_model_file: str = None
    initial_model_iterations: int = 200
    initial_model_offset_range: float = 6
    initial_model_offset_step: float = 2
    do_initial_model_C1: bool = True
    dont_combine_weights_via_disc: bool = True
    preread_images: bool = True
    scratch_dir: str = None
    nr_pool: int = 10
    pad: int = 2
    skip_gridding: bool = False
    dont_correct_greyscale: bool = True
    ini_high: float = 40.0
    do_ctf: bool = True
    ctf_intact_first_peak: bool = False
    nr_iter: int = 20
    tau_fudge: float = 4
    nr_classes: int = 4
    flatten_solvent: bool = True
    do_zero_mask: bool = True
    highres_limit: float = None
    fn_mask: str = None
    oversampling: int = 1
    skip_align: bool = False
    healpix_order: float = 2
    offset_range: float = 5
    offset_step: float = 1
    allow_coarser: bool = False
    symmetry: str = "C1"
    do_norm: bool = True
    do_scale: bool = True
    threads: int = 4
    gpus: str = "0"
    mc_uuid: int
    relion_it_options: Optional[dict] = None


class Class3DWrapper(zocalo.wrapper.BaseWrapper):
    """
    A wrapper for the Relion 3D classification job.
    """

    # Values to extract for ISPyB
    resolution = -1

    common_flags = {
        "dont_combine_weights_via_disc": "--dont_combine_weights_via_disc",
        "preread_images": "--preread_images",
        "scratch_dir": "--scratch_dir",
        "nr_pool": "--pool",
        "pad": "--pad",
        "skip_gridding": "--skip_gridding",
        "do_ctf": "--ctf",
        "ctf_intact_first_peak": "--ctf_intact_first_peak",
        "particle_diameter": "--particle_diameter",
        "nr_classes": "--K",
        "flatten_solvent": "--flatten_solvent",
        "do_zero_mask": "--zero_mask",
        "oversampling": "--oversampling",
        "healpix_order": "--healpix_order",
        "threads": "--j",
        "gpus": "--gpu",
    }

    def parse_class3d_output(self, line: str):
        """
        Read the output logs of relion 3D classification
        """
        if not line:
            return

        if line.startswith("CurrentResolution="):
            line_split = line.split()
            self.resolution = int(line_split[1])

    def run_initial_model(self, initial_model_params, project_dir, job_num):
        """
        Run the initial model for 3D classification and register results
        """
        job_dir = project_dir / f"InitialModel/job{job_num:03}/"
        job_dir.mkdir(parents=True, exist_ok=True)
        particles_file = str(
            Path(initial_model_params.particles_file).relative_to(project_dir)
        )

        initial_model_flags = {
            "initial_model_iterations": "--iter",
            "initial_model_offset_range": "--offset_range",
            "initial_model_offset_step": "--offset_step",
        }
        initial_model_flags.update(self.common_flags)

        initial_model_command = [
            "relion_refine",
            "--grad",
            "--denovo_3dref",
            "--i",
            particles_file,
            "--o",
            f"{job_dir.relative_to(project_dir)}/run",
        ]
        if initial_model_params.do_initial_model_C1:
            initial_model_command.extend(("--sym", "C1"))
        else:
            initial_model_command.extend(("--sym", initial_model_params.symmetry))
        for k, v in initial_model_params.dict().items():
            if v and (k in initial_model_flags):
                if type(v) is tuple:
                    initial_model_command.extend(
                        (initial_model_flags[k], " ".join(str(_) for _ in v))
                    )
                elif type(v) is bool:
                    initial_model_command.append(initial_model_flags[k])
                else:
                    initial_model_command.extend((initial_model_flags[k], str(v)))
        initial_model_command.extend(
            ("--pipeline_control", f"{job_dir.relative_to(project_dir)}/")
        )

        # Run initial model and confirm it ran successfully
        self.log.info(f"Running {initial_model_command}")
        result = procrunner.run(
            command=initial_model_command,
            callback_stdout=self.parse_class3d_output,
            working_directory=str(project_dir),
        )
        if result.returncode:
            self.log.error(
                f"Relion initial model failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            return False

        ini_model_file = job_dir / "initial_model.mrc"

        align_symmetry_command = [
            "relion_align_symmetry",
            "--i",
            job_dir.relative_to(project_dir)
            / f"run_it{initial_model_params.initial_model_iterations:03}_model.star",
            "--o",
            ini_model_file.relative_to(project_dir),
            "--sym",
            initial_model_params.symmetry,
            "--apply_sym",
            "--select_largest_class",
            "--pipeline_control",
            job_dir.relative_to(project_dir),
        ]
        # Run symmetry alignment and confirm it ran successfully
        self.log.info(f"Running {align_symmetry_command}")
        result = procrunner.run(
            command=align_symmetry_command,
            callback_stdout=self.parse_class3d_output,
            working_directory=str(project_dir),
        )
        if result.returncode:
            self.log.error(
                f"Relion initial model symmetry alignment "
                f"failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            return False

        # Register the initial model job with the node creator
        self.log.info("Sending to node creator")
        node_creator_parameters = {
            "job_type": "relion.initialmodel",
            "input_file": f"{project_dir}/{particles_file}",
            "output_file": f"{job_dir}/initial_model.mrc",
            "relion_it_options": initial_model_params.relion_it_options,
        }
        self.recwrap.send_to("spa.node_creator", node_creator_parameters)

        return ini_model_file

    def run(self):
        """
        Run the 3D classification and register results
        """
        assert hasattr(self, "recwrap"), "No recipewrapper object found"
        params_dict = self.recwrap.recipe_step["parameters"]
        try:
            class3d_params = Class3DParameters(**params_dict)
        except (ValidationError, TypeError):
            self.log.warning(
                f"Class3D parameter validation failed for parameters: {params_dict}."
            )
            return False

        # Make the job directory and move to the project directory
        job_dir = Path(class3d_params.class3d_dir)
        job_dir.mkdir(parents=True, exist_ok=True)
        project_dir = job_dir.parent.parent
        os.chdir(project_dir)

        particles_file = str(
            Path(class3d_params.particles_file).relative_to(project_dir)
        )
        self.log.info(f"Running Class3D for {particles_file}")

        # Run the initial model if requested, otherwise look for a pre-existing file
        if class3d_params.do_initial_model:
            job_num_3d = int(
                re.search("/job[0-9]{3}", class3d_params.class3d_dir)[0][4:7]
            )
            initial_model_file = self.run_initial_model(
                class3d_params, project_dir, job_num_3d - 1
            )
        else:
            initial_model_file = str(
                Path(class3d_params.initial_model_file).relative_to(project_dir)
            )
        if not initial_model_file:
            # If there isn't an initial model file something has gone wrong
            return False

        class3d_flags = {
            "dont_correct_greyscale": "--firstiter_cc",
            "ini_high": "--ini_high",
            "nr_iter": "--iter",
            "tau_fudge": "--tau2_fudge",
            "highres_limit": "--strict_highres_exp",
            "fn_mask": "--solvent_mask",
            "skip_align": "--skip_align",
            "offset_range": "--offset_range",
            "offset_step": "--offset_step",
            "allow_coarser": "--allow_coarser_sampling",
            "symmetry": "--sym",
            "do_norm": "--norm",
            "do_scale": "--scale",
        }
        class3d_flags.update(self.common_flags)

        # Create the classification command
        class3d_command = [
            "relion_refine",
            "--i",
            particles_file,
            "--o",
            f"{job_dir.relative_to(project_dir)}/run",
            "--ref",
            initial_model_file,
        ]
        for k, v in class3d_params.dict().items():
            if v and (k in class3d_flags):
                if type(v) is tuple:
                    class3d_command.extend(
                        (class3d_flags[k], " ".join(str(_) for _ in v))
                    )
                elif type(v) is bool:
                    class3d_command.append(class3d_flags[k])
                else:
                    class3d_command.extend((class3d_flags[k], str(v)))
        class3d_command.extend(
            ("--pipeline_control", f"{job_dir.relative_to(project_dir)}/")
        )
        self.log.info(f"Running {class3d_command}")

        # Run Class3D and confirm it ran successfully
        result = procrunner.run(
            command=class3d_command,
            callback_stdout=self.parse_class3d_output,
            working_directory=str(project_dir),
        )
        if result.returncode:
            self.log.error(
                f"Relion Class3D failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            return False

        # Register the Class3D job with the node creator
        self.log.info("Sending to node creator")
        node_creator_parameters = {
            "job_type": "relion.class3d",
            "input_file": class3d_params.particles_file + f":{initial_model_file}",
            "output_file": class3d_params.class3d_dir,
            "relion_it_options": class3d_params.relion_it_options,
        }
        self.recwrap.send_to("spa.node_creator", node_creator_parameters)

        # Send results to ispyb
        ispyb_insert = {"command": "classification"}
        self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_insert})

        return True

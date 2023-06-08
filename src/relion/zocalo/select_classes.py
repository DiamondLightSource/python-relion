from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import numpy as np
import procrunner
import workflows.recipe
from gemmi import cif
from pydantic import BaseModel, Field
from pydantic.error_wrappers import ValidationError
from workflows.services.common_service import CommonService


class SelectClassesParameters(BaseModel):
    input_file: str = Field(..., min_length=1)
    combine_star_job_number: int
    particles_file: str = "particles.star"
    classes_file: str = "class_averages.star"
    python_exe: str = "/dls_sw/apps/EM/relion/4.0/conda/bin/python"
    min_score: float = 0
    min_particles: int = 500
    class3d_batch_size: int = 50000
    class3d_max_size: int = 200000
    mc_uuid: int
    relion_it_options: Optional[dict] = None


class SelectClasses(CommonService):
    """
    A service for running Relion autoselection on 2D classes
    """

    # Human readable service name
    _service_name = "SelectClasses"

    # Logger name
    _logger_name = "relion.zocalo.select.classes"

    # Values to extract for ISPyB
    previous_total_count = 0
    total_count = 0

    def initializing(self):
        """Subscribe to a queue. Received messages must be acknowledged."""
        self.log.info("Select particles service starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "select.classes",
            self.select_classes,
            acknowledgement=True,
            log_extender=self.extend_log,
            allow_non_recipe_messages=True,
        )

    def parse_autoselect_output(self, line: str):
        """
        Read the output logs of relion class selection
        """
        if not line:
            return

    def parse_combiner_output(self, line: str):
        """
        Read the output logs of the star file combination
        """
        if not line:
            return

        if line.startswith("Adding") and "particles_all.star" in line:
            line_split = line.split()
            self.previous_total_count = int(line_split[3])

        if line.startswith("Split"):
            line_split = line.split()
            self.total_count = int(line_split[1])

    def select_classes(self, rw, header: dict, message: dict):
        class MockRW:
            def dummy(self, *args, **kwargs):
                pass

        if not rw:
            print(
                "Incoming message is not a recipe message. Simple messages can be valid"
            )
            if (
                not isinstance(message, dict)
                or not message.get("parameters")
                or not message.get("content")
            ):
                self.log.error("Rejected invalid simple message")
                self._transport.nack(header)
                return
            self.log.debug("Received a simple message")

            # Create a wrapper-like object that can be passed to functions
            # as if a recipe wrapper was present.
            rw = MockRW()
            rw.transport = self._transport
            rw.recipe_step = {"parameters": message["parameters"]}
            rw.environment = {"has_recipe_wrapper": False}
            rw.set_default_channel = rw.dummy
            rw.send = rw.dummy
            message = message["content"]

        try:
            if isinstance(message, dict):
                autoselect_params = SelectClassesParameters(
                    **{**rw.recipe_step.get("parameters", {}), **message}
                )
            else:
                autoselect_params = SelectClassesParameters(
                    **{**rw.recipe_step.get("parameters", {})}
                )
        except (ValidationError, TypeError):
            self.log.warning(
                f"Selection parameter validation failed for message: {message} "
                f"and recipe parameters: {rw.recipe_step.get('parameters', {})}"
            )
            rw.transport.nack(header)
            return

        self.log.info(f"Inputs: {autoselect_params.input_file}")

        class2d_job_dir = Path(
            re.search(".+/job[0-9]{3}/", autoselect_params.input_file)[0]
        )
        project_dir = class2d_job_dir.parent.parent

        select_job_num = (
            int(re.search("/job[0-9]{3}", str(class2d_job_dir))[0][4:7]) + 1
        )
        select_dir = project_dir / f"Select/job{select_job_num:03}"
        select_dir.mkdir(parents=True, exist_ok=True)

        autoselect_flags = {
            "particles_file": "--fn_sel_parts",
            "classes_file": "--fn_sel_classavgs",
            "python_exe": "--python",
            "min_particles": "--select_min_nr_particles",
        }
        # Create the class selection command
        autoselect_command = [
            "relion_class_ranker",
            "--opt",
            autoselect_params.input_file,
            "--o",
            f"{select_dir.relative_to(project_dir)}/",
            "--auto_select",
            "--fn_root",
            "rank",
            "--do_granularity_features",
        ]
        for k, v in autoselect_params.dict().items():
            if v and (k in autoselect_flags):
                if type(v) is tuple:
                    autoselect_command.extend(
                        (autoselect_flags[k], " ".join(str(_) for _ in v))
                    )
                else:
                    autoselect_command.extend((autoselect_flags[k], str(v)))
        autoselect_command.extend(
            ("--pipeline_control", f"{select_dir.relative_to(project_dir)}/")
        )

        if not autoselect_params.min_score:
            autoselect_command.extend(("--min_score", "0.0"))
        else:
            autoselect_command.extend(("--min_score", str(autoselect_params.min_score)))

        result = procrunner.run(
            command=autoselect_command,
            callback_stdout=self.parse_autoselect_output,
            working_directory=str(project_dir),
        )
        if result.returncode:
            self.log.error(
                f"2D autoselection failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            rw.transport.nack(header)
            return

        if not autoselect_params.min_score:
            # If a minimum score isn't given, then work it out and rerun the job
            star_doc = cif.read_file(str(select_dir / "rank_model.star"))
            star_block = star_doc["model_classes"]
            class_scores = np.array(star_block.find_loop("_rlnClassScore"), dtype=float)
            quantile_threshold = np.quantile(
                class_scores,
                float(
                    autoselect_params.relion_it_options[
                        "class2d_fraction_of_classes_to_remove"
                    ]
                ),
            )

            self.log.info(
                f"Re-running class selection with new threshold {quantile_threshold}"
            )
            autoselect_command[-1] = str(quantile_threshold)
            result = procrunner.run(
                command=autoselect_command,
                callback_stdout=self.parse_autoselect_output,
                working_directory=str(project_dir),
            )
            if result.returncode:
                self.log.error(
                    f"2D autoselection failed with exitcode {result.returncode}:\n"
                    + result.stderr.decode("utf8", "replace")
                )
                rw.transport.nack(header)
                return

        # Send to node creator
        autoselect_node_creator_params = {
            "job_type": "relion.select.class2dauto",
            "input_file": autoselect_params.input_file,
            "output_file": str(select_dir / autoselect_params.particles_file),
            "relion_it_options": autoselect_params.relion_it_options,
        }
        if isinstance(rw, MockRW):
            rw.transport.send(
                destination="spa.node_creator",
                message={
                    "parameters": autoselect_node_creator_params,
                    "content": "dummy",
                },
            )
        else:
            rw.send_to("spa.node_creator", autoselect_node_creator_params)

        # Run the combine star files job
        combine_star_command = [
            "combine_star_files.py",
            str(select_dir / autoselect_params.particles_file),
        ]

        combine_star_dir = Path(
            project_dir / f"Select/job{autoselect_params.combine_star_job_number:03}"
        )
        if (combine_star_dir / "particles_all.star").exists():
            combine_star_command.append(str(combine_star_dir / "particles_all.star"))
        else:
            combine_star_dir.mkdir(parents=True, exist_ok=True)
            self.previous_total_count = 0
        combine_star_command.extend(
            (
                "--output_dir",
                str(combine_star_dir),
                "--split",
                "--split_size",
                str(autoselect_params.class3d_batch_size),
            )
        )

        result = procrunner.run(
            command=combine_star_command,
            callback_stdout=self.parse_combiner_output,
            working_directory=str(project_dir),
        )
        if result.returncode:
            self.log.error(
                f"Star file combination failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            rw.transport.nack(header)
            return

        # Send to node creator
        combine_node_creator_params = {
            "job_type": "combine_star_files_job",
            "input_file": str(select_dir / autoselect_params.particles_file),
            "output_file": str(combine_star_dir / "particles_all.star"),
            "relion_it_options": autoselect_params.relion_it_options,
        }
        if isinstance(rw, MockRW):
            rw.transport.send(
                destination="spa.node_creator",
                message={"parameters": combine_node_creator_params, "content": "dummy"},
            )
        else:
            rw.send_to("spa.node_creator", combine_node_creator_params)

        # Create 3D classification jobs
        if (
            self.total_count // autoselect_params.class3d_batch_size
            > self.previous_total_count // autoselect_params.class3d_batch_size
        ) and (self.total_count <= autoselect_params.class3d_max_size):
            # Only send to 3D if a new multiple of the batch threshold is crossed
            # and the count does not exceed the maximum
            self.log.info("Sending to murfey for Class3D")
            class3d_params = {}
            murfey_params = {
                "register": "run_class3d",
                "class3d": class3d_params,
            }
            if isinstance(rw, MockRW):
                rw.transport.send("murfey_feedback", murfey_params)
            else:
                rw.send_to("murfey_feedback", murfey_params)

        rw.transport.ack(header)

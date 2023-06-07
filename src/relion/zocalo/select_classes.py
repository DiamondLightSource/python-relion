from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import procrunner
import workflows.recipe
from pydantic import BaseModel, Field
from pydantic.error_wrappers import ValidationError
from workflows.services.common_service import CommonService


class SelectClassesParameters(BaseModel):
    input_file: str = Field(..., min_length=1)
    particles_file: str = "particles.star"
    classes_file: str = "class_averages.star"
    python_exe: str = "/dls_sw/apps/EM/relion/4.0/conda/bin/python"
    min_score: float = 0
    min_particles: int = 500
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
    particle_count = 0

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

        if "selected particles to" in line:
            line_split = line.split()
            self.particle_count = int(line_split[1])

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
            "min_score": "--min_score",
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
        node_creator_params = {
            "job_type": "relion.select.class2dauto",
            "input_file": autoselect_params.input_file,
            "output_file": str(select_dir / autoselect_params.particles_file),
            "relion_it_options": autoselect_params.relion_it_options,
        }
        if isinstance(rw, MockRW):
            rw.transport.send(
                destination="spa.node_creator",
                message={"parameters": node_creator_params, "content": "dummy"},
            )
        else:
            rw.send_to("spa.node_creator", node_creator_params)

        rw.transport.ack(header)

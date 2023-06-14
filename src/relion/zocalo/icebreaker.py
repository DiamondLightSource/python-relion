from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Literal, Optional

import procrunner
import workflows.recipe
from pydantic import BaseModel, Field
from pydantic.error_wrappers import ValidationError
from workflows.services.common_service import CommonService


class IceBreakerParameters(BaseModel):
    input_micrographs: str = Field(..., min_length=1)
    input_particles: Optional[str] = None
    output_path: str = Field(..., min_length=1)
    icebreaker_type: str = Literal[
        "micrographs", "enhancecontrast", "summary", "particles"
    ]

    cpus: int = 1
    total_motion: float = 0
    relion_it_options: Optional[dict] = None


class IceBreaker(CommonService):
    """
    A service that runs the IceBreaker micrographs job
    """

    # Human readable service name
    _service_name = "IceBreaker"

    # Logger name
    _logger_name = "relion.zocalo.icebreaker"

    # Job name
    job_type = "icebreaker.micrograph_analysis"

    # Values to extract for ISPyB
    number_of_particles: int

    def initializing(self):
        """Subscribe to a queue. Received messages must be acknowledged."""
        self.log.info("IceBreaker service starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "icebreaker",
            self.icebreaker,
            acknowledgement=True,
            log_extender=self.extend_log,
            allow_non_recipe_messages=True,
        )

    def parse_icebreaker_output(self, line: str):
        if not line:
            return

    def icebreaker(self, rw, header: dict, message: dict):
        """
        Main function which interprets received messages, runs icebreaker
        and sends messages to the ispyb and image services
        """

        class MockRW:
            def dummy(self, *args, **kwargs):
                pass

        if not rw:
            if (
                not isinstance(message, dict)
                or not message.get("parameters")
                or not message.get("content")
            ):
                self.log.error("Rejected invalid simple message")
                self._transport.nack(header)
                return

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
                icebreaker_params = IceBreakerParameters(
                    **{**rw.recipe_step.get("parameters", {}), **message}
                )
            else:
                icebreaker_params = IceBreakerParameters(
                    **{**rw.recipe_step.get("parameters", {})}
                )
        except (ValidationError, TypeError):
            self.log.warning(
                f"IceBreaker parameter validation failed for message: {message} "
                + "and recipe parameters: "
                + f"{rw.recipe_step.get('parameters', {})}"
            )
            rw.transport.nack(header)
            return

        # IceBreaker requires running in the project directory
        project_dir = Path(icebreaker_params.output_path).parent.parent
        os.chdir(project_dir)
        if not Path(icebreaker_params.output_path).exists():
            Path(icebreaker_params.output_path).mkdir(parents=True)
        mic_from_project = Path(icebreaker_params.input_micrographs).relative_to(
            project_dir
        )

        self.log.info(
            f"Type: {icebreaker_params.icebreaker_type} "
            f"Input: {icebreaker_params.input_micrographs} "
            f"Output: {icebreaker_params.output_path}"
        )
        this_job_type = f"{self.job_type}.{icebreaker_params.icebreaker_type}"

        # Create commands depending on the icebreaker types
        if icebreaker_params.icebreaker_type == "micrographs":
            command = [
                "ib_job",
                "--j",
                str(icebreaker_params.cpus),
                "--mode",
                "group",
                "--single_mic",
                str(mic_from_project),
                "--o",
                icebreaker_params.output_path,
            ]
        elif icebreaker_params.icebreaker_type == "enhancecontrast":
            command = [
                "ib_job",
                "--j",
                str(icebreaker_params.cpus),
                "--mode",
                "flatten",
                "--single_mic",
                str(mic_from_project),
                "--o",
                icebreaker_params.output_path,
            ]
        elif icebreaker_params.icebreaker_type == "summary":
            command = [
                "ib_5fig",
                "--single_mic",
                str(mic_from_project),
                "--o",
                icebreaker_params.output_path,
            ]
        else:  # icebreaker_params.icebreaker_type == "particles":
            command = [
                "ib_group",
                "--in_mics",
                str(mic_from_project),
                "--in_parts",
                str(Path(icebreaker_params.input_particles).relative_to(project_dir)),
                "--o",
                icebreaker_params.output_path,
            ]
        with open(Path(icebreaker_params.output_path) / "note.txt", "w") as f:
            f.write(" ".join(command))

        # Run the icebreaker command
        result = procrunner.run(
            command=command, callback_stdout=self.parse_icebreaker_output
        )
        if result.returncode:
            self.log.error(
                f"IceBreaker failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            rw.transport.nack(header)
            return

        # Forward results to next IceBreaker job
        if icebreaker_params.icebreaker_type == "micrographs":
            self.log.info("Sending to IceBreaker summary")
            next_icebreaker_params = {
                "icebreaker_type": "summary",
                "input_micrographs": str(
                    Path(
                        re.sub(
                            ".+/job[0-9]{3}/",
                            icebreaker_params.output_path,
                            str(mic_from_project),
                        )
                    ).parent
                    / mic_from_project.stem
                )
                + "_grouped.mrc",
                "relion_it_options": icebreaker_params.relion_it_options,
            }
            job_number = int(
                re.search("/job[0-9]{3}/", icebreaker_params.output_path)[0][4:7]
            )
            next_icebreaker_params["output_path"] = re.sub(
                f"IceBreaker/job{job_number:03}/",
                f"IceBreaker/job{job_number + 2:03}/",
                icebreaker_params.output_path,
            )
            if isinstance(rw, MockRW):
                rw.transport.send(
                    destination="icebreaker",
                    message={"parameters": next_icebreaker_params, "content": "dummy"},
                )
            else:
                rw.send_to("icebreaker", next_icebreaker_params)

        # Register the icebreaker job with the node creator
        self.log.info(f"Sending {this_job_type} to node creator")
        node_creator_parameters = {
            "job_type": this_job_type,
            "input_file": icebreaker_params.input_micrographs,
            "output_file": icebreaker_params.output_path,
            "relion_it_options": icebreaker_params.relion_it_options,
            "results": {
                "icebreaker_type": icebreaker_params.icebreaker_type,
                "total_motion": str(icebreaker_params.total_motion),
            },
        }
        if icebreaker_params.icebreaker_type == "particles":
            node_creator_parameters[
                "input_file"
            ] += f":{icebreaker_params.input_particles}"
        if isinstance(rw, MockRW):
            rw.transport.send(
                destination="spa.node_creator",
                message={"parameters": node_creator_parameters, "content": "dummy"},
            )
        else:
            rw.send_to("spa.node_creator", node_creator_parameters)

        self.log.info(
            f"Done {this_job_type} for {icebreaker_params.input_micrographs}."
        )
        rw.transport.ack(header)

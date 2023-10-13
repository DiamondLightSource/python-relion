from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Literal, Optional

import workflows.recipe
from pydantic import BaseModel, Field, ValidationError
from workflows.services.common_service import CommonService

from relion.cryolo_relion_it import icebreaker_histogram
from relion.zocalo.spa_relion_service_options import RelionServiceOptions


class IceBreakerParameters(BaseModel):
    input_micrographs: str = Field(..., min_length=1)
    input_particles: Optional[str] = None
    output_path: str = Field(..., min_length=1)
    icebreaker_type: str = Literal[
        "micrographs", "enhancecontrast", "summary", "particles"
    ]
    cpus: int = 1
    total_motion: float = 0
    early_motion: float = 0
    late_motion: float = 0
    mc_uuid: int
    relion_options: RelionServiceOptions


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
    ice_minimum: int
    ice_q1: int
    ice_median: int
    ice_q3: int
    ice_maximum: int

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

    def parse_icebreaker_output(self, icebreaker_stdout: str):
        """
        Read the output logs of IceBreaker summary jobs to extract values for ispyb
        """
        for line in icebreaker_stdout.split("\n"):
            if line.startswith("Results:"):
                try:
                    line_split = line.split()
                    self.ice_minimum = int(line_split[2])
                    self.ice_q1 = int(line_split[3])
                    self.ice_median = int(line_split[4])
                    self.ice_q3 = int(line_split[5])
                    self.ice_maximum = int(line_split[6])
                except IndexError:
                    self.log.error(f"Failed to read line {line} in {icebreaker_stdout}")

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
        except (ValidationError, TypeError) as e:
            self.log.warning(
                f"IceBreaker parameter validation failed for message: {message} "
                f"and recipe parameters: {rw.recipe_step.get('parameters', {})} "
                f"with exception: {e}"
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

        # Check for existing temporary directory
        job_dir = Path(re.search(".+/job[0-9]{3}/", icebreaker_params.output_path)[0])
        icebreaker_tmp_dir = job_dir / f"IB_input_{mic_from_project.stem}"
        if icebreaker_tmp_dir.is_dir():
            self.log.warning(
                f"Directory {icebreaker_tmp_dir} already exists - now removing it"
            )
            shutil.rmtree(icebreaker_tmp_dir)

        # Run the icebreaker command and confirm it ran successfully
        result = subprocess.run(command, capture_output=True)

        # Register the icebreaker job with the node creator
        self.log.info(f"Sending {this_job_type} to node creator")
        node_creator_parameters = {
            "job_type": this_job_type,
            "input_file": icebreaker_params.input_micrographs,
            "output_file": icebreaker_params.output_path,
            "relion_options": dict(icebreaker_params.relion_options),
            "command": " ".join(command),
            "stdout": result.stdout.decode("utf8", "replace"),
            "stderr": result.stderr.decode("utf8", "replace"),
            "results": {
                "icebreaker_type": icebreaker_params.icebreaker_type,
                "total_motion": icebreaker_params.total_motion,
                "early_motion": icebreaker_params.early_motion,
                "late_motion": icebreaker_params.late_motion,
            },
        }
        if result.returncode:
            node_creator_parameters["success"] = False
        else:
            node_creator_parameters["success"] = True
            if icebreaker_params.icebreaker_type == "summary":
                # Summary jobs need to read results and send them to node creation
                self.parse_icebreaker_output(result.stdout.decode("utf8", "replace"))
                node_creator_parameters["results"]["summary"] = [
                    str(self.ice_minimum),
                    str(self.ice_q1),
                    str(self.ice_median),
                    str(self.ice_q3),
                    str(self.ice_maximum),
                ]
        if icebreaker_params.icebreaker_type == "particles":
            node_creator_parameters[
                "input_file"
            ] += f":{icebreaker_params.input_particles}"
        if isinstance(rw, MockRW):
            rw.transport.send(
                destination="node_creator",
                message={"parameters": node_creator_parameters, "content": "dummy"},
            )
        else:
            rw.send_to("node_creator", node_creator_parameters)

        # End here if the command failed
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
                "relion_options": dict(icebreaker_params.relion_options),
                "mc_uuid": icebreaker_params.mc_uuid,
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

        # Send results to ispyb
        if icebreaker_params.icebreaker_type == "summary":
            ispyb_parameters = {
                "ispyb_command": "buffer",
                "buffer_lookup": {"motion_correction_id": icebreaker_params.mc_uuid},
                "buffer_command": {"ispyb_command": "insert_relative_ice_thickness"},
                "minimum": self.ice_minimum,
                "q1": self.ice_q1,
                "median": self.ice_median,
                "q3": self.ice_q3,
                "maximum": self.ice_maximum,
            }
            self.log.info(f"Sending to ispyb: {ispyb_parameters}")
            if isinstance(rw, MockRW):
                rw.transport.send(
                    destination="ispyb_connector",
                    message={"parameters": ispyb_parameters, "content": "dummy"},
                )
            else:
                rw.send_to("ispyb_connector", ispyb_parameters)

        # Create histograms and send to ispyb for the particle grouping jobs
        if (
            icebreaker_params.icebreaker_type == "particles"
            and Path(icebreaker_params.input_particles).name == "particles_split1.star"
        ):
            Path(project_dir / "IceBreaker/Icebreaker_group_batch_1").unlink(
                missing_ok=True
            )
            Path(project_dir / "IceBreaker/Icebreaker_group_batch_1").symlink_to(
                icebreaker_params.output_path
            )
            try:
                pdf_file_path = icebreaker_histogram.create_pdf_histogram(
                    project_dir,
                    version=4,
                )
                json_file_path = icebreaker_histogram.create_json_histogram(
                    project_dir,
                    version=4,
                )
                if pdf_file_path and json_file_path:
                    attachment_list = [
                        {
                            "ispyb_command": "add_program_attachment",
                            "file_name": f"{Path(json_file_path.name)}",
                            "file_path": f"{Path(json_file_path.parent)}",
                            "file_type": "Graph",
                        },
                        {
                            "ispyb_command": "add_program_attachment",
                            "file_name": f"{Path(pdf_file_path.name)}",
                            "file_path": f"{Path(pdf_file_path.parent)}",
                            "file_type": "Graph",
                        },
                    ]
                    self.log.info(f"Sending ISPyB attachments {attachment_list}")
                    if isinstance(rw, MockRW):
                        rw.transport.send(
                            destination="ispyb_connector",
                            message={
                                "parameters": {
                                    "ispyb_command": "multipart_message",
                                    "ispyb_command_list": attachment_list,
                                },
                                "content": "dummy",
                            },
                        )
                    else:
                        rw.send_to(
                            "ispyb_connector",
                            {
                                "ispyb_command": "multipart_message",
                                "ispyb_command_list": attachment_list,
                            },
                        )
            except (FileNotFoundError, OSError, RuntimeError, ValueError):
                self.log.warning("Error creating Icebreaker histogram.")

        self.log.info(
            f"Done {this_job_type} for {icebreaker_params.input_micrographs}."
        )
        rw.transport.ack(header)

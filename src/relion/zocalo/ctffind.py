from __future__ import annotations

import re
from pathlib import Path
from typing import Literal, Optional

import procrunner
import workflows.recipe
from pydantic import BaseModel, Field
from pydantic.error_wrappers import ValidationError
from workflows.services.common_service import CommonService


class CTFParameters(BaseModel):
    collection_type: str = Literal["spa", "tomography"]
    pix_size: float
    voltage: float = 300.0
    spher_aber: float = 2.7
    ampl_contrast: float = 0.8
    ampl_spectrum: int = 512
    min_res: float = 30.0
    max_res: float = 5.0
    min_defocus: float = 5000.0
    max_defocus: float = 50000.0
    defocus_step: float = 100.0
    astigmatism_known: str = "no"
    slow_search: str = "no"
    astigmatism_restrain: str = "no"
    additional_phase_shift: str = "no"
    expert_options: str = "no"
    input_image: str = Field(..., min_length=1)
    output_image: str = Field(..., min_length=1)
    mc_uuid: int
    relion_it_options: Optional[dict] = None
    autopick: dict = {}


class CTFFind(CommonService):
    """
    A service for CTF estimating micrographs with CTFFind
    """

    # Human readable service name
    _service_name = "CTFFind"

    # Logger name
    _logger_name = "relion.zocalo.ctffind"

    # Values to extract for ISPyB
    astigmatism_angle: float
    cc_value: float
    estimated_resolution: float
    defocus1: float
    defocus2: float

    def initializing(self):
        """Subscribe to a queue. Received messages must be acknowledged."""
        self.log.info("CTFFind service starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "ctffind",
            self.ctf_find,
            acknowledgement=True,
            log_extender=self.extend_log,
            allow_non_recipe_messages=True,
        )

    def parse_ctf_output(self, line: str):
        if not line:
            return

        try:
            if line.startswith("Estimated defocus values"):
                line_split = line.split()
                self.defocus1 = float(line_split[4])
                self.defocus2 = float(line_split[6])
            if line.startswith("Estimated azimuth"):
                line_split = line.split()
                self.astigmatism_angle = float(line_split[4])
            if line.startswith("Score"):
                line_split = line.split()
                self.cc_value = float(line_split[2])
            if line.startswith("Thon rings"):
                line_split = line.split()
                self.estimated_resolution = float(line_split[8])
        except Exception as e:
            self.log.warning(f"{e}")

    def ctf_find(self, rw, header: dict, message: dict):
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

        command = ["ctffind"]

        try:
            if isinstance(message, dict):
                ctf_params = CTFParameters(
                    **{**rw.recipe_step.get("parameters", {}), **message}
                )
            else:
                ctf_params = CTFParameters(**{**rw.recipe_step.get("parameters", {})})
        except (ValidationError, TypeError):
            self.log.warning(
                f"CTF estimation parameter validation failed for message: {message} "
                f"and recipe parameters: {rw.recipe_step.get('parameters', {})}"
            )
            rw.transport.nack(header)
            return

        # Make sure the output directory exists
        if not Path(ctf_params.output_image).parent.exists():
            Path(ctf_params.output_image).parent.mkdir(parents=True)

        parameters_list = [
            ctf_params.input_image,
            ctf_params.output_image,
            ctf_params.pix_size,
            ctf_params.voltage,
            ctf_params.spher_aber,
            ctf_params.ampl_contrast,
            ctf_params.ampl_spectrum,
            ctf_params.min_res,
            ctf_params.max_res,
            ctf_params.min_defocus,
            ctf_params.max_defocus,
            ctf_params.defocus_step,
            ctf_params.astigmatism_known,
            ctf_params.slow_search,
            ctf_params.astigmatism_restrain,
            ctf_params.additional_phase_shift,
            ctf_params.expert_options,
        ]

        parameters_string = "\n".join(map(str, parameters_list))
        self.log.info(
            f"Input: {ctf_params.input_image} Output: {ctf_params.output_image}"
        )

        result = procrunner.run(
            command=command,
            stdin=parameters_string.encode("ascii"),
            callback_stdout=self.parse_ctf_output,
        )
        if result.returncode:
            self.log.error(
                f"CTFFind failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            rw.transport.nack(header)
            return

        # Write stdout to logfile
        with open(
            str(Path(ctf_params.output_image).with_suffix("")) + "_ctffind4.log", "w"
        ) as f:
            f.write(result.stdout.decode("utf8", "replace"))

        # Extract results for ispyb
        astigmatism = self.defocus2 - self.defocus1
        estimated_defocus = (self.defocus1 + self.defocus2) / 2

        ispyb_parameters = {
            "box_size_x": str(ctf_params.ampl_spectrum),
            "box_size_y": str(ctf_params.ampl_spectrum),
            "min_resolution": str(ctf_params.min_res),
            "max_resolution": str(ctf_params.max_res),
            "min_defocus": str(ctf_params.min_defocus),
            "max_defocus": str(ctf_params.max_defocus),
            "astigmatism": str(astigmatism),
            "defocus_step_size": str(ctf_params.defocus_step),
            "astigmatism_angle": str(self.astigmatism_angle),
            "estimated_resolution": str(self.estimated_resolution),
            "estimated_defocus": str(estimated_defocus),
            "amplitude_contrast": str(ctf_params.ampl_contrast),
            "cc_value": str(self.cc_value),
            "fft_theoretical_full_path": str(
                Path(ctf_params.output_image).with_suffix(".jpeg")
            ),  # path to output mrc (would be jpeg if we could convert in SW)
        }

        # Forward results to ispyb
        ispyb_parameters.update(
            {
                "ispyb_command": "buffer",
                "buffer_lookup": {"motion_correction_id": ctf_params.mc_uuid},
                "buffer_command": {"ispyb_command": "insert_ctf"},
            }
        )
        self.log.info(f"Sending to ispyb {ispyb_parameters}")
        if isinstance(rw, MockRW):
            rw.transport.send(
                destination="ispyb_connector",
                message={
                    "parameters": ispyb_parameters,
                    "content": {"dummy": "dummy"},
                },
            )
        else:
            rw.send_to("ispyb", ispyb_parameters)

        # Forward results to images service
        self.log.info(f"Sending to images service {ctf_params.output_image}")
        if isinstance(rw, MockRW):
            rw.transport.send(
                destination="images",
                message={
                    "parameters": {"images_command": "mrc_to_jpeg"},
                    "file": ctf_params.output_image,
                },
            )
        else:
            rw.send_to(
                "images",
                {
                    "parameters": {"images_command": "mrc_to_jpeg"},
                    "file": ctf_params.output_image,
                },
            )

        # If this is SPA, send the results to be processed by the node creator
        if ctf_params.collection_type.lower() == "spa":
            # Register the ctf job with the node creator
            node_creator_parameters = {
                "job_type": "relion.ctffind.ctffind4",
                "input_file": ctf_params.input_image,
                "output_file": ctf_params.output_image,
                "relion_it_options": ctf_params.relion_it_options,
            }
            if isinstance(rw, MockRW):
                rw.transport.send(
                    destination="spa.node_creator",
                    message={"parameters": node_creator_parameters, "content": "dummy"},
                )
            else:
                rw.send_to("spa.node_creator", node_creator_parameters)

        # If this is SPA, also set up a cryolo job
        if ctf_params.collection_type.lower() == "spa":
            # Forward results to particle picking
            self.log.info(f"Sending to autopicking: {ctf_params.input_image}")
            ctf_params.autopick["input_path"] = ctf_params.input_image
            ctf_job_number = int(
                re.search("/job[0-9]{3}/", ctf_params.output_image)[0][4:7]
            )
            ctf_params.autopick["output_path"] = str(
                Path(
                    re.sub(
                        "MotionCorr/job002/.+",
                        f"AutoPick/job{ctf_job_number + 1:03}/STAR/",
                        ctf_params.input_image,
                    )
                )
                / Path(ctf_params.input_image).with_suffix(".star").name
            )
            ctf_params.autopick["ctf_values"] = {
                "file": ctf_params.output_image,
                "CtfMaxResolution": self.estimated_resolution,
                "CtfFigureOfMerit": self.cc_value,
                "DefocusU": self.defocus1,
                "DefocusV": self.defocus2,
                "DefocusAngle": self.astigmatism_angle,
            }
            ctf_params.autopick["relion_it_options"] = ctf_params.relion_it_options
            ctf_params.autopick["mc_uuid"] = ctf_params.mc_uuid
            ctf_params.autopick["pix_size"] = ctf_params.pix_size
            ctf_params.autopick["threshold"] = ctf_params.relion_it_options[
                "cryolo_threshold"
            ]
            if isinstance(rw, MockRW):
                rw.transport.send(
                    destination="cryolo",
                    message={"parameters": ctf_params.autopick, "content": "dummy"},
                )
            else:
                rw.send_to("cryolo", ctf_params.autopick)

        rw.transport.ack(header)

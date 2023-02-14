from __future__ import annotations

import json
from pathlib import Path

import procrunner
import workflows.recipe
from pydantic import BaseModel, Field
from pydantic.error_wrappers import ValidationError
from workflows.services.common_service import CommonService


class CryoloParameters(BaseModel):
    boxsize: int
    pix_size: float
    input_path: str = Field(..., min_length=1)
    output_path: str = Field(..., min_length=1)
    config_file: str = "/dls_sw/apps/EM/crYOLO/phosaurus_models/config.json"
    weights: str = (
        "/dls_sw/apps/EM/crYOLO/phosaurus_models/gmodel_phosnet_202005_N63_c17.h5"
    )
    threshold: float = 0.3
    mc_uuid: int
    cryolo_command: str


class CrYOLO(CommonService):
    """
    A service that runs crYOLO particle picking
    """

    # Human readable service name
    _service_name = "CrYOLO"

    # Logger name
    _logger_name = "relion.zocalo.cryolo"

    # Values to extract for ISPyB
    number_of_particles: int

    def initializing(self):
        """Subscribe to a queue. Received messages must be acknowledged."""
        self.log.info("crYOLO service starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "cryolo",
            self.cryolo,
            acknowledgement=True,
            log_extender=self.extend_log,
            allow_non_recipe_messages=True,
        )

    def parse_cryolo_output(self, line: str):
        """
        Read the output logs of cryolo to determine
        the number of particles that are picked
        """
        if not line:
            return

        if "particles in total has been found" in line:
            line_split = line.split()
            self.number_of_particles += int(line_split[0])

        if line.startswith("Deleted"):
            line_split = line.split()
            self.number_of_particles -= int(line_split[1])

    def cryolo(self, rw, header: dict, message: dict):
        """
        Main function which interprets received messages, runs cryolo
        and sends messages to the ispyb and image services
        """

        class RW_mock:
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
            rw = RW_mock()
            rw.transport = self._transport
            rw.recipe_step = {"parameters": message["parameters"]}
            rw.environment = {"has_recipe_wrapper": False}
            rw.set_default_channel = rw.dummy
            rw.send = rw.dummy
            message = message["content"]

        # Reset number of particles
        self.number_of_particles = 0

        try:
            if isinstance(message, dict):
                cryolo_params = CryoloParameters(
                    **{**rw.recipe_step.get("parameters", {}), **message}
                )
            else:
                cryolo_params = CryoloParameters(
                    **{**rw.recipe_step.get("parameters", {})}
                )
        except (ValidationError, TypeError):
            self.log.warning(
                f"crYOLO parameter validation failed for message: {message} "
                + "and recipe parameters: "
                + f"{rw.recipe_step.get('parameters', {})}"
            )
            rw.transport.nack(header)
            return

        # Make a cryolo config file with the correct box size
        with open(cryolo_params.config_file, "r") as json_file:
            data = json.load(json_file)
            data["model"]["anchors"] = [cryolo_params.boxsize, cryolo_params.boxsize]
        with open("config.json", "w") as outfile:
            json.dump(data, outfile)

        # Construct a command to run cryolo with the given parameters
        command = cryolo_params.cryolo_command.split()
        command.extend((["--conf", "config.json"]))

        cryolo_flags = {
            "weights": "--weights",
            "input_path": "-i",
            "output_path": "-o",
            "threshold": "--threshold",
            "cryolo_gpus": "--gpu",
        }

        for k, v in cryolo_params.dict().items():
            if v and (k in cryolo_flags):
                if type(v) is tuple:
                    command.extend((cryolo_flags[k], " ".join(str(_) for _ in v)))
                else:
                    command.extend((cryolo_flags[k], str(v)))

        self.log.info(
            f"Input: {cryolo_params.input_path} "
            + f"Output: {cryolo_params.output_path}"
        )

        # Run cryolo and confirm it ran successfully
        result = procrunner.run(
            command=command, callback_stdout=self.parse_cryolo_output
        )
        if result.returncode:
            self.log.error(
                f"crYOLO failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            rw.transport.nack(header)
            return

        # Extract results for ispyb
        ispyb_parameters = {
            "particle_picking_template": cryolo_params.weights,
            "particle_diameter": cryolo_params.pix_size * cryolo_params.boxsize / 10,
            "number_of_particles": self.number_of_particles,
            "summary_image_full_path": cryolo_params.output_path
            + "/picked_particles.mrc",
        }

        # Forward results to ISPyB
        ispyb_parameters.update(
            {
                "ispyb_command": "buffer",
                "buffer_lookup": {"motion_correction_id": cryolo_params.mc_uuid},
                "buffer_command": {"ispyb_command": "insert_particle_picker"},
            }
        )
        self.log.info(f"Sending to ispyb {ispyb_parameters}")
        if isinstance(rw, RW_mock):
            rw.transport.send(
                destination="ispyb_connector",
                message={
                    "parameters": ispyb_parameters,
                    "content": {"dummy": "dummy"},
                },
            )
        else:
            rw.send_to("ispyb", ispyb_parameters)

        # Extract results for images service
        with open(
            Path(cryolo_params.output_path + "/STAR/")
            / Path(cryolo_params.input_path).with_suffix(".star").name,
            "r",
        ) as coords_file:
            coords = [line.split() for line in coords_file][6:]
            coords_file.close()

        # Forward results to images service
        self.log.info("Sending to images service")
        if isinstance(rw, RW_mock):
            rw.transport.send(
                destination="images",
                message={
                    "parameters": {"images_command": "picked_particles"},
                    "file": cryolo_params.input_path,
                    "coordinates": coords,
                    "angpix": cryolo_params.pix_size,
                    "diameter": cryolo_params.pix_size * cryolo_params.boxsize,
                    "outfile": cryolo_params.output_path + "/picked_particles.jpeg",
                },
            )
        else:
            rw.send_to(
                "images",
                {
                    "parameters": {"images_command": "picked_particles"},
                    "file": cryolo_params.input_path,
                    "coordinates": coords,
                    "angpix": cryolo_params.pix_size,
                    "diameter": cryolo_params.pix_size * cryolo_params.boxsize,
                    "outfile": cryolo_params.output_path + "/picked_particles.jpeg",
                },
            )

        rw.transport.ack(header)

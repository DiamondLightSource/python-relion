from __future__ import annotations

import re
import subprocess
from pathlib import Path

import numpy as np
import workflows.recipe
from gemmi import cif
from pydantic import BaseModel, Field, ValidationError
from workflows.services.common_service import CommonService

from relion.zocalo.spa_relion_service_options import RelionServiceOptions


class CryoloParameters(BaseModel):
    input_path: str = Field(..., min_length=1)
    output_path: str = Field(..., min_length=1)
    pix_size: float
    config_file: str = "/dls_sw/apps/EM/crYOLO/phosaurus_models/config.json"
    weights: str = (
        "/dls_sw/apps/EM/crYOLO/phosaurus_models/gmodel_phosnet_202005_N63_c17.h5"
    )
    threshold: float = 0.3
    cryolo_command: str = "cryolo_predict.py"
    particle_diameter: float = None
    mc_uuid: int
    picker_uuid: int
    program_id: int
    session_id: int
    relion_options: RelionServiceOptions
    ctf_values: dict = {}


class CrYOLO(CommonService):
    """
    A service that runs crYOLO particle picking
    """

    # Human readable service name
    _service_name = "CrYOLO"

    # Logger name
    _logger_name = "relion.zocalo.cryolo"

    # Job name
    job_type = "cryolo.autopick"

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

    def parse_cryolo_output(self, cryolo_stdout: str):
        """
        Read the output logs of cryolo to determine
        the number of particles that are picked
        """
        for line in cryolo_stdout.split("\n"):
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
        except (ValidationError, TypeError) as e:
            self.log.warning(
                f"crYOLO parameter validation failed for message: {message} "
                f"and recipe parameters: {rw.recipe_step.get('parameters', {})} "
                f"with exception: {e}"
            )
            rw.transport.nack(header)
            return

        # CrYOLO requires running in the project directory or job directory
        job_dir = Path(re.search(".+/job[0-9]{3}/", cryolo_params.output_path)[0])
        job_dir.mkdir(parents=True, exist_ok=True)

        # Construct a command to run cryolo with the given parameters
        command = cryolo_params.cryolo_command.split()
        command.extend((["--conf", cryolo_params.config_file]))
        command.extend((["-o", str(job_dir)]))
        command.extend((["--otf"]))

        cryolo_flags = {
            "weights": "--weights",
            "input_path": "-i",
            "threshold": "--threshold",
            "cryolo_gpus": "--gpu",
        }

        for k, v in cryolo_params.dict().items():
            if v and (k in cryolo_flags):
                command.extend((cryolo_flags[k], str(v)))

        self.log.info(
            f"Input: {cryolo_params.input_path} "
            + f"Output: {cryolo_params.output_path}"
        )

        # Run cryolo
        result = subprocess.run(command, cwd=job_dir, capture_output=True)

        # Remove cryosparc file to minimise crashes
        (job_dir / "CRYOSPARC/cryosparc.star").unlink(missing_ok=True)

        # Read in the stdout from cryolo
        self.parse_cryolo_output(result.stdout.decode("utf8", "replace"))

        # Register the cryolo job with the node creator
        self.log.info(f"Sending {self.job_type} to node creator")
        node_creator_parameters = {
            "job_type": self.job_type,
            "input_file": cryolo_params.input_path,
            "output_file": cryolo_params.output_path,
            "relion_options": dict(cryolo_params.relion_options),
            "command": " ".join(command),
            "stdout": result.stdout.decode("utf8", "replace"),
            "stderr": result.stderr.decode("utf8", "replace"),
        }
        if (
            result.returncode
            and result.stderr.decode("utf8", "replace").split("\n")[-1]
            == "IndexError: list index out of range"
        ):
            # If Cryolo failed because there are no picks then consider it a success
            result.returncode = 0

        if result.returncode:
            node_creator_parameters["success"] = False
        else:
            node_creator_parameters["success"] = True
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
                f"crYOLO failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            rw.transport.nack(header)
            return

        # Find the diameters of the particles
        try:
            cbox_file = cif.read_file(
                str(
                    job_dir
                    / f"CBOX/{Path(cryolo_params.output_path).with_suffix('.cbox').name}"
                )
            )
            cbox_block = cbox_file.find_block("cryolo")
            cbox_sizes = np.append(
                np.array(cbox_block.find_loop("_EstWidth"), dtype=float),
                np.array(cbox_block.find_loop("_EstHeight"), dtype=float),
            )
            cbox_confidence = np.append(
                np.array(cbox_block.find_loop("_Confidence"), dtype=float),
                np.array(cbox_block.find_loop("_Confidence"), dtype=float),
            )
            cryolo_particle_sizes = cbox_sizes[
                cbox_confidence > cryolo_params.threshold
            ]
        except (FileNotFoundError, OSError, AttributeError):
            cryolo_particle_sizes = []

        # Forward results to ISPyB
        ispyb_parameters = {
            "ispyb_command": "buffer",
            "buffer_lookup": {"motion_correction_id": cryolo_params.mc_uuid},
            "buffer_command": {"ispyb_command": "insert_particle_picker"},
            "buffer_store": cryolo_params.picker_uuid,
            "particle_picking_template": cryolo_params.weights,
            "number_of_particles": self.number_of_particles,
            "summary_image_full_path": str(
                Path(cryolo_params.output_path).with_suffix(".jpeg")
            ),
        }
        if cryolo_params.particle_diameter:
            ispyb_parameters["particle_diameter"] = cryolo_params.particle_diameter
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
            rw.send_to("ispyb_connector", ispyb_parameters)

        # Extract results for images service
        try:
            with open(cryolo_params.output_path, "r") as coords_file:
                coords = [line.split() for line in coords_file][6:]
        except FileNotFoundError:
            coords = []

        # Forward results to images service
        self.log.info("Sending to images service")
        if isinstance(rw, MockRW):
            rw.transport.send(
                destination="images",
                message={
                    "image_command": "picked_particles",
                    "file": cryolo_params.input_path,
                    "coordinates": coords,
                    "angpix": cryolo_params.pix_size,
                    "diameter": cryolo_params.pix_size * 160,
                    "outfile": str(
                        Path(cryolo_params.output_path).with_suffix(".jpeg")
                    ),
                },
            )
        else:
            rw.send_to(
                "images",
                {
                    "image_command": "picked_particles",
                    "file": cryolo_params.input_path,
                    "coordinates": coords,
                    "angpix": cryolo_params.pix_size,
                    "diameter": cryolo_params.pix_size * 160,
                    "outfile": str(
                        Path(cryolo_params.output_path).with_suffix(".jpeg")
                    ),
                },
            )

        # Gather results needed for particle extraction
        extraction_params = {
            "ctf_values": cryolo_params.ctf_values,
            "micrographs_file": cryolo_params.input_path,
            "coord_list_file": cryolo_params.output_path,
        }
        job_number = int(re.search("/job[0-9]{3}/", cryolo_params.output_path)[0][4:7])
        extraction_params["extract_file"] = str(
            Path(
                re.sub(
                    "MotionCorr/job002/.+",
                    f"Extract/job{job_number + 1:03}/Movies/",
                    cryolo_params.input_path,
                )
            )
            / (Path(cryolo_params.input_path).stem + "_extract.star")
        )

        # Forward results to murfey
        self.log.info("Sending to Murfey for particle extraction")
        if isinstance(rw, MockRW):
            rw.transport.send(
                destination="murfey_feedback",
                message={
                    "register": "picked_particles",
                    "motion_correction_id": cryolo_params.mc_uuid,
                    "micrograph": cryolo_params.input_path,
                    "particle_diameters": list(cryolo_particle_sizes),
                    "extraction_parameters": extraction_params,
                    "program_id": cryolo_params.program_id,
                    "session_id": cryolo_params.session_id,
                },
            )
        else:
            rw.send_to(
                "murfey_feedback",
                {
                    "register": "picked_particles",
                    "motion_correction_id": cryolo_params.mc_uuid,
                    "micrograph": cryolo_params.input_path,
                    "particle_diameters": list(cryolo_particle_sizes),
                    "extraction_parameters": extraction_params,
                    "program_id": cryolo_params.program_id,
                    "session_id": cryolo_params.session_id,
                },
            )

        self.log.info(f"Done {self.job_type} for {cryolo_params.input_path}.")
        rw.transport.ack(header)

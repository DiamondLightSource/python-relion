from __future__ import annotations

from pathlib import Path
from typing import Optional

import mrcfile
import numpy as np
import workflows.recipe
from gemmi import cif
from pydantic import BaseModel, Field
from pydantic.error_wrappers import ValidationError
from workflows.services.common_service import CommonService


class ExtractParameters(BaseModel):
    pix_size: float
    micrographs_file: str = Field(..., min_length=1)
    ctf_file: str = Field(..., min_length=1)
    coord_list_file: str = Field(..., min_length=1)
    output_file: str = Field(..., min_length=1)
    extract_boxsize: int = 256
    norm: bool = True
    bg_radius: int = 96
    downscale: bool = False
    downscale_boxsize: int = 64
    invert_contrast: bool = True
    mc_uuid: int
    relion_it_options: Optional[dict] = None


class Extract(CommonService):
    """
    A service for CTF estimating micrographs with CTFFind
    """

    # Human readable service name
    _service_name = "Extract"

    # Logger name
    _logger_name = "relion.zocalo.extract"

    def initializing(self):
        """Subscribe to a queue. Received messages must be acknowledged."""
        self.log.info("Extract service starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "extract",
            self.extract,
            acknowledgement=True,
            log_extender=self.extend_log,
            allow_non_recipe_messages=True,
        )

    def extract(self, rw, header: dict, message: dict):
        class RW_mock:
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
            rw = RW_mock()
            rw.transport = self._transport
            rw.recipe_step = {"parameters": message["parameters"]}
            rw.environment = {"has_recipe_wrapper": False}
            rw.set_default_channel = rw.dummy
            rw.send = rw.dummy
            message = message["content"]

        try:
            if isinstance(message, dict):
                extract_params = ExtractParameters(
                    **{**rw.recipe_step.get("parameters", {}), **message}
                )
            else:
                extract_params = ExtractParameters(
                    **{**rw.recipe_step.get("parameters", {})}
                )
        except (ValidationError, TypeError):
            self.log.warning(
                f"Extraction parameter validation failed for message: {message} "
                f"and recipe parameters: {rw.recipe_step.get('parameters', {})}"
            )
            rw.transport.nack(header)
            return

        self.log.info(
            f"Inputs: {extract_params.micrographs_file}, "
            f"{extract_params.ctf_file}, {extract_params.coord_list_file} "
            f"Output: {extract_params.output_file}"
        )

        # Make sure the output directory exists
        if not Path(extract_params.output_file).parent.exists():
            Path(extract_params.output_file).parent.mkdir(parents=True)
        output_mrc_file = (
            Path(extract_params.output_file).parent
            / Path(extract_params.micrographs_file).with_suffix(".mrcs").name
        )

        # Find the locations of the particles
        coords_file = cif.read(extract_params.coord_list_file)
        coords_block = coords_file.sole_block()
        particles_x = np.array(coords_block.find_loop("_rlnCoordinateX"))
        particles_y = np.array(coords_block.find_loop("_rlnCoordinateY"))

        # CTF results are stored in a txt file with the CTF outputs
        with open(Path(extract_params.ctf_file).with_suffix(".txt"), "r") as f:
            ctf_results = f.readlines()[-1].split()

        # Construct the output star file
        output_document = cif.Document()
        output_file_block = output_document.add_new_block("images")
        output_loop = output_file_block.init_loop(
            "_rln",
            [
                "CoordinateX",
                "CoordinateY",
                "ImageName",
                "MicrographName",
                "OpticsGroup",
                "CtfMaxResolution",
                "CtfFigureOfMerit",
                "DefocusU",
                "DefocusV",
                "DefocusAngle",
                "CtfBfactor",
                "CtfScalefactor",
                "PhaseShift",
            ],
        )
        for particle in range(len(particles_x)):
            output_loop.add_row(
                [
                    particles_x[particle],
                    particles_y[particle],
                    f"{particle:06}@{output_mrc_file}",
                    extract_params.micrographs_file,
                    "1",
                    ctf_results[6],
                    ctf_results[5],
                    ctf_results[1],
                    ctf_results[2],
                    ctf_results[3],
                    "0.0",
                    "1.0",
                    "0.0",
                ]
            )

        # Extraction
        if extract_params.downscale:
            output_angpix = (
                extract_params.pix_size
                * extract_params.extract_boxsize
                / extract_params.downscale_boxsize
            )
        else:
            output_angpix = extract_params.pix_size
        extract_width = int(extract_params.extract_boxsize / 2)

        input_micrograph = mrcfile.open(extract_params.micrographs_file)
        input_micrograph_image = np.array(input_micrograph.data, dtype=np.float32)
        image_size = np.shape(input_micrograph_image)
        output_mrc_stack = []

        for particle in range(len(particles_x)):
            pixel_location_x = int(float(particles_x[particle]))
            pixel_location_y = int(float(particles_y[particle]))

            # Extract the particle image and pad the edges if it is not square
            x_low_pad = 0
            x_high_pad = 0
            y_low_pad = 0
            y_high_pad = 0

            x_low = pixel_location_x - extract_width
            if x_low < 0:
                x_low_pad = -x_low
                x_low = 0
            x_high = pixel_location_x + extract_width
            if x_high >= image_size[0]:
                x_high_pad = x_high - image_size[0]
                x_high = image_size[0]
            y_low = pixel_location_y - extract_width
            if y_low < 0:
                y_low_pad = -y_low
                y_low = 0
            y_high = pixel_location_y + extract_width
            if y_high >= image_size[1]:
                y_high_pad = y_high - image_size[1]
                y_high = image_size[1]

            subimage = input_micrograph_image[x_low:x_high, y_low:y_high]
            subimage = np.pad(
                subimage,
                ((x_low_pad, x_high_pad), (y_low_pad, y_high_pad)),
                mode="edge",
            )

            # Downscale

            # Background subtract

            # Add to output stack
            if len(output_mrc_stack):
                output_mrc_stack = np.append(output_mrc_stack, [subimage], axis=0)
            else:
                output_mrc_stack = np.array([subimage], dtype=np.float32)
        print(np.shape(output_mrc_stack), output_angpix)

        mrcfile.write(output_mrc_file, data=output_mrc_stack, overwrite=True)

        rw.transport.ack(header)

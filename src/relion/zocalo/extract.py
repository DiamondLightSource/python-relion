from __future__ import annotations

import re
from pathlib import Path

import cv2
import mrcfile
import numpy as np
import workflows.recipe
from gemmi import cif
from pydantic import BaseModel, Field
from pydantic.error_wrappers import ValidationError
from workflows.services.common_service import CommonService

from relion.zocalo.spa_relion_service_options import RelionServiceOptions


class ExtractParameters(BaseModel):
    micrographs_file: str = Field(..., min_length=1)
    coord_list_file: str = Field(..., min_length=1)
    output_file: str = Field(..., min_length=1)
    pix_size: float
    ctf_values: dict
    particle_diameter: float
    norm: bool = True
    bg_radius: int = -1
    downscale: bool = False
    invert_contrast: bool = True
    relion_options: RelionServiceOptions


class Extract(CommonService):
    """
    A service for extracting particles from cryolo autopicking
    """

    # Human readable service name
    _service_name = "Extract"

    # Logger name
    _logger_name = "relion.zocalo.extract"

    # Job name
    job_type = "relion.extract"

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
            f"{extract_params.ctf_values['file']}, {extract_params.coord_list_file} "
            f"Output: {extract_params.output_file}"
        )

        # Update the relion options to get out the box sizes
        extract_params.relion_options.particle_diameter = (
            extract_params.particle_diameter
        )
        extract_params.relion_options.downscale = extract_params.downscale

        # Make sure the output directory exists
        job_dir = Path(re.search(".+/job[0-9]{3}/", extract_params.output_file)[0])
        project_dir = job_dir.parent.parent
        if not Path(extract_params.output_file).parent.exists():
            Path(extract_params.output_file).parent.mkdir(parents=True)
        output_mrc_file = (
            Path(extract_params.output_file).parent
            / Path(extract_params.micrographs_file).with_suffix(".mrcs").name
        )

        # If no background radius set diameter as 75% of box
        if extract_params.bg_radius == -1:
            extract_params.bg_radius = round(
                0.375 * extract_params.relion_options.boxsize
            )

        # Find the locations of the particles
        coords_file = cif.read(extract_params.coord_list_file)
        coords_block = coords_file.sole_block()
        particles_x = np.array(coords_block.find_loop("_rlnCoordinateX"))
        particles_y = np.array(coords_block.find_loop("_rlnCoordinateY"))

        # Construct the output star file
        extracted_parts_doc = cif.Document()
        extracted_parts_block = extracted_parts_doc.add_new_block("particles")
        extracted_parts_loop = extracted_parts_block.init_loop(
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
            extracted_parts_loop.add_row(
                [
                    particles_x[particle],
                    particles_y[particle],
                    f"{particle:06}@{output_mrc_file.relative_to(project_dir)}",
                    str(Path(extract_params.micrographs_file).relative_to(project_dir)),
                    "1",
                    str(extract_params.ctf_values["CtfMaxResolution"]),
                    str(extract_params.ctf_values["CtfFigureOfMerit"]),
                    str(extract_params.ctf_values["DefocusU"]),
                    str(extract_params.ctf_values["DefocusV"]),
                    str(extract_params.ctf_values["DefocusAngle"]),
                    "0.0",
                    "1.0",
                    "0.0",
                ]
            )
        extracted_parts_doc.write_file(
            extract_params.output_file, style=cif.Style.Simple
        )

        # Extraction
        extract_width = round(extract_params.relion_options.boxsize / 2)
        input_micrograph = mrcfile.open(extract_params.micrographs_file)
        input_micrograph_image = np.array(input_micrograph.data, dtype=np.float32)
        image_size = np.shape(input_micrograph_image)
        output_mrc_stack = []

        for particle in range(len(particles_x)):
            # Pixel locations are from bottom left, need to flip the image later
            pixel_location_x = round(float(particles_x[particle]))
            pixel_location_y = round(float(particles_y[particle]))

            # Extract the particle image and pad the edges if it is not square
            x_left_pad = 0
            x_right_pad = 0
            y_top_pad = 0
            y_bot_pad = 0

            x_left = pixel_location_x - extract_width
            if x_left < 0:
                x_left_pad = -x_left
                x_left = 0
            x_right = pixel_location_x + extract_width
            if x_right >= image_size[1]:
                x_right_pad = x_right - image_size[1]
                x_right = image_size[1]
            y_top = pixel_location_y - extract_width
            if y_top < 0:
                y_top_pad = -y_top
                y_top = 0
            y_bot = pixel_location_y + extract_width
            if y_bot >= image_size[0]:
                y_bot_pad = y_bot - image_size[0]
                y_bot = image_size[0]

            particle_subimage = input_micrograph_image[y_top:y_bot, x_left:x_right]
            particle_subimage = np.flip(particle_subimage, axis=0)
            particle_subimage = np.pad(
                particle_subimage,
                ((y_bot_pad, y_top_pad), (x_left_pad, x_right_pad)),
                mode="edge",
            )

            # Flip all the values on inversion
            if extract_params.invert_contrast:
                particle_subimage = -1 * particle_subimage

            # Background normalisation
            if extract_params.norm:
                # Distance of each pixel from the centre, compared to background radius
                grid_indexes = np.meshgrid(
                    np.arange(2 * extract_width), np.arange(2 * extract_width)
                )
                distance_from_centre = np.sqrt(
                    (grid_indexes[0] - extract_width + 0.5) ** 2
                    + (grid_indexes[1] - extract_width + 0.5) ** 2
                )
                bg_region = (
                    distance_from_centre
                    > np.ones(np.shape(particle_subimage)) * extract_params.bg_radius
                )

                # Standardise the values using the background
                bg_mean = np.mean(particle_subimage[bg_region])
                bg_std = np.std(particle_subimage[bg_region])
                particle_subimage = (particle_subimage - bg_mean) / bg_std

            # Downscale the image size
            if extract_params.downscale:
                extract_params.relion_options.pixel_size_downscaled = (
                    extract_params.pix_size
                    * extract_params.relion_options.boxsize
                    / extract_params.relion_options.small_boxsize
                )
                particle_subimage = cv2.resize(
                    particle_subimage,
                    dsize=(
                        extract_params.relion_options.small_boxsize,
                        extract_params.relion_options.small_boxsize,
                    ),
                    interpolation=cv2.INTER_CUBIC,
                )

            # Add to output stack
            if len(output_mrc_stack):
                output_mrc_stack = np.append(
                    output_mrc_stack, [particle_subimage], axis=0
                )
            else:
                output_mrc_stack = np.array([particle_subimage], dtype=np.float32)

        # Produce the mrc file of the extracted particles
        if extract_params.downscale:
            box_len = extract_params.relion_options.small_boxsize
            pix_size = extract_params.relion_options.pixel_size_downscaled
        else:
            box_len = extract_params.relion_options.boxsize
            pix_size = extract_params.relion_options.pixel_size_on_image

        self.log.info(f"Extracted {np.shape(output_mrc_stack)[0]} particles")
        with mrcfile.new(str(output_mrc_file), overwrite=True) as mrc:
            mrc.set_data(output_mrc_stack)
            mrc.header.mx = box_len
            mrc.header.my = box_len
            mrc.header.mz = 1
            mrc.header.cella.x = pix_size * box_len
            mrc.header.cella.y = pix_size * box_len
            mrc.header.cella.z = 1

        # Register the extract job with the node creator
        self.log.info(f"Sending {self.job_type} to node creator")
        node_creator_parameters = {
            "job_type": "relion.extract",
            "input_file": extract_params.coord_list_file
            + ":"
            + extract_params.ctf_values["file"],
            "output_file": extract_params.output_file,
            "relion_options": dict(extract_params.relion_options),
            "command": "",
            "stdout": "",
            "stderr": "",
            "results": {"box_size": box_len},
        }
        if isinstance(rw, MockRW):
            rw.transport.send(
                destination="spa.node_creator",
                message={"parameters": node_creator_parameters, "content": "dummy"},
            )
        else:
            rw.send_to("spa.node_creator", node_creator_parameters)

        # Register the files needed for selection and batching
        self.log.info("Sending to particle selection")
        select_params = {
            "input_file": extract_params.output_file,
            "batch_size": extract_params.relion_options.batch_size,
            "image_size": box_len,
            "relion_options": dict(extract_params.relion_options),
        }
        if isinstance(rw, MockRW):
            rw.transport.send(
                destination="select.particles",
                message={"parameters": select_params, "content": "dummy"},
            )
        else:
            rw.send_to("select.particles", select_params)

        self.log.info(f"Done {self.job_type} for {extract_params.coord_list_file}.")
        rw.transport.ack(header)

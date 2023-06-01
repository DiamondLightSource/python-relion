from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import workflows.recipe
from gemmi import cif
from pydantic import BaseModel, Field
from pydantic.error_wrappers import ValidationError
from workflows.services.common_service import CommonService

from relion.zocalo.spa_output_files import get_optics_table


class SelectParticlesParameters(BaseModel):
    input_file: str = Field(..., min_length=1)
    batch_size: int
    mc_uuid: int
    relion_it_options: Optional[dict] = None


class SelectParticles(CommonService):
    """
    A service for batching particles
    """

    # Human readable service name
    _service_name = "SelectParticles"

    # Logger name
    _logger_name = "relion.zocalo.select.particles"

    def initializing(self):
        """Subscribe to a queue. Received messages must be acknowledged."""
        self.log.info("Select particles service starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "select.particles",
            self.select,
            acknowledgement=True,
            log_extender=self.extend_log,
            allow_non_recipe_messages=True,
        )

    def select(self, rw, header: dict, message: dict):
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
                select_params = SelectParticlesParameters(
                    **{**rw.recipe_step.get("parameters", {}), **message}
                )
            else:
                select_params = SelectParticlesParameters(
                    **{**rw.recipe_step.get("parameters", {})}
                )
        except (ValidationError, TypeError):
            self.log.warning(
                f"Selection parameter validation failed for message: {message} "
                f"and recipe parameters: {rw.recipe_step.get('parameters', {})}"
            )
            rw.transport.nack(header)
            return

        self.log.info(f"Inputs: {select_params.input_file}")

        extract_job_dir = Path(
            re.search(".+/job[0-9]{3}/", select_params.input_file)[0]
        )
        project_dir = extract_job_dir.parent.parent

        select_job_num = (
            int(re.search("/job[0-9]{3}", str(extract_job_dir))[0][4:7]) + 1
        )
        select_dir = project_dir / f"Select/job{select_job_num:03}"
        select_dir.mkdir(parents=True, exist_ok=True)

        extracted_parts_file = cif.read_file(select_params.input_file)
        extracted_parts_block = extracted_parts_file.sole_block()
        extracted_parts_loop = extracted_parts_block.find_loop(
            "_rlnCoordinateX"
        ).get_loop()

        current_splits = sorted(select_dir.glob("particles_split*.star"))
        num_new_parts = extracted_parts_loop.length()
        num_remaining_parts = extracted_parts_loop.length()
        if current_splits:
            # If this is a continuation, find the previous split files
            select_output_file = str(current_splits[-1])

            particles_cif = cif.read_file(select_output_file)
            prev_parts_block = particles_cif.find_block("particles")
            prev_parts_loop = prev_parts_block.find_loop("_rlnCoordinateX").get_loop()

            num_prev_parts = prev_parts_loop.length()
            # While we have particles to add and the file is not full
            while num_prev_parts < select_params.batch_size and num_remaining_parts > 0:
                new_row = []
                for col in range(extracted_parts_loop.width()):
                    new_row.append(
                        extracted_parts_loop.val(
                            num_new_parts - num_remaining_parts, col
                        )
                    )
                prev_parts_loop.add_row(new_row)

                num_prev_parts += 1
                num_remaining_parts -= 1

            particles_cif.write_file(select_output_file)
        else:
            # If this is the first time we ran the job create a new particle split
            # Set this to be split zero so the while loop starts from one
            select_output_file = str(select_dir / "particles_split0.star")

        # If we filled the last file we need a new one for the remaining particles
        while num_remaining_parts > 0:
            new_split = int(re.search("split[0-9]+", select_output_file)[0][5:]) + 1
            select_output_file = str(select_dir / f"particles_split{new_split}.star")
            new_particles_cif = get_optics_table(select_params.relion_it_options)

            new_split_block = new_particles_cif.add_new_block("particles")
            new_split_loop = new_split_block.init_loop(
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

            num_prev_parts = 0
            # While we have particles to add and the file is not full
            while num_prev_parts < select_params.batch_size and num_remaining_parts > 0:
                new_row = []
                for col in range(extracted_parts_loop.width()):
                    new_row.append(
                        extracted_parts_loop.val(
                            num_new_parts - num_remaining_parts, col
                        )
                    )
                new_split_loop.add_row(new_row)

                num_prev_parts += 1
                num_remaining_parts -= 1

            new_particles_cif.write_file(select_output_file)

        # Send to node creator
        node_creator_params = {
            "job_type": "relion.select.split",
            "input_file": select_params.input_file,
            "output_file": select_output_file,
            "relion_it_options": select_params.relion_it_options,
        }
        if isinstance(rw, MockRW):
            rw.transport.send(
                destination="spa.node_creator",
                message={"parameters": node_creator_params, "content": "dummy"},
            )
        else:
            rw.send_to("spa.node_creator", node_creator_params)

        rw.transport.ack(header)
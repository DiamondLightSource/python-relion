from __future__ import annotations

import json
import os
from pathlib import Path

import workflows.recipe
from pipeliner.job_factory import read_job
from pipeliner.process import Process
from pipeliner.project_graph import ProjectGraph, update_jobinfo_file
from pydantic.error_wrappers import ValidationError
from workflows.services.common_service import CommonService

from relion.zocalo.zocalo_spa.job_centre import SpaParameters
from relion.zocalo.zocalo_spa.output_files import create_output_files


class NodeCreator(CommonService):
    """
    A service for setting up pipeliner jobs
    """

    # Human readable service name
    _service_name = "SPA_NodeCreator"

    # Logger name
    _logger_name = "relion.zocalo_spa.node_setup"

    # Values to extract for ISPyB
    shift_list = []

    def initializing(self):
        """Subscribe to a queue. Received messages must be acknowledged."""
        self.log.info("SPA node creator service starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "spa.node_creator",
            self.spa_node_creator,
            acknowledgement=True,
            log_extender=self.extend_log,
            allow_non_recipe_messages=True,
        )

    def spa_node_creator(self, rw, header: dict, message: dict):
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

        # Read in and validate the parameters
        try:
            if isinstance(message, dict):
                job_info = SpaParameters(
                    **{**rw.recipe_step.get("parameters", {}), **message}
                )
            else:
                job_info = SpaParameters(**{**rw.recipe_step.get("parameters", {})})
        except (ValidationError, TypeError):
            self.log.warning(
                f"Node creator parameter validation failed for message: {message} "
                f"and recipe parameters: {rw.recipe_step.get('parameters', {})}"
            )
            rw.transport.nack(header)
            return

        self.log.info(f"Received information to process job {job_info.job_dir}")

        # Make sure we are in the processing directory
        os.chdir(job_info.processing_dir)

        if not Path(job_info.job_dir).exists():
            self.log.error(f"This job has not been run: {job_info.job_type}")
            rw.transport.nack(header)
            return

        # Mark the job completion status
        for exit_file in Path(f"{job_info.job_dir}").glob("PIPELINER_JOB_EXIT_*"):
            exit_file.unlink()
        if job_info.job_status.lower() == "success":
            Path(f"{job_info.job_dir}/PIPELINER_JOB_EXIT_SUCCESS").touch()
        else:
            Path(f"{job_info.job_dir}/PIPELINER_JOB_EXIT_FAILURE").touch()

        # Load this job as a pipeliner job to create the nodes
        pipeliner_job = read_job(str(job_info.job_dir / "job.star"))
        pipeliner_job.output_dir = str(job_info.job_dir) + "/"
        relion_commands = pipeliner_job.get_commands()

        # Write the output files which Relion produces
        create_output_files(
            job_type=job_info.job_type,
            job_dir=job_info.job_dir,
            file_to_add=job_info.file,
            options=job_info.options,
        )

        # Produce the node display files
        for node in pipeliner_job.input_nodes + pipeliner_job.output_nodes:
            if node.name[0].isalpha():
                node.write_default_result_file()

        # Save the metadata file
        metadata_dict = pipeliner_job.gather_metadata()
        with open(job_info.job_dir / "job_metadata.json", "w") as metadata_file:
            metadata_file.write(json.dumps(metadata_dict))

        # Create the node and default_pipeline.star files in the project directory
        with ProjectGraph(read_only=False) as project:
            process = project.add_new_process(
                Process(
                    name=str(job_info.job_dir),
                    p_type=job_info.job_type,
                    status="Running",
                ),
                do_overwrite=True,
            )
            # Add each of the nodes to the project process
            for node in pipeliner_job.input_nodes:
                if node.name[0].isalpha():
                    project.add_new_input_edge(node, process)
            for node in pipeliner_job.output_nodes:
                if node.name[0].isalpha():
                    project.add_new_output_edge(process, node)
            # Add the job commands to the process jobinfo
            update_jobinfo_file(
                process, action="Run", command_list=[[], relion_commands]
            )
            # Generate the default_pipeline.star file
            project.check_process_completion()
            # Copy the default_pipeline.star file
            Path(job_info.job_dir / "default_pipeline.star").write_bytes(
                Path("default_pipeline.star").read_bytes()
            )

        self.log.info(f"Processed outputs from job {job_info.job_dir}")
        rw.transport.ack(header)


"""Files to create
continue_job.star
note.txt
"""

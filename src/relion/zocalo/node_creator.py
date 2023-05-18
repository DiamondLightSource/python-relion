from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Optional

import workflows.recipe
from pipeliner.api.api_utils import (
    edit_jobstar,
    job_default_parameters_dict,
    write_default_jobstar,
)
from pipeliner.job_factory import read_job
from pipeliner.node_factory import create_node
from pipeliner.nodes import NODE_MICROGRAPHCOORDSGROUP
from pipeliner.process import Process
from pipeliner.project_graph import ProjectGraph, update_jobinfo_file
from pydantic import BaseModel, Field
from pydantic.error_wrappers import ValidationError
from workflows.services.common_service import CommonService

from relion.cryolo_relion_it.cryolo_relion_it import RelionItOptions
from relion.pipeline.options import generate_pipeline_options
from relion.zocalo.spa_output_files import create_output_files

pipeline_spa_jobs = {
    "relion.import.movies": {"folder": "Import", "input_label": "fn_in_raw"},
    "relion.motioncorr.motioncor2": {
        "folder": "MotionCorr",
        "input_label": "input_star_mics",
        "input_star": "movies.star",
    },
    "icebreaker.micrograph_analysis.micrographs": {
        "folder": "IceBreaker",
        "input_label": "in_mics",
        "input_star": "corrected_micrographs.star",
    },
    "icebreaker.micrograph_analysis.enhancecontrast": {
        "folder": "IceBreaker",
        "input_label": "in_mics",
        "input_star": "corrected_micrographs.star",
    },
    "icebreaker.micrograph_analysis.summary": {
        "folder": "IceBreaker",
        "input_label": "in_mics",
        "input_star": "grouped_micrographs.star",
    },
    "relion.ctffind.ctffind4": {
        "folder": "CtfFind",
        "input_label": "input_star_mics",
        "input_star": "corrected_micrographs.star",
    },
    "cryolo.autopick": {
        "folder": "AutoPick",
        "input_label": "input_file",
        "input_star": "corrected_micrographs.star",
        "extra_output_nodes": {"autopick.star": NODE_MICROGRAPHCOORDSGROUP},
    },
    "relion.extract": {"folder": "Extract", "input_label": "coords_suffix"},
    "relion.select.split": {"folder": "Select", "input_label": "fn_data"},
    "icebreaker.micrograph_analysis.particles": {"folder": "IceBreaker"},
    "relion.class2d.em": {"folder": "Class2D"},
    "relion.class2d.vdam": {"folder": "Class2D"},
    "relion.initialmodel": {"folder": "InitialModel"},
    "relion.class3d": {"folder": "Class3D"},
}


class NodeCreatorParameters(BaseModel):
    job_type: str
    input_file: str = Field(..., min_length=1)
    output_file: str = Field(..., min_length=1)
    relion_it_options: RelionItOptions
    results: Optional[dict] = None


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
                job_info = NodeCreatorParameters(
                    **{**rw.recipe_step.get("parameters", {}), **message}
                )
            else:
                job_info = NodeCreatorParameters(
                    **{**rw.recipe_step.get("parameters", {})}
                )
        except (ValidationError, TypeError):
            self.log.warning(
                f"Node creator parameter validation failed for message: {message} "
                f"and recipe parameters: {rw.recipe_step.get('parameters', {})}"
            )
            rw.transport.nack(header)
            return

        self.log.info(
            f"Received job {job_info.job_type} with output file {job_info.output_file}"
        )

        # Find the job directory and make sure we are in the processing directory
        job_dir = Path(re.search(".+/job[0-9]{3}/", job_info.output_file)[0])
        project_dir = job_dir.parent.parent
        os.chdir(project_dir)

        try:
            # Get the options for this job out of the RelionItOptions
            pipeline_options = generate_pipeline_options(
                job_info.relion_it_options,
                {job_info.job_type: ""},
            )
            # Work out the name of the input star file
            if job_dir.parent.name != "Import":
                input_job_dir = Path(
                    re.search(".+/job[0-9]{3}/", job_info.input_file)[0]
                )
                input_star_file = (
                    input_job_dir.relative_to(project_dir)
                    / pipeline_spa_jobs[job_info.job_type]["input_star"]
                )
            else:
                input_star_file = Path(job_info.input_file).relative_to(project_dir)
            # Add this to the job.star options
            pipeline_options[job_info.job_type][
                pipeline_spa_jobs[job_info.job_type]["input_label"]
            ] = input_star_file

            # If this is a new job we need a job.star
            if not Path(f"{job_info.job_type.replace('.', '_')}_job.star").is_file():
                self.log.info(f"Generating options for new job: {job_info.job_type}")
                write_default_jobstar(job_info.job_type)
                params = job_default_parameters_dict(job_info.job_type)
                params.update(pipeline_options.get(job_info.job_type, {}))
                _params = {
                    k: str(v) for k, v in params.items() if not isinstance(v, bool)
                }
                _params.update(
                    {
                        k: ("Yes" if v else "No")
                        for k, v in params.items()
                        if isinstance(v, bool)
                    }
                )
                params = _params
                edit_jobstar(
                    f"{job_info.job_type.replace('.', '_')}_job.star",
                    params,
                    f"{job_info.job_type.replace('.', '_')}_job.star",
                )

                # Copy the job.star file
                (job_dir / "job.star").write_bytes(
                    Path(f"{job_info.job_type.replace('.', '_')}_job.star").read_bytes()
                )
                Path(
                    f".gui_{job_info.job_type.replace('.', '_')}_job.star"
                ).write_bytes(
                    Path(f"{job_info.job_type.replace('.', '_')}_job.star").read_bytes()
                )
        except IndexError:
            self.log.error(f"Unknown job type: {job_info.job_type}")
            rw.transport.nack(header)
            return

        # Mark the job completion status
        for exit_file in job_dir.glob("PIPELINER_JOB_EXIT_*"):
            exit_file.unlink()
        (job_dir / "PIPELINER_JOB_EXIT_SUCCESS").touch()

        # Load this job as a pipeliner job to create the nodes
        pipeliner_job = read_job(f"{job_dir}/job.star")
        pipeliner_job.output_dir = str(job_dir.relative_to(project_dir)) + "/"
        pipeliner_job.create_input_nodes()
        relion_commands = pipeliner_job.get_commands()
        relion_commands = pipeliner_job.prepare_final_command(
            relion_commands, do_makedir=False
        )

        # Write the output files which Relion produces
        create_output_files(
            job_type=job_info.job_type,
            job_dir=job_dir.relative_to(project_dir),
            input_file=Path(job_info.input_file).relative_to(project_dir),
            output_file=Path(job_info.output_file).relative_to(project_dir),
            relion_it_options=dict(job_info.relion_it_options),
            results=job_info.results,
        )

        if pipeline_spa_jobs[job_info.job_type].get("extra_output_nodes"):
            for node_name, node_type in pipeline_spa_jobs[job_info.job_type][
                "extra_output_nodes"
            ].items():
                pipeliner_job.output_nodes.append(
                    create_node(
                        str(job_dir.relative_to(project_dir) / node_name),
                        node_type,
                        job_info.job_type.split("."),
                    )
                )

        # Produce the node display files
        for node in pipeliner_job.input_nodes + pipeliner_job.output_nodes:
            if node.name[0].isalpha():
                node.write_default_result_file()

        # Save the metadata file
        metadata_dict = pipeliner_job.gather_metadata()
        with open(job_dir / "job_metadata.json", "w") as metadata_file:
            metadata_file.write(json.dumps(metadata_dict))

        # Create the node and default_pipeline.star files in the project directory
        with ProjectGraph(read_only=False) as project:
            process = project.add_new_process(
                Process(
                    name=str(job_dir.relative_to(project_dir)),
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
            # Add the job commands to the process .CCPEM_pipeliner_jobinfo file
            update_jobinfo_file(process, action="Run", command_list=relion_commands)
            # Generate the default_pipeline.star file
            project.check_process_completion()
            # Copy the default_pipeline.star file
            (job_dir / "default_pipeline.star").write_bytes(
                Path("default_pipeline.star").read_bytes()
            )

        self.log.info(f"Processed outputs from job {job_info.job_type}")
        rw.transport.ack(header)
from __future__ import annotations

import datetime
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
from pipeliner.api.manage_project import PipelinerProject
from pipeliner.data_structure import FAIL_FILE, SUCCESS_FILE
from pipeliner.job_factory import read_job
from pipeliner.project_graph import ProjectGraph
from pydantic import BaseModel, Field, ValidationError
from workflows.services.common_service import CommonService

from relion.zocalo.spa_output_files import create_output_files
from relion.zocalo.spa_relion_service_options import (
    RelionServiceOptions,
    generate_service_options,
)

# A dictionary of all the available jobs,
# the folder name they run in, and the names of their inputs in the job star
pipeline_spa_jobs = {
    "relion.import.movies": {"folder": "Import"},
    "relion.motioncorr.motioncor2": {
        "folder": "MotionCorr",
        "input_stars": {"input_star_mics": "movies.star"},
    },
    "icebreaker.micrograph_analysis.micrographs": {
        "folder": "IceBreaker",
        "input_stars": {"in_mics": "corrected_micrographs.star"},
    },
    "icebreaker.micrograph_analysis.enhancecontrast": {
        "folder": "IceBreaker",
        "input_stars": {"in_mics": "corrected_micrographs.star"},
    },
    "icebreaker.micrograph_analysis.summary": {
        "folder": "IceBreaker",
        "input_stars": {"in_mics": "grouped_micrographs.star"},
    },
    "relion.ctffind.ctffind4": {
        "folder": "CtfFind",
        "input_stars": {"input_star_mics": "corrected_micrographs.star"},
    },
    "cryolo.autopick": {
        "folder": "AutoPick",
        "input_stars": {"input_file": "corrected_micrographs.star"},
    },
    "relion.extract": {
        "folder": "Extract",
        "input_stars": {
            "coords_suffix": "autopick.star",
            "star_mics": "micrographs_ctf.star",
        },
    },
    "relion.select.split": {
        "folder": "Select",
        "input_stars": {"fn_data": "particles.star"},
    },
    "icebreaker.micrograph_analysis.particles": {
        "folder": "IceBreaker",
        "input_stars": {
            "in_mics": "grouped_micrographs.star",
            "in_parts": "particles_split1.star",
        },
    },
    "relion.class2d.em": {
        "folder": "Class2D",
        "input_stars": {"fn_img": "particles_split1.star"},
    },
    "relion.class2d.vdam": {
        "folder": "Class2D",
        "input_stars": {"fn_img": "particles_split1.star"},
    },
    "relion.select.class2dauto": {
        "folder": "Select",
        "input_stars": {"fn_model": "run_it020_optimiser.star"},
    },
    "combine_star_files_job": {
        "folder": "Select",
        "input_stars": {"files_to_process": "particles.star"},
    },
    "relion.initialmodel": {
        "folder": "InitialModel",
        "input_stars": {"fn_img": "particles_split1.star"},
    },
    "relion.class3d": {
        "folder": "Class3D",
        "input_stars": {
            "fn_img": "particles_split1.star",
            "fn_ref": "initial_model.mrc",
        },
    },
}


class NodeCreatorParameters(BaseModel):
    job_type: str
    input_file: str = Field(..., min_length=1)
    output_file: str = Field(..., min_length=1)
    relion_options: RelionServiceOptions
    command: str
    stdout: str
    stderr: str
    success: bool = True
    results: Optional[dict] = None


class NodeCreator(CommonService):
    """
    A service for setting up pipeliner jobs
    """

    # Human readable service name
    _service_name = "NodeCreator"

    # Logger name
    _logger_name = "relion.zocalo.node_creator"

    # Values to extract for ISPyB
    shift_list = []

    def initializing(self):
        """Subscribe to a queue. Received messages must be acknowledged."""
        self.log.info("Relion node creator service starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "node_creator",
            self.node_creator,
            acknowledgement=True,
            log_extender=self.extend_log,
            allow_non_recipe_messages=True,
        )

    def node_creator(self, rw, header: dict, message: dict):
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
        except (ValidationError, TypeError) as e:
            self.log.warning(
                f"Node creator parameter validation failed for message: {message} "
                f"and recipe parameters: {rw.recipe_step.get('parameters', {})} "
                f"with exception: {e}"
            )
            rw.transport.nack(header)
            return

        self.log.info(
            f"Received job {job_info.job_type} with output file {job_info.output_file}"
        )
        start_time = datetime.datetime.now()

        # Find the job directory and make sure we are in the processing directory
        job_dir = Path(re.search(".+/job[0-9]{3}", job_info.output_file)[0])
        project_dir = job_dir.parent.parent
        os.chdir(project_dir)

        if not (project_dir / "default_pipeline.star").exists():
            self.log.info("No existing project found, so creating one")
            PipelinerProject(make_new_project=True)

        try:
            # Get the options for this job out of the RelionServiceOptions
            pipeline_options = generate_service_options(
                job_info.relion_options,
                job_info.job_type,
            )
            # Work out the name of the input star file and add this to the job.star
            if job_dir.parent.name != "Import":
                ii = 0
                for label, star in pipeline_spa_jobs[job_info.job_type][
                    "input_stars"
                ].items():
                    input_job_dir = Path(
                        re.search(
                            ".+/job[0-9]{3}/", job_info.input_file.split(":")[ii]
                        )[0]
                    )
                    pipeline_options[label] = (
                        input_job_dir.relative_to(project_dir) / star
                    )
                    ii += 1
            else:
                pipeline_options["fn_in_raw"] = job_info.input_file

            # If this is a new job we need a job.star
            if not Path(f"{job_info.job_type.replace('.', '_')}_job.star").is_file():
                self.log.info(f"Generating options for new job: {job_info.job_type}")
                write_default_jobstar(job_info.job_type)
                params = job_default_parameters_dict(job_info.job_type)
                params.update(pipeline_options)
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
        except IndexError:
            self.log.error(f"Unknown job type: {job_info.job_type}")
            rw.transport.nack(header)
            return

        # Copy the job.star file
        (job_dir / "job.star").write_bytes(
            Path(f"{job_info.job_type.replace('.', '_')}_job.star").read_bytes()
        )

        # Mark the job completion status
        for exit_file in job_dir.glob("PIPELINER_JOB_EXIT_*"):
            exit_file.unlink()
        if job_info.success:
            (job_dir / SUCCESS_FILE).touch()
        else:
            (job_dir / FAIL_FILE).touch()

        # Load this job as a pipeliner job to create the nodes
        pipeliner_job = read_job(f"{job_dir}/job.star")
        pipeliner_job.output_dir = str(job_dir.relative_to(project_dir)) + "/"
        relion_commands = [[], pipeliner_job.get_final_commands()]
        pipeliner_job.prepare_to_run()

        # Write the log files
        with open(job_dir / "run.out", "w") as f:
            f.write(job_info.stdout)
        with open(job_dir / "run.err", "a") as f:
            f.write(f"{job_info.stderr}\n")
        with open(job_dir / "note.txt", "a") as f:
            f.write(f"{job_info.command}\n")

        if job_info.success:
            # Write the output files which Relion produces
            extra_output_nodes = create_output_files(
                job_type=job_info.job_type,
                job_dir=job_dir.relative_to(project_dir),
                input_file=(
                    job_info.input_file
                    if job_dir.parent.name == "Import"
                    else Path(job_info.input_file).relative_to(project_dir)
                ),
                output_file=Path(job_info.output_file).relative_to(project_dir),
                relion_options=job_info.relion_options,
                results=job_info.results,
            )
            if extra_output_nodes:
                # Add any extra nodes if they are not already present
                existing_nodes = []
                for node in pipeliner_job.output_nodes:
                    existing_nodes.append(node.name)
                for node in extra_output_nodes.keys():
                    if (
                        f"{job_dir.relative_to(project_dir)}/{node}"
                        not in existing_nodes
                    ):
                        pipeliner_job.add_output_node(
                            node,
                            extra_output_nodes[node][0],
                            extra_output_nodes[node][1],
                        )

            # Save the metadata file
            metadata_dict = pipeliner_job.gather_metadata()
            with open(job_dir / "job_metadata.json", "w") as metadata_file:
                metadata_file.write(json.dumps(metadata_dict))

        # Create the node and default_pipeline.star files in the project directory
        with ProjectGraph(read_only=False) as project:
            process = project.add_job(
                pipeliner_job,
                as_status=("Succeeded" if job_info.success else "Failed"),
                do_overwrite=True,
            )
            # Add the job commands to the process .CCPEM_pipeliner_jobinfo file
            process.update_jobinfo_file(action="Run", command_list=relion_commands)
            # Generate the default_pipeline.star file
            project.check_process_completion()
            # Copy the default_pipeline.star file
            (job_dir / "default_pipeline.star").write_bytes(
                Path("default_pipeline.star").read_bytes()
            )

        end_time = datetime.datetime.now()
        self.log.info(
            f"Processed outputs from job {job_info.job_type}, "
            f"in {(end_time - start_time).total_seconds()} seconds."
        )
        rw.transport.ack(header)

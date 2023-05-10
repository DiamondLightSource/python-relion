from __future__ import annotations

import copy
import os
from pathlib import Path
from typing import Dict, Optional

import workflows.recipe
from pipeliner.api.api_utils import (
    edit_jobstar,
    job_default_parameters_dict,
    write_default_jobstar,
)
from pipeliner.starfile_handler import JobStar
from pydantic import BaseModel
from pydantic.error_wrappers import ValidationError
from workflows.services.common_service import CommonService

from relion.cryolo_relion_it.cryolo_relion_it import RelionItOptions
from relion.pipeline.extra_options import generate_extra_options
from relion.pipeline.options import generate_pipeline_options

pipeline_jobs = {
    "relion.import.movies": "Import",
    "relion.motioncorr.motioncor2": "MotionCorr",
    "relion.motioncorr.own": "MotionCorr",
    "icebreaker.micrograph_analysis.micrographs": "IceBreaker",
    "icebreaker.micrograph_analysis.enhancecontrast": "IceBreaker",
    "icebreaker.micrograph_analysis.summary": "IceBreaker",
    "relion.ctffind.ctffind4": "CtfFind",
    "relion.autopick.log": "AutoPick",
    "relion.autopick.ref3d": "AutoPick",
    "cryolo.autopick": "AutoPick",
    "relion.extract": "Extract",
    "relion.select.split": "Select",
    "icebreaker.micrograph_analysis.particles": "IceBreaker",
    "relion.class2d.em": "Class2D",
    "relion.class2d.vdam": "Class2D",
    "relion.initialmodel": "InitialModel",
    "relion.class3d": "Class3D",
}


class SpaParameters(BaseModel):
    processing_dir: Path
    file: Path
    job_type: str
    options: RelionItOptions
    job_paths: Dict[str, Path] = {}
    job_number: int = 1
    job_status: str = "Running"
    job_dir: Optional[Path] = None
    job_params: Optional[dict] = None


class JobRunner(CommonService):
    """
    A service for setting up pipeliner jobs
    """

    # Human readable service name
    _service_name = "SPA_JobRunner"

    # Logger name
    _logger_name = "relion.zocalo_spa.job_centre"

    # Values to extract for ISPyB
    shift_list = []

    def initializing(self):
        """Subscribe to a queue. Received messages must be acknowledged."""
        self.log.info("SPA job runner service starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "spa.job_centre",
            self.spa_job_centre,
            acknowledgement=True,
            log_extender=self.extend_log,
            allow_non_recipe_messages=True,
        )

    def spa_job_centre(self, rw, header: dict, message: dict):
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
                f"Job runner parameter validation failed for message: {message} "
                f"and recipe parameters: {rw.recipe_step.get('parameters', {})}"
            )
            rw.transport.nack(header)
            return

        # Make sure we are in the processing directory
        os.chdir(job_info.processing_dir)

        # Get the options for this job out of the RelionItOptions
        try:
            pipeline_options = generate_pipeline_options(
                job_info.options,
                {job_info.job_type: ""},
            )
        except IndexError:
            self.log.error(f"Unknown job type: {job_info.job_type}")
            rw.transport.nack(header)
            return

        # Create the directory in which to run the job
        try:
            job_dir = Path(
                f"{pipeline_jobs[job_info.job_type]}/job{job_info.job_number:03d}"
            )
            if not job_dir.is_dir():
                job_dir.mkdir(parents=True)
                job_info.job_paths[job_info.job_type] = job_dir

                # As this is a new job we need a job.star
                self.log.info(f"Generating options for new job: {job_info.job_type}")
                write_default_jobstar(job_info.job_type)
                params = job_default_parameters_dict(job_info.job_type)
                params.update(pipeline_options.get(job_info.job_type, {}))
                extra_params = generate_extra_options(
                    job_info.job_type, job_info.job_paths, job_info.options
                )
                if extra_params is not None:
                    params.update(extra_params)
                _params = {
                    k: str(v) for k, v in params.items() if not isinstance(v, bool)
                }

                def _b2s(bv: bool) -> str:
                    if bv:
                        return "Yes"
                    return "No"

                _params.update(
                    {k: _b2s(v) for k, v in params.items() if isinstance(v, bool)}
                )
                params = _params
                edit_jobstar(
                    f"{job_info.job_type.replace('.', '_')}_job.star",
                    params,
                    f"{job_info.job_type.replace('.', '_')}_job.star",
                )

                # Copy the job.star file
                Path(job_dir / "job.star").write_bytes(
                    Path(f"{job_info.job_type.replace('.', '_')}_job.star").read_bytes()
                )
                Path(
                    f".gui_{job_info.job_type.replace('.', '_')}_job.star"
                ).write_bytes(
                    Path(f"{job_info.job_type.replace('.', '_')}_job.star").read_bytes()
                )
            else:
                # If we have run this job before then read the job.star
                params_jobstar = JobStar(
                    f"{job_info.job_type.replace('.', '_')}_job.star"
                )
                params = params_jobstar.all_options_as_dict()
                job_info.job_paths[job_info.job_type] = job_dir

        except IndexError:
            self.log.error(f"Unknown job type: {job_info.job_type}")
            rw.transport.nack(header)
            return

        # Create and send message with file and job name to job service
        service_parameters = dict(job_info)
        service_parameters.update(
            {
                "processing_dir": str(job_info.processing_dir),
                "file": str(job_info.file),
                "options": dict(job_info.options),
                "job_paths": {
                    job: str(jobdir) for (job, jobdir) in job_info.job_paths.items()
                },
                "job_dir": str(job_dir),
                "job_params": params,
            }
        )

        if job_info.job_type == "relion.import.movies":
            # If this is an import job then we only need to set up the nodes
            self.log.info("Sending to pipeliner node processing service")
            node_creator_parameters = copy.deepcopy(service_parameters)
            node_creator_parameters.update({"job_status": "Success"})
            if isinstance(rw, RW_mock):
                rw.transport.send(
                    destination="spa.node_creator",
                    message={"parameters": node_creator_parameters, "content": "dummy"},
                )
            else:
                rw.send_to("spa.node_creator", node_creator_parameters)

            # Then set the job to be motion correction
            job_info.job_type = "spa.job_centre"
            service_parameters.update(
                {
                    "job_number": job_info.job_number + 1,
                    "job_type": "relion.motioncorr.motioncor2",
                    "job_params": {},
                }
            )

        # For all jobs except import a message is sent to the job service
        self.log.info(f"Sending to job service {job_info.job_type}")
        if isinstance(rw, RW_mock):
            rw.transport.send(
                destination=job_info.job_type,
                message={"parameters": service_parameters, "content": "dummy"},
            )
        else:
            rw.send_to(job_info.job_type, service_parameters)

        rw.transport.ack(header)


"""Service process
First step records files and sets up project (murfey?)
Send file name to centre service, creates mc job dir, job.star and params
Send mc job params and file to mc service
mc service runs
Send completion marker to node creator
Send file name and name of next job to centre service, creates ctf job
"""

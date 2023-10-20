from __future__ import annotations

import re
import string
import subprocess
from collections import ChainMap
from math import hypot
from pathlib import Path
from typing import Optional

import plotly.express as px
import workflows.recipe
from pydantic import BaseModel, Field, ValidationError, validator
from workflows.services.common_service import CommonService

from relion.zocalo.spa_relion_service_options import RelionServiceOptions


class MotionCorrParameters(BaseModel):
    movie: str = Field(..., min_length=1)
    mrc_out: str = Field(..., min_length=1)
    experiment_type: str
    pix_size: float
    fm_dose: float
    patch_size: dict = {"x": 5, "y": 5}
    gpu: int = 0
    gain_ref: str = None
    rot_gain: int = None
    flip_gain: int = None
    dark: str = None
    use_gpus: int = None
    sum_range: Optional[dict] = None
    iter: int = None
    tol: float = None
    throw: int = None
    trunc: int = None
    fm_ref: int = 1
    kv: int = None
    fm_int_file: str = None
    mag: Optional[dict] = None
    ft_bin: float = None
    serial: int = None
    in_suffix: str = None
    eer_sampling: int = None
    out_stack: int = None
    bft: Optional[dict] = None
    group: int = None
    defect_file: str = None
    arc_dir: str = None
    in_fm_motion: int = None
    split_sum: int = None
    dose_motionstats_cutoff: float = 4.0
    movie_id: int
    mc_uuid: int
    picker_uuid: int
    relion_options: Optional[RelionServiceOptions] = None
    ctf: dict = {}

    @validator("experiment_type")
    def is_spa_or_tomo(cls, experiment):
        if experiment not in ["spa", "tomography"]:
            raise ValueError("Specify an experiment type of spa or tomography.")
        return experiment

    class Config:
        ignore_extra = True


class ChainMapWithReplacement(ChainMap):
    def __init__(self, *maps, substitutions=None) -> None:
        super().__init__(*maps)
        self._substitutions = substitutions

    def __getitem__(self, k):
        v = super().__getitem__(k)
        if self._substitutions and isinstance(v, str) and "$" in v:
            template = string.Template(v)
            return template.substitute(**self._substitutions)
        return v


class MotionCorr(CommonService):
    """
    A service for motion correcting cryoEM movies using MotionCor2
    """

    # Human readable service name
    _service_name = "MotionCorr"

    # Logger name
    _logger_name = "relion.zocalo.motioncorr"

    # Job name
    job_type = "relion.motioncorr.motioncor2"

    # Values to extract for ISPyB
    x_shift_list = []
    y_shift_list = []

    def initializing(self):
        """Subscribe to a queue. Received messages must be acknowledged."""
        self.log.info("Motion correction service starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "motioncorr",
            self.motion_correction,
            acknowledgement=True,
            log_extender=self.extend_log,
            allow_non_recipe_messages=True,
        )

    def parse_mc_output(self, mc_stdout: str):
        """
        Read the output logs of MotionCorr to determine
        the movement of each frame
        """
        frames_line = False
        for line in mc_stdout.split("\n"):
            # Frame reading in MotionCorr 1.4.0
            if line.startswith("...... Frame"):
                line_split = line.split()
                self.x_shift_list.append(float(line_split[-2]))
                self.y_shift_list.append(float(line_split[-1]))

            # Alternative frame reading for MotionCorr 1.6.3
            if not line:
                frames_line = False
            if frames_line:
                line_split = line.split()
                self.x_shift_list.append(float(line_split[1]))
                self.y_shift_list.append(float(line_split[2]))
            if "x Shift" in line:
                frames_line = True

    def motioncor2(self, command, mrc_out):
        """Run the MotionCor2 command"""
        result = subprocess.run(command, capture_output=True)
        self.parse_mc_output(result.stdout.decode("utf8", "replace"))
        return result

    def motion_correction(self, rw, header: dict, message: dict):
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
            rw.recipe_step = {"parameters": message["parameters"], "output": None}
            rw.environment = {"has_recipe_wrapper": False}
            rw.set_default_channel = rw.dummy
            rw.send = rw.dummy
            message = message["content"]

        command = ["MotionCor2"]

        parameter_map = ChainMapWithReplacement(
            message if isinstance(message, dict) else {},
            rw.recipe_step["parameters"],
            substitutions=rw.environment,
        )

        try:
            if isinstance(message, dict):
                mc_params = MotionCorrParameters(**{**dict(parameter_map), **message})
            else:
                mc_params = MotionCorrParameters(**{**dict(parameter_map)})
        except (ValidationError, TypeError) as e:
            self.log.warning(
                f"Motion correction parameter validation failed for message: {message} "
                f"and recipe parameters: {rw.recipe_step.get('parameters', {})} "
                f"with exception: {e}"
            )
            rw.transport.nack(header)
            return

        # Determine the input and output files
        if not Path(mc_params.mrc_out).parent.exists():
            Path(mc_params.mrc_out).parent.mkdir(parents=True)
        if mc_params.movie.endswith(".mrc"):
            input_flag = "-InMrc"
        elif mc_params.movie.endswith((".tif", ".tiff")):
            input_flag = "-InTiff"
        elif mc_params.movie.endswith(".eer"):
            input_flag = "-InEer"
        else:
            self.log.error(f"No input flag found for movie {mc_params.movie}")
            input_flag = None
            rw.transport.nack(header)
        command.extend([input_flag, mc_params.movie])

        mc_flags = {
            "mrc_out": "-OutMrc",
            "patch_size": "-Patch",
            "pix_size": "-PixSize",
            "gain_ref": "-Gain",
            "rot_gain": "-RotGain",
            "flip_gain": "-FlipGain",
            "dark": "-Dark",
            "gpu": "-Gpu",
            "use_gpus": "-UseGpus",
            "sum_range": "-SumRange",
            "iter": "-Iter",
            "tol": "-Tol",
            "throw": "-Throw",
            "trunc": "-Trunc",
            "fm_ref": "-FmRef",
            "kv": "-Kv",
            "fm_dose": "-FmDose",
            "fm_int_file": "-FmIntFile",
            "mag": "-Mag",
            "ft_bin": "-FtBin",
            "serial": "-Serial",
            "in_suffix": "-InSuffix",
            "eer_sampling": "-EerSampling",
            "out_stack": "-OutStack",
            "bft": "-Bft",
            "group": "-Group",
            "defect_file": "-DefectFile",
            "arc_dir": "-ArcDir",
            "in_fm_motion": "-InFmMotion",
            "split_sum": "-SplitSum",
        }

        # Create the motion correction command
        for k, v in mc_params.dict().items():
            if v and (k in mc_flags):
                if type(v) is dict:
                    command.extend((mc_flags[k], " ".join(str(_) for _ in v.values())))
                else:
                    command.extend((mc_flags[k], str(v)))

        self.log.info(f"Input: {mc_params.movie} Output: {mc_params.mrc_out}")

        # Run motion correction and confirm it ran successfully
        result = self.motioncor2(command, mc_params.mrc_out)
        if result.returncode:
            self.log.error(
                f"Motion correction of {mc_params.movie} "
                f"failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            if mc_params.experiment_type == "spa":
                # On spa failure send the outputs to the node creator
                node_creator_parameters = {
                    "job_type": self.job_type,
                    "input_file": mc_params.mrc_out,
                    "output_file": mc_params.mrc_out,
                    "relion_options": dict(mc_params.relion_options),
                    "command": " ".join(command),
                    "stdout": result.stdout.decode("utf8", "replace"),
                    "stderr": result.stderr.decode("utf8", "replace"),
                    "success": False,
                }
                if isinstance(rw, MockRW):
                    rw.transport.send(
                        destination="node_creator",
                        message={
                            "parameters": node_creator_parameters,
                            "content": "dummy",
                        },
                    )
                else:
                    rw.send_to("node_creator", node_creator_parameters)
            rw.transport.nack(header)
            return

        # Extract results for ispyb
        total_motion = 0
        early_motion = 0
        late_motion = 0
        cutoff_frame = round(mc_params.dose_motionstats_cutoff / mc_params.fm_dose)
        for i in range(1, len(self.x_shift_list)):
            total_motion += hypot(
                self.x_shift_list[i] - self.x_shift_list[i - 1],
                self.y_shift_list[i] - self.y_shift_list[i - 1],
            )
            if i < cutoff_frame:
                early_motion += hypot(
                    self.x_shift_list[i] - self.x_shift_list[i - 1],
                    self.y_shift_list[i] - self.y_shift_list[i - 1],
                )
            else:
                late_motion += hypot(
                    self.x_shift_list[i] - self.x_shift_list[i - 1],
                    self.y_shift_list[i] - self.y_shift_list[i - 1],
                )
        average_motion_per_frame = total_motion / len(self.x_shift_list)

        # Extract results for ispyb
        fig = px.scatter(x=self.x_shift_list, y=self.y_shift_list)
        drift_plot_name = str(Path(mc_params.movie).stem) + "_drift_plot.json"
        plot_path = Path(mc_params.mrc_out).parent / drift_plot_name
        snapshot_path = Path(mc_params.mrc_out).with_suffix(".jpeg")
        fig.write_json(plot_path)

        # Forward results to ISPyB
        ispyb_parameters = {
            "ispyb_command": "buffer",
            "buffer_command": {"ispyb_command": "insert_motion_correction"},
            "buffer_store": mc_params.mc_uuid,
            "first_frame": 1,
            "last_frame": len(self.x_shift_list),
            "total_motion": total_motion,
            "average_motion_per_frame": average_motion_per_frame,
            "drift_plot_full_path": str(plot_path),
            "micrograph_snapshot_full_path": str(snapshot_path),
            "micrograph_full_path": str(mc_params.mrc_out),
            "patches_used_x": mc_params.patch_size["x"],
            "patches_used_y": mc_params.patch_size["y"],
            "dose_per_frame": mc_params.fm_dose,
        }
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

        # If this is SPA, determine and set up the next jobs
        if mc_params.experiment_type == "spa":
            # Set up icebreaker if requested, then ctffind
            if mc_params.relion_options.do_icebreaker_jobs:
                # Three IceBreaker jobs: CtfFind job is MC+4
                ctf_job_number = 6

                # Both IceBreaker micrographs and flattening inherit from motioncorr
                self.log.info(
                    f"Sending to IceBreaker micrograph analysis: {mc_params.mrc_out}"
                )
                icebreaker_job003_params = {
                    "icebreaker_type": "micrographs",
                    "input_micrographs": mc_params.mrc_out,
                    "output_path": re.sub(
                        "MotionCorr/job002/.+",
                        "IceBreaker/job003/",
                        mc_params.mrc_out,
                    ),
                    "mc_uuid": mc_params.mc_uuid,
                    "relion_options": dict(mc_params.relion_options),
                    "total_motion": total_motion,
                    "early_motion": early_motion,
                    "late_motion": late_motion,
                }
                if isinstance(rw, MockRW):
                    rw.transport.send(
                        destination="icebreaker",
                        message={
                            "parameters": icebreaker_job003_params,
                            "content": "dummy",
                        },
                    )
                else:
                    rw.send_to("icebreaker", icebreaker_job003_params)

                self.log.info(
                    f"Sending to IceBreaker contract enhancement: {mc_params.mrc_out}"
                )
                icebreaker_job004_params = {
                    "icebreaker_type": "enhancecontrast",
                    "input_micrographs": mc_params.mrc_out,
                    "output_path": re.sub(
                        "MotionCorr/job002/.+",
                        "IceBreaker/job004/",
                        mc_params.mrc_out,
                    ),
                    "mc_uuid": mc_params.mc_uuid,
                    "relion_options": dict(mc_params.relion_options),
                    "total_motion": total_motion,
                    "early_motion": early_motion,
                    "late_motion": late_motion,
                }
                if isinstance(rw, MockRW):
                    rw.transport.send(
                        destination="icebreaker",
                        message={
                            "parameters": icebreaker_job004_params,
                            "content": "dummy",
                        },
                    )
                else:
                    rw.send_to("icebreaker", icebreaker_job004_params)

            else:
                # No IceBreaker jobs: CtfFind job is MC+1
                ctf_job_number = 3
            mc_params.ctf["output_image"] = str(
                Path(
                    mc_params.mrc_out.replace(
                        "MotionCorr/job002", f"CtfFind/job{ctf_job_number:03}"
                    )
                ).with_suffix(".ctf")
            )
            mc_params.ctf["relion_options"] = dict(mc_params.relion_options)
            mc_params.ctf["amplitude_contrast"] = mc_params.relion_options.ampl_contrast

        # Forward results to ctffind (in both SPA and tomography)
        self.log.info(f"Sending to ctf: {mc_params.mrc_out}")
        mc_params.ctf["experiment_type"] = mc_params.experiment_type
        mc_params.ctf["input_image"] = mc_params.mrc_out
        mc_params.ctf["mc_uuid"] = mc_params.mc_uuid
        mc_params.ctf["picker_uuid"] = mc_params.picker_uuid
        mc_params.ctf["pix_size"] = mc_params.pix_size
        if isinstance(rw, MockRW):
            rw.transport.send(  # type: ignore
                destination="ctffind",
                message={"parameters": mc_params.ctf, "content": "dummy"},
            )
        else:
            rw.send_to("ctffind", mc_params.ctf)

        # Forward results to images service
        self.log.info(f"Sending to images service {mc_params.mrc_out}")
        if isinstance(rw, MockRW):
            rw.transport.send(
                destination="images",
                message={
                    "image_command": "mrc_to_jpeg",
                    "file": mc_params.mrc_out,
                },
            )
        else:
            rw.send_to(
                "images",
                {
                    "image_command": "mrc_to_jpeg",
                    "file": mc_params.mrc_out,
                },
            )

        # If this is SPA, send the results to be processed by the node creator
        if mc_params.experiment_type == "spa":
            # As this is the entry point we need to import the file to the project
            self.log.info("Sending relion.import.movies to node creator")
            project_dir = Path(
                re.search(".+/job[0-9]{3}/", mc_params.mrc_out)[0]
            ).parent.parent
            import_movie = (
                project_dir
                / "Import/job001"
                / Path(mc_params.mrc_out)
                .relative_to(project_dir / "MotionCorr/job002")
                .parent
                / Path(mc_params.movie).name
            )
            if not import_movie.parent.is_dir():
                import_movie.parent.mkdir(parents=True)
            import_movie.unlink(missing_ok=True)
            import_movie.symlink_to(mc_params.movie)
            import_parameters = {
                "job_type": "relion.import.movies",
                "input_file": str(mc_params.movie),
                "output_file": str(import_movie),
                "relion_options": dict(mc_params.relion_options),
                "command": "",
                "stdout": "",
                "stderr": "",
            }
            if isinstance(rw, MockRW):
                rw.transport.send(
                    destination="node_creator",
                    message={"parameters": import_parameters, "content": "dummy"},
                )
            else:
                rw.send_to("node_creator", import_parameters)

            # Then register the motion correction job with the node creator
            self.log.info(f"Sending {self.job_type} to node creator")
            node_creator_parameters = {
                "job_type": self.job_type,
                "input_file": str(import_movie),
                "output_file": mc_params.mrc_out,
                "relion_options": dict(mc_params.relion_options),
                "command": " ".join(command),
                "stdout": result.stdout.decode("utf8", "replace"),
                "stderr": result.stderr.decode("utf8", "replace"),
                "results": {
                    "total_motion": total_motion,
                    "early_motion": early_motion,
                    "late_motion": late_motion,
                },
            }
            if isinstance(rw, MockRW):
                rw.transport.send(
                    destination="node_creator",
                    message={"parameters": node_creator_parameters, "content": "dummy"},
                )
            else:
                rw.send_to("node_creator", node_creator_parameters)

        # Register completion with Murfey if this is tomography
        if mc_params.experiment_type == "tomography":
            self.log.info("Sending to Murfey")
            if isinstance(rw, MockRW):
                rw.transport.send(
                    "murfey_feedback",
                    {
                        "register": "motion_corrected",
                        "movie": mc_params.movie,
                        "mrc_out": mc_params.mrc_out,
                        "movie_id": mc_params.movie_id,
                    },
                )
            else:
                rw.send_to(
                    "murfey_feedback",
                    {
                        "register": "motion_corrected",
                        "movie": mc_params.movie,
                        "mrc_out": mc_params.mrc_out,
                        "movie_id": mc_params.movie_id,
                    },
                )

        self.log.info(f"Done {self.job_type} for {mc_params.movie}.")
        rw.transport.ack(header)
        self.x_shift_list = []
        self.y_shift_list = []

from __future__ import annotations

import re
import string
from collections import ChainMap
from math import hypot
from pathlib import Path
from typing import Literal, Optional

import plotly.express as px
import procrunner
import workflows.recipe
from pydantic import BaseModel, Field
from pydantic.error_wrappers import ValidationError
from workflows.services.common_service import CommonService


class MotionCorrParameters(BaseModel):
    collection_type: Literal["spa", "tomography"]
    pix_size: float
    ctf: dict
    movie: str = Field(..., min_length=1)
    mrc_out: str = Field(..., min_length=1)
    patch_size: int = 5
    gpu: int = 0
    gain_ref: str = None
    mc_uuid: int
    rot_gain: int = None
    flip_gain: int = None
    dark: str = None
    use_gpus: int = None
    sum_range: Optional[tuple] = None
    iter: int = None
    tol: float = None
    throw: int = None
    trunc: int = None
    fm_ref: int = None
    kv: int = None
    fm_dose: float = None
    fm_int_file: str = None
    mag: Optional[tuple] = None
    ft_bin: float = None
    serial: int = None
    in_suffix: str = None
    eer_sampling: int = None
    out_stack: int = None
    bft: Optional[tuple] = None
    group: int = None
    detect_file: str = None
    arc_dir: str = None
    in_fm_motion: int = None
    split_sum: int = None
    movie_id: int
    relion_it_options: Optional[dict] = None

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

    # Values to extract for ISPyB
    shift_list = []

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

    def parse_mc_output(self, line: str):
        if not line:
            return

        if line.startswith("...... Frame"):
            line_split = line.split()
            self.shift_list.append((float(line_split[-2]), float(line_split[-1])))

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
        except (ValidationError, TypeError):
            self.log.warning(
                f"Motion correction parameter validation failed for message: {message} "
                f"and recipe parameters: {rw.recipe_step.get('parameters', {})}"
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
            "patch": "-Patch",
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
            "detect_file": "-DetectFile",
            "arc_dir": "-ArcDir",
            "in_fm_motion": "-InFmMotion",
            "split_sum": "-SplitSum",
        }

        # Create the motion correction command
        for k, v in mc_params.dict().items():
            if v and (k in mc_flags):
                if type(v) is tuple:
                    command.extend((mc_flags[k], " ".join(str(_) for _ in v)))
                else:
                    command.extend((mc_flags[k], str(v)))

        self.log.info(f"Input: {mc_params.movie} Output: {mc_params.mrc_out}")

        # Run motion correction
        result = procrunner.run(command=command, callback_stdout=self.parse_mc_output)
        if result.returncode:
            self.log.error(
                f"Motion correction of {mc_params.movie} "
                f"failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            rw.transport.nack(header)
            return

        # Extract the motion for the image
        total_x_shift = sum([item[0] for item in self.shift_list])
        total_y_shift = sum([item[1] for item in self.shift_list])
        total_motion = hypot(total_x_shift, total_y_shift)
        each_total_motion = [hypot(item[0], item[1]) for item in self.shift_list]
        average_motion_per_frame = sum(each_total_motion) / len(self.shift_list)

        # If this is SPA, determine and set up the next jobs
        if mc_params.collection_type.lower() == "spa":
            # Set up icebreaker if requested, then ctffind
            if mc_params.relion_it_options["do_icebreaker_job_group"]:
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
                    "relion_it_options": mc_params.relion_it_options,
                    "total_motion": total_motion,
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
                    "relion_it_options": mc_params.relion_it_options,
                    "total_motion": total_motion,
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
            mc_params.ctf["collection_type"] = "spa"
            mc_params.ctf["output_image"] = str(
                Path(
                    mc_params.mrc_out.replace(
                        "MotionCorr/job002", f"CtfFind/job{ctf_job_number:03}"
                    )
                ).with_suffix(".ctf")
            )
            mc_params.ctf["relion_it_options"] = mc_params.relion_it_options
            mc_params.ctf["amplitude_contrast"] = mc_params.relion_it_options[
                "ampl_contrast"
            ]

        # Forward results to ctffind (in both SPA and tomography)
        self.log.info(f"Sending to ctf: {mc_params.mrc_out}")
        mc_params.ctf["input_image"] = mc_params.mrc_out
        mc_params.ctf["mc_uuid"] = mc_params.mc_uuid
        mc_params.ctf["pix_size"] = mc_params.pix_size
        if isinstance(rw, MockRW):
            rw.transport.send(  # type: ignore
                destination="ctffind",
                message={"parameters": mc_params.ctf, "content": "dummy"},
            )
        else:
            rw.send_to("ctffind", mc_params.ctf)

        # Extract results for ispyb
        drift_plot_x = [range(0, len(self.shift_list))]
        drift_plot_y = each_total_motion
        fig = px.scatter(x=drift_plot_x, y=drift_plot_y)
        drift_plot_name = str(Path(mc_params.movie).stem) + "_drift_plot.json"
        plot_path = Path(mc_params.mrc_out).parent / drift_plot_name
        snapshot_path = Path(mc_params.mrc_out).with_suffix(".jpeg")
        fig.write_json(plot_path)

        ispyb_parameters = {
            "first_frame": 1,
            "last_frame": len(self.shift_list),
            "total_motion": total_motion,
            "average_motion_per_frame": average_motion_per_frame,
            "drift_plot_full_path": str(plot_path),
            "micrograph_snapshot_full_path": str(snapshot_path),
            "micrograph_full_path": str(mc_params.mrc_out),
            "patches_used_x": mc_params.patch_size,
            "patches_used_y": mc_params.patch_size,
            "buffer_store": mc_params.mc_uuid,
            "dose_per_frame": mc_params.fm_dose,
        }

        # Forward results to ISPyB
        ispyb_parameters.update(
            {
                "ispyb_command": "buffer",
                "buffer_command": {"ispyb_command": "insert_motion_correction"},
            }
        )
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
            rw.send_to("ispyb", ispyb_parameters)

        # Forward results to murfey
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

        # Forward results to images service
        self.log.info(f"Sending to images service {mc_params.mrc_out}")
        if isinstance(rw, MockRW):
            rw.transport.send(
                destination="images",
                message={
                    "parameters": {"images_command": "mrc_to_jpeg"},
                    "file": mc_params.mrc_out,
                },
            )
        else:
            rw.send_to(
                "images",
                {
                    "parameters": {"images_command": "mrc_to_jpeg"},
                    "file": mc_params.mrc_out,
                },
            )

        # If this is SPA, send the results to be processed by the node creator
        if mc_params.collection_type == "spa":
            # As this is the entry point we need to import the file to the project
            project_dir = Path(
                re.search(".+/job[0-9]{3}/", mc_params.mrc_out)[0]
            ).parent.parent
            import_movie = (
                project_dir
                / "Import/job001"
                / Path(mc_params.movie).relative_to(project_dir)
            )
            if not import_movie.parent.is_dir():
                import_movie.parent.mkdir(parents=True)
            import_movie.unlink(missing_ok=True)
            import_movie.symlink_to(mc_params.movie)
            import_parameters = {
                "job_type": "relion.import.movies",
                "input_file": str(mc_params.movie),
                "output_file": str(import_movie),
                "relion_it_options": mc_params.relion_it_options,
            }
            if isinstance(rw, MockRW):
                rw.transport.send(
                    destination="spa.node_creator",
                    message={"parameters": import_parameters, "content": "dummy"},
                )
            else:
                rw.send_to("spa.node_creator", import_parameters)

            # Then register the motion correction job with the node creator
            node_creator_parameters = {
                "job_type": "relion.motioncorr.motioncor2",
                "input_file": str(import_movie),
                "output_file": mc_params.mrc_out,
                "relion_it_options": mc_params.relion_it_options,
                "results": {"total_motion": str(total_motion)},
            }
            if isinstance(rw, MockRW):
                rw.transport.send(
                    destination="spa.node_creator",
                    message={"parameters": node_creator_parameters, "content": "dummy"},
                )
            else:
                rw.send_to("spa.node_creator", node_creator_parameters)

        rw.transport.ack(header)
        self.shift_list = []

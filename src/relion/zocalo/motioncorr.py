from __future__ import annotations

import string
from collections import ChainMap
from math import hypot
from pathlib import Path
from typing import Optional

import plotly.express as px
import procrunner
import workflows.recipe
from pydantic import BaseModel, Field
from pydantic.error_wrappers import ValidationError
from workflows.services.common_service import CommonService


class MotionCorrParameters(BaseModel):
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
    _service_name = "DLS MotionCorr"

    # Logger name
    _logger_name = "relion.zocalo.motioncorr"

    # Values to extract for ISPyB
    x_shift_list = []
    y_shift_list = []
    each_total_motion = []

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
            self.x_shift_list.append(float(line_split[-2]))
            self.y_shift_list.append(float(line_split[-1]))
            self.each_total_motion.append(
                hypot(float(line_split[-2]), float(line_split[-1]))
            )

    def motion_correction(self, rw, header: dict, message: dict):
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
                f"Motion correction parameter validation failed for message: {message} and recipe parameters: {rw.recipe_step.get('parameters', {})}"
            )
            rw.transport.nack(header)
            return
        if Path(mc_params.mrc_out).is_file():
            self.log.info(f"File exists {mc_params.mrc_out}")
            rw.transport.ack(header)
            return
        movie = mc_params.movie
        if movie.endswith(".mrc"):
            input_flag = "-InMrc"
        elif movie.endswith((".tif", ".tiff")):
            input_flag = "-InTiff"
        elif movie.endswith(".eer"):
            input_flag = "-InEer"
        else:
            self.log.error(f"No input flag found for movie {movie}")
            input_flag = None
            rw.transport.nack(header)
        command.extend([input_flag, movie])

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

        for k, v in mc_params.dict().items():
            if v and (k in mc_flags):
                if type(v) is tuple:
                    command.extend((mc_flags[k], " ".join(str(_) for _ in v)))
                else:
                    command.extend((mc_flags[k], str(v)))

        self.log.info(f"Input: {movie} Output: {mc_params.mrc_out}")

        result = procrunner.run(command=command, callback_stdout=self.parse_mc_output)
        if result.returncode:
            self.log.error(
                f"Motion correction of {movie} failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            rw.transport.nack(header)
            return

        # Forward results to ctffind
        self.log.info("Sending to ctf")
        mc_params.ctf["input_image"] = mc_params.mrc_out
        mc_params.ctf["mc_uuid"] = mc_params.mc_uuid
        mc_params.ctf["pix_size"] = mc_params.pix_size
        if isinstance(rw, RW_mock):
            rw.transport.send(  # type: ignore
                destination="ctffind",
                message={"parameters": mc_params.ctf, "content": "dummy"},
            )
        else:
            rw.send_to("ctf", mc_params.ctf)

        # Extract results for ispyb
        total_motion = 0
        for i in range(1, len(self.x_shift_list)):
            total_motion += hypot(
                self.x_shift_list[i] - self.x_shift_list[i - 1],
                self.y_shift_list[i] - self.y_shift_list[i - 1],
            )
        average_motion_per_frame = total_motion / len(self.x_shift_list)

        drift_plot_x = [range(0, len(self.x_shift_list))]
        drift_plot_y = self.each_total_motion
        fig = px.scatter(x=drift_plot_x, y=drift_plot_y)
        drift_plot_name = str(Path(mc_params.movie).stem) + "_drift_plot.json"
        plot_path = Path(mc_params.mrc_out).parent / drift_plot_name
        snapshot_path = Path(mc_params.mrc_out).with_suffix(".jpeg")
        fig.write_json(plot_path)

        ispyb_parameters = {
            "first_frame": 1,
            "last_frame": len(self.x_shift_list),
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
        if isinstance(rw, RW_mock):
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
        if isinstance(rw, RW_mock):
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
                "murfey",
                {
                    "register": "motion_corrected",
                    "movie": mc_params.movie,
                    "mrc_out": mc_params.mrc_out,
                    "movie_id": mc_params.movie_id,
                },
            )

        # Forward results to images service
        self.log.info(f"Sending to images service {mc_params.mrc_out}")
        if isinstance(rw, RW_mock):
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

        rw.transport.ack(header)
        self.x_shift_list = []
        self.y_shift_list = []
        self.each_total_motion = []

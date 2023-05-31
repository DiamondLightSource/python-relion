from __future__ import annotations

import ast
import os.path
import time
from pathlib import Path
from typing import List, Optional, Union

import plotly.express as px
import procrunner
import workflows.recipe
import workflows.transport
from pydantic import BaseModel, Field, validator
from pydantic.error_wrappers import ValidationError
from workflows.services.common_service import CommonService


class TomoParameters(BaseModel):
    stack_file: str = Field(..., min_length=1)
    path_pattern: str = None
    input_file_list: str = None
    position: Optional[str] = None
    aretomo_output_file: Optional[str] = None
    vol_z: int = 1200
    align: Optional[int] = None
    out_bin: int = 4
    tilt_axis: Optional[float] = None
    tilt_cor: int = 1
    flip_int: Optional[int] = None
    flip_vol: int = 1
    wbp: Optional[int] = None
    roi_file: list = []
    patch: Optional[int] = None
    kv: Optional[int] = None
    align_file: Optional[str] = None
    angle_file: Optional[str] = None
    align_z: Optional[int] = None
    pix_size: Optional[float] = None
    init_val: Optional[int] = None
    refine_flag: Optional[int] = None
    out_imod: int = 1
    out_imod_xf: Optional[int] = None
    dark_tol: Optional[Union[int, str]] = None
    manual_tilt_offset: Optional[float] = None

    @validator("input_file_list")
    def check_only_one_is_provided(cls, v, values):
        if not v and not values.get("path_pattern"):
            raise ValueError("input_file_list or path_pattern must be provided")
        if v and values.get("path_pattern"):
            raise ValueError(
                "Message must only include one of 'path_pattern' and 'input_file_list'. Both are set or one has been set by the recipe."
            )
        return v

    @validator("input_file_list")
    def convert_to_list_of_lists(cls, v):
        file_list = None
        try:
            file_list = ast.literal_eval(
                v
            )  # if input_file_list is '' it will break here
        except Exception:
            return v
        if isinstance(file_list, list) and isinstance(file_list[0], list):
            return file_list
        else:
            raise ValueError("input_file_list is not a list of lists")

    @validator("input_file_list")
    def check_lists_are_not_empty(cls, v):
        for item in v:
            if not item:
                raise ValueError("Empty list found")
        return v


class TomoAlign(CommonService):
    """
    A service for grouping and aligning tomography tilt-series with Newstack and AreTomo
    """

    # Human readable service name
    _service_name = "DLS TomoAlign"
    # Logger name
    _logger_name = "relion.zocalo.tomo_align"

    # Values to extract for ISPyB
    refined_tilts: List[float] | None = None
    tilt_offset: float | None = None
    rot_centre_z_list: List[str] = []
    rot_centre_z: str | None = None
    rot: float | None = None
    mag: float | None = None
    plot_path: str | None = None
    plot_file: str | None = None
    dark_images_file: str | None = None
    imod_directory: str | None = None
    xy_proj_file: str | None = None
    xz_proj_file: str | None = None
    central_slice_file: str | None = None
    tomogram_movie_file: str | None = None
    newstack_path: str | None = None
    alignment_output_dir: str | None = None
    stack_name: str | None = None

    def initializing(self):
        """Subscribe to a queue. Received messages must be acknowledged."""
        self.log.info("TomoAlign service starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "tomo_align",
            self.tomo_align,
            acknowledgement=True,
            log_extender=self.extend_log,
            allow_non_recipe_messages=True,
        )

    def parse_tomo_output(self, line):
        if line.startswith("Rot center Z"):
            self.rot_centre_z_list.append(line.split()[5])
        if line.startswith("Tilt offset"):
            self.tilt_offset = float(line.split()[2].strip(","))

    def extract_from_aln(self, tomo_parameters):
        tomo_aln_file = None
        x_shift = []
        y_shift = []
        self.refined_tilts = []
        aln_files = list(Path(self.alignment_output_dir).glob("*.aln"))

        file_name = Path(tomo_parameters.stack_file).stem
        for aln_file in aln_files:
            if file_name in str(aln_file):
                tomo_aln_file = aln_file

        with open(tomo_aln_file) as f:
            lines = f.readlines()
            for line in lines:
                if not line.startswith("#"):
                    line_split = line.split()
                    if self.rot is None:
                        self.rot = float(line_split[1])
                    if self.mag is None:
                        self.mag = float(line_split[2])
                    x_shift.append(float(line_split[3]))
                    y_shift.append(float(line_split[4]))
                    self.refined_tilts.append(float(line_split[9]))
        fig = px.scatter(x=x_shift, y=y_shift)
        fig.write_json(self.plot_path)
        return tomo_aln_file  # not needed anywhere atm

    def tomo_align(self, rw, header: dict, message: dict):
        class RW_mock:
            transport: workflows.transport.common_transport.CommonTransport

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
                tomo_params = TomoParameters(
                    **{**rw.recipe_step.get("parameters", {}), **message}
                )
            else:
                tomo_params = TomoParameters(**{**rw.recipe_step.get("parameters", {})})
            tomo_params.pix_size = tomo_params.pix_size * 1e10
        except (ValidationError, TypeError) as e:
            self.log.warning(
                f"{e} TomoAlign parameter validation failed for message: {message} and recipe parameters: {rw.recipe_step.get('parameters', {})}"
            )
            rw.transport.nack(header)
            return

        def _tilt(file_list):
            return float(file_list[1])

        if tomo_params.path_pattern:
            directory = Path(tomo_params.path_pattern).parent

            input_file_list = []
            for item in directory.glob(Path(tomo_params.path_pattern).name):
                parts = str(Path(item).with_suffix("").name).split("_")
                for part in parts:
                    if "." in part:
                        input_file_list.append([str(item), part])
            tomo_params.input_file_list = input_file_list

        self.log.info(f"Input list {tomo_params.input_file_list}")
        tomo_params.input_file_list.sort(key=_tilt)

        tilt_dict: dict = {}
        for tilt in tomo_params.input_file_list:
            if not Path(tilt[0]).is_file():
                self.log.warning(f"File not found {tilt[0]}")
                rw.transport.nack(header)
            if tilt[1] not in tilt_dict:
                tilt_dict[tilt[1]] = []
            tilt_dict[tilt[1]].append(tilt[0])

        values_to_remove = []
        for item in tilt_dict:
            values = tilt_dict[item]
            if len(values) > 1:
                # sort by age and remove oldest ones
                values.sort(key=os.path.getctime)
                values_to_remove = values[1:]

        for tilt in tomo_params.input_file_list:
            if tilt[0] in values_to_remove:
                index = tomo_params.input_file_list.index(tilt)
                self.log.warning(f"Removing: {values_to_remove}")
                tomo_params.input_file_list.remove(tomo_params.input_file_list[index])

        self.alignment_output_dir = str(Path(tomo_params.stack_file).parent)
        self.stack_name = str(Path(tomo_params.stack_file).stem)

        tomo_params.aretomo_output_file = self.stack_name + "_aretomo.mrc"
        self.aretomo_output_path = (
            self.alignment_output_dir + "/" + tomo_params.aretomo_output_file
        )
        self.plot_file = self.stack_name + "_xy_shift_plot.json"
        self.plot_path = self.alignment_output_dir + "/" + self.plot_file
        self.dark_images_file = self.stack_name + "_DarkImgs.txt"
        self.xy_proj_file = self.stack_name + "_aretomo_projXY.jpeg"
        self.xz_proj_file = self.stack_name + "_aretomo_projXZ.jpeg"
        self.central_slice_file = self.stack_name + "_aretomo_thumbnail.jpeg"
        self.tomogram_movie_file = self.stack_name + "_aretomo_movie.png"
        self.newstack_path = (
            self.alignment_output_dir + "/" + self.stack_name + "_newstack.txt"
        )

        newstack_result = self.newstack(tomo_params)
        if newstack_result.returncode:
            self.log.error(
                f"Newstack failed with exitcode {newstack_result.returncode}:\n"
                + newstack_result.stderr.decode("utf8", "replace")
            )
            rw.transport.nack(header)
            return

        tomo_params.position = str(Path(tomo_params.input_file_list[0][0]).name).split(
            "_"
        )[1]

        p = Path(self.plot_path)
        if p.is_file():
            p.chmod(0o740)

        d = Path(self.dark_images_file)
        if d.is_file():
            d.chmod(0o740)

        if tomo_params.out_imod:
            self.imod_directory = (
                self.alignment_output_dir + "/" + self.stack_name + "_aretomo_Imod"
            )

        aretomo_result = self.aretomo(tomo_params)

        if not aretomo_result:
            rw.send("tomo_align", message)

        if aretomo_result.returncode:
            self.log.error(
                f"AreTomo failed with exitcode {aretomo_result.returncode}:\n"
                + aretomo_result.stderr.decode("utf8", "replace")
            )
            # Update failure processing status
            if isinstance(rw, RW_mock):
                rw.transport.send(
                    destination="failure",
                    message="",
                )
            else:
                rw.send_to(
                    "failure",
                    "",
                )
            rw.transport.nack(header)
            return

        if tomo_params.out_imod:
            start_time = time.time()
            while not Path(self.imod_directory).is_dir():
                time.sleep(30)
                elapsed = time.time() - start_time
                if elapsed > 600:
                    self.log.warning("Timeout waiting for Imod directory")
                    break
            else:
                _f = Path(self.imod_directory)
                _f.chmod(0o750)
                for file in _f.iterdir():
                    file.chmod(0o740)

        # Extract results for ispyb

        # XY shift plot
        # Autoproc program attachment - plot
        self.extract_from_aln(tomo_params)
        if tomo_params.tilt_cor:
            try:
                self.rot_centre_z = self.rot_centre_z_list[-1]
            except IndexError:
                self.log.warning(f"No rot Z {self.rot_centre_z_list}")

        if tomo_params.pix_size:
            pix_spacing: str | None = str(tomo_params.pix_size * tomo_params.out_bin)
        else:
            pix_spacing = None
        # Forward results to ispyb

        # Tomogram (one per-tilt-series)
        ispyb_command_list = [
            {
                "ispyb_command": "insert_tomogram",
                "volume_file": tomo_params.aretomo_output_file,
                "stack_file": tomo_params.stack_file,
                "size_x": None,  # volume image size, pix
                "size_y": None,
                "size_z": None,
                "pixel_spacing": pix_spacing,
                "tilt_angle_offset": str(self.tilt_offset),
                "z_shift": self.rot_centre_z,
                "file_directory": self.alignment_output_dir,
                "central_slice_image": self.central_slice_file,
                "tomogram_movie": self.tomogram_movie_file,
                "xy_shift_plot": self.plot_file,
                "proj_xy": self.xy_proj_file,
                "proj_xz": self.xz_proj_file,
                "store_result": "ispyb_tomogram_id",
            }
        ]

        missing_indices = []
        if Path(self.dark_images_file).is_file():
            with open(self.dark_images_file) as f:
                missing_indices = [int(i) for i in f.readlines()[2:]]
        elif self.imod_directory and Path(self.imod_directory).is_dir():
            self.dark_images_file = str(Path(self.imod_directory) / "tilt.com")
            with open(self.dark_images_file) as f:
                lines = f.readlines()
                for line in lines:
                    if line.startswith("EXCLUDELIST"):
                        numbers = line.split(" ")
                        missing_indices = [
                            int(item.replace(",", "").strip()) for item in numbers[1:]
                        ]

        im_diff = 0
        # TiltImageAlignment (one per movie)
        for im, movie in enumerate(tomo_params.input_file_list):
            if im in missing_indices:
                im_diff += 1
            else:
                try:
                    ispyb_command_list.append(
                        {
                            "ispyb_command": "insert_tilt_image_alignment",
                            "psd_file": None,  # should be in ctf table but useful, so we will insert
                            "refined_magnification": str(self.mag),
                            "refined_tilt_angle": str(self.refined_tilts[im - im_diff])
                            if self.refined_tilts
                            else None,
                            "refined_tilt_axis": str(self.rot),
                            "path": movie[0],
                        }
                    )
                except IndexError as e:
                    self.log.error(
                        f"{e} - Dark images haven't been accounted for properly"
                    )

        ispyb_parameters = {
            "ispyb_command": "multipart_message",
            "ispyb_command_list": ispyb_command_list,
        }
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

        # Forward results to images service
        self.log.info(f"Sending to images service {self.aretomo_output_path}")
        if isinstance(rw, RW_mock):
            rw.transport.send(
                destination="images",
                message={
                    "parameters": {"images_command": "mrc_central_slice"},
                    "file": self.aretomo_output_path,
                },
            )
            rw.transport.send(
                destination="movie",
                message={
                    "parameters": {"images_command": "mrc_to_apng"},
                    "file": self.aretomo_output_path,
                },
            )
        else:
            rw.send_to(
                "images",
                {
                    "parameters": {"images_command": "mrc_central_slice"},
                    "file": self.aretomo_output_path,
                },
            )
            rw.send_to(
                "movie",
                {
                    "parameters": {"images_command": "mrc_to_apng"},
                    "file": self.aretomo_output_path,
                },
            )
        xy_input = (
            self.alignment_output_dir
            + "/"
            + str(Path(self.xy_proj_file).with_suffix(".mrc"))
        )
        xz_input = (
            self.alignment_output_dir
            + "/"
            + str(Path(self.xz_proj_file).with_suffix(".mrc"))
        )
        self.log.info(f"Sending to images service {xy_input}, {xz_input}")
        if isinstance(rw, RW_mock):
            rw.transport.send(
                destination="projxy",
                message={
                    "parameters": {"images_command": "mrc_to_jpeg"},
                    "file": xy_input,
                },
            )
            rw.transport.send(
                destination="projxz",
                message={
                    "parameters": {"images_command": "mrc_to_jpeg"},
                    "file": xz_input,
                },
            )
        else:
            rw.send_to(
                "projxy",
                {
                    "parameters": {"images_command": "mrc_to_jpeg"},
                    "file": xy_input,
                },
            )
            rw.send_to(
                "projxz",
                {
                    "parameters": {"images_command": "mrc_to_jpeg"},
                    "file": xz_input,
                },
            )

        # Forward results to denoise service
        self.log.info(f"Sending to denoise service {self.aretomo_output_path}")
        if isinstance(rw, RW_mock):
            rw.transport.send(
                destination="denoise",
                message={
                    "volume": self.aretomo_output_path,
                },
            )
        else:
            rw.send_to(
                "denoise",
                {
                    "volume": self.aretomo_output_path,
                },
            )

        # Update success processing status
        if isinstance(rw, RW_mock):
            rw.transport.send(
                destination="success",
                message="",
            )
        else:
            rw.send_to(
                "success",
                "",
            )
        rw.transport.ack(header)

    def newstack(self, tomo_parameters):
        """
        Construct file containing a list of files
        Run newstack
        """

        # Write a file with a list of .mrcs for input to Newstack
        with open(self.newstack_path, "w") as f:
            f.write(f"{len(tomo_parameters.input_file_list)}\n")
            f.write("\n0\n".join(i[0] for i in tomo_parameters.input_file_list))
            f.write("\n0\n")

        newstack_cmd = [
            "newstack",
            "-fileinlist",
            self.newstack_path,
            "-output",
            tomo_parameters.stack_file,
            "-quiet",
        ]
        self.log.info("Running Newstack")
        result = procrunner.run(newstack_cmd)
        return result

    def aretomo(self, tomo_parameters):
        """
        Run AreTomo on output of Newstack
        """
        command = ["AreTomo", "-OutMrc", self.aretomo_output_path]

        if tomo_parameters.angle_file:
            command.extend(("-AngFile", tomo_parameters.angle_file))
        else:
            command.extend(
                (
                    "-TiltRange",
                    tomo_parameters.input_file_list[0][1],  # lowest tilt
                    tomo_parameters.input_file_list[-1][1],
                )
            )  # highest tilt

        if tomo_parameters.manual_tilt_offset:
            command.extend(
                (
                    "-TiltCor",
                    str(tomo_parameters.tilt_cor),
                    str(tomo_parameters.manual_tilt_offset),
                )
            )
        elif tomo_parameters.tilt_cor:
            command.extend(("-TiltCor", str(tomo_parameters.tilt_cor)))

        aretomo_flags = {
            "stack_file": "-InMrc",
            "vol_z": "-VolZ",
            "out_bin": "-OutBin",
            "tilt_axis": "-TiltAxis",
            "flip_int": "-FlipInt",
            "flip_vol": "-FlipVol",
            "wbp": "-Wbp",
            "align": "-Align",
            "roi_file": "-RoiFile",
            "patch": "-Patch",
            "kv": "-Kv",
            "align_file": "-AlnFile",
            "align_z": "-AlignZ",
            "pix_size": "-PixSize",
            "init_val": "initVal",
            "refine_flag": "refineFlag",
            "out_imod": "-OutImod",
            "out_imod_xf": "-OutXf",
            "dark_tol": "-DarkTol",
        }

        for k, v in tomo_parameters.dict().items():
            if v and (k in aretomo_flags):
                command.extend((aretomo_flags[k], str(v)))

        self.log.info(f"Running AreTomo {command}")
        self.log.info(
            f"Input stack: {tomo_parameters.stack_file} \nOutput file: {self.aretomo_output_path}"
        )
        if tomo_parameters.tilt_cor:
            callback = self.parse_tomo_output
        else:
            callback = None
        result = procrunner.run(command=command, callback_stdout=callback)
        return result

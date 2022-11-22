from __future__ import annotations

import ast
from pathlib import Path

import plotly.express as px
import procrunner
import workflows.recipe
from pydantic import BaseModel, Field, validator
from pydantic.error_wrappers import ValidationError
from workflows.services.common_service import CommonService

# Possible parameters:
# "input_file_list" Required
# "stack_file" Required
# "vol_z" default 1200
# "align"
# "out_bin" default 4
# "tilt_range" calculatable or "ang_file"
# "tilt_axis"
# "tilt_cor"
# "flip_int"
# "flip_vol"
# "wbp"
# "roi_file"
# "patch"
# "kv"
# "align_file"
# "angle_file"
# "align_z"
# "pix_size"
# "init_val"
# "refine_flag"
# "out_imod"
# "out_imod_xf"
# "dark_tol"


class TomoParameters(BaseModel):
    input_file_list: str
    stack_file: str = Field(..., min_length=1)
    position: str = None
    aretomo_output_file: str = None
    vol_z: int = 1200
    align: int = None
    out_bin: int = 4
    tilt_axis: float = None
    tilt_cor: int = None
    flip_int: int = None
    flip_vol: int = 1
    wbp: int = None
    roi_file: list = None
    patch: int = None
    kv: int = None
    align_file: str = None
    angle_file: str = None
    align_z: int = None
    pix_size: int = None
    init_val: int = None
    refine_flag: int = None
    out_imod: int = 1
    out_imod_xf: int = None
    dark_tol: int or str = None

    @validator("input_file_list")
    def convert_to_list_of_lists(cls, v):
        file_list = ast.literal_eval(v)
        if isinstance(file_list, list) and isinstance(file_list[0], list):
            return file_list
        else:
            raise ValueError("input_file_list is not a list of lists")


class TomoAlign(CommonService):
    """
    A service for grouping and aligning tomography tilt-series with Newstack and AreTomo
    """

    # Required parameters: list of lists with filename, tilt and uuid, stack output file name (output file name will be used for both stages)

    # Human readable service name
    _service_name = "DLS TomoAlign"

    # Logger name
    _logger_name = "dlstbx.services.tomo_align"

    # Values to extract for ISPyB
    refined_tilts = None
    tilt_offset = None
    rot_centre_z_list = []
    rot_centre_z = None
    rot = None
    mag = None
    plot_path = None
    central_slice_location = None
    dark_images_file = None
    imod_directory = None

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
        if line.startswith("Rotation centre"):
            self.rot_centre_z_list.append(line.split()[7])
        if line.startswith("Tilt offset"):
            self.tilt_offset = line.split()[2]

    def extract_from_aln(self, tomo_parameters):
        tomo_aln_file = None
        x_shift = []
        y_shift = []
        self.refined_tilts = []
        aln_files = list(Path(tomo_parameters.aretomo_output_file).parent.glob("*.aln"))

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
        except (ValidationError, TypeError) as e:
            self.log.warning(
                f"{e} TomoAlign parameter validation failed for message: {message} and recipe parameters: {rw.recipe_step.get('parameters', {})}"
            )
            rw.transport.nack(header)
            return

        def tilt(file_list):
            return float(file_list[1])

        tomo_params.input_file_list.sort(key=tilt)
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

        stack_file_root = str(Path(tomo_params.stack_file).with_suffix(""))
        tomo_params.aretomo_output_file = stack_file_root + "_aretomo.mrc"
        self.central_slice_location = (
            str(Path(tomo_params.aretomo_output_file).with_suffix(""))
            + "_thumbnail.jpeg"
        )
        self.plot_path = stack_file_root + "_xy_shift_plot.json"
        self.dark_images_file = stack_file_root + "_DarkImgs.txt"

        p = Path(self.plot_path)
        if p.is_file():
            p.chmod(0o740)

        d = Path(self.dark_images_file)
        if d.is_file():
            d.chmod(0o740)

        c = Path(self.central_slice_location)
        if c.is_file():
            c.chmod(0o740)

        aretomo_result = self.aretomo(tomo_params.aretomo_output_file, tomo_params)

        if aretomo_result.returncode:
            self.log.error(
                f"AreTomo failed with exitcode {aretomo_result.returncode}:\n"
                + aretomo_result.stderr.decode("utf8", "replace")
            )
            rw.transport.nack(header)
            return

        if tomo_params.out_imod and tomo_params.out_imod != 0:
            self.imod_directory = (
                str(Path(tomo_params.aretomo_output_file).with_suffix("")) + "_Imod"
            )
            f = Path(self.imod_directory)
            f.chmod(0o750)
            for file in f.iterdir():
                file.chmod(0o740)

        # Extract results for ispyb

        # XY shift plot
        # Autoproc program attachment - plot
        self.extract_from_aln(tomo_params)
        if tomo_params.tilt_cor:
            self.rot_centre_z = self.rot_centre_z_list[-1]

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
                "pixel_spacing": tomo_params.pix_size,
                "tilt_angle_offset": self.tilt_offset,
                "z_shift": self.rot_centre_z,
                "store_result": "ispyb_tomogram_id",
            }
        ]
        if self.plot_path:
            ispyb_command_list.append(
                {
                    "ispyb_command": "add_program_attachment",
                    "file_name": str(Path(self.plot_path).name),
                    "file_path": str(Path(self.plot_path).parent),
                    "file_type": "Graph",
                }
            )
        if self.central_slice_location:
            ispyb_command_list.append(
                {
                    "ispyb_command": "add_program_attachment",
                    "file_name": str(Path(self.central_slice_location).name),
                    "file_path": str(Path(self.central_slice_location).parent),
                    "file_type": "Result",
                }
            )

        missing_indices = []
        if Path(self.dark_images_file).is_file():
            with open(self.dark_images_file) as f:
                missing_indices = [int(i) for i in f.readlines()[2:]]
        elif Path(self.imod_directory).is_dir():
            self.dark_images_file = Path(self.imod_directory) / "tilt.com"
            with open(self.dark_images_file) as f:
                lines = f.readlines()
                for line in lines:
                    if line.startswith("EXCLUDELIST"):
                        numbers = line.split(" ")
                        missing_indices = [
                            item.replace(",", "").strip() for item in numbers[1:]
                        ]

        im_diff = 0
        # TiltImageAlignment (one per movie)
        for im, movie in enumerate(tomo_params.input_file_list):
            if im in missing_indices:
                im_diff += 1
            else:
                ispyb_command_list.append(
                    {
                        "ispyb_command": "insert_tilt_image_alignment",
                        "psd_file": None,  # should be in ctf table but useful, so we will insert
                        "refined_magnification": self.mag,
                        "refined_tilt_angle": self.refined_tilts[im - im_diff],
                        "refined_tilt_axis": self.rot,
                        "movie_id": movie[2],
                    }
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
        self.log.info(f"Sending to images service {tomo_params.aretomo_output_file}")
        if isinstance(rw, RW_mock):
            rw.transport.send(
                destination="images",
                message={
                    "parameters": {"images_command": "mrc_central_slice"},
                    "file": tomo_params.aretomo_output_file,
                },
            )
        else:
            rw.send_to(
                "images",
                {
                    "parameters": {"images_command": "mrc_central_slice"},
                    "file": tomo_params.aretomo_output_file,
                },
            )

        rw.transport.ack(header)

    def newstack(self, tomo_parameters):
        """
        Construct file containing a list of files
        Run newstack
        """

        # Write a file with a list of .mrcs for input to Newstack
        with open("newstack-fileinlist.txt", "w") as f:
            f.write(f"{len(tomo_parameters.input_file_list)}\n")
            f.write("\n0\n".join(i[0] for i in tomo_parameters.input_file_list))
            f.write("\n0\n")

        newstack_cmd = [
            "newstack",
            "-fileinlist",
            "newstack-fileinlist.txt",
            "-output",
            tomo_parameters.stack_file,
            "-quiet",
        ]
        self.log.info("Running Newstack")
        result = procrunner.run(newstack_cmd)
        return result

    def aretomo(self, output_file, tomo_parameters):
        """
        Run AreTomo on output of Newstack
        """
        command = ["AreTomo", "-OutMrc", output_file]

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

        aretomo_flags = {
            "stack_file": "-InMrc",
            "vol_z": "-VolZ",
            "out_bin": "-OutBin",
            "tilt_axis": "-TiltAxis",
            "tilt_cor": "-TiltCor",
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
            f"Input stack: {tomo_parameters.stack_file} \nOutput file: {output_file}"
        )
        if tomo_parameters.tilt_cor:
            callback = self.parse_tomo_output
        else:
            callback = None
        result = procrunner.run(command=command, callback_stdout=callback)
        return result

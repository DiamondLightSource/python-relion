from __future__ import annotations

import logging
import pathlib
import time
from pathlib import Path

import mrcfile
import numpy as np
import PIL.Image
from PIL import ImageDraw, ImageEnhance, ImageFilter

logger = logging.getLogger("relion.zocalo.images_service_plugin")


def mrc_to_jpeg(plugin_params):
    filename = plugin_params.parameters("file")
    allframes = plugin_params.parameters("all_frames")
    send_to_ispyb = plugin_params.parameters("send_to_ispyb") or 0
    if not filename or filename == "None":
        logger.error("Skipping mrc to jpeg conversion: filename not specified")
        return False
    filepath = pathlib.Path(filename)
    if not filepath.is_file():
        logger.error(f"File {filepath} not found")
        return False
    start = time.perf_counter()
    try:
        with mrcfile.open(filepath) as mrc:
            data = mrc.data
    except ValueError:
        logger.error(
            f"File {filepath} could not be opened. It may be corrupted or not in mrc format"
        )
        return False
    outfile = filepath.with_suffix(".jpeg")
    outfiles = []
    if len(data.shape) == 2:
        mean = np.mean(data)
        sdev = np.std(data)
        sigma_min = mean - 3 * sdev
        sigma_max = mean + 3 * sdev
        data = np.ndarray.copy(data)
        data[data < sigma_min] = sigma_min
        data[data > sigma_max] = sigma_max
        data = data - data.min()
        data = data * 255 / data.max()
        data = data.astype("uint8")
        im = PIL.Image.fromarray(data, mode="L")
        try:
            im.save(outfile)
        except FileNotFoundError:
            logger.error(
                f"Trying to save to file {outfile} but directory does not exist"
            )
            return False
    elif len(data.shape) == 3:
        if allframes:
            for i, frame in enumerate(data):
                frame = frame - frame.min()
                frame = frame * 255 / frame.max()
                frame = frame.astype("uint8")
                im = PIL.Image.fromarray(frame, mode="L")
                frame_outfile = str(outfile).replace(".jpeg", f"_{i+1}.jpeg")
                try:
                    im.save(frame_outfile)
                except FileNotFoundError:
                    logger.error(
                        f"Trying to save to file {frame_outfile} but directory does not exist"
                    )
                    return False
                outfiles.append(frame_outfile)
        else:
            data = data - data[0].min()
            data = data * 255 / data[0].max()
            data = data.astype("uint8")
            im = PIL.Image.fromarray(data[0], mode="L")
            try:
                im.save(outfile)
            except FileNotFoundError:
                logger.error(
                    f"Trying to save to file {outfile} but directory does not exist"
                )
                return False
    timing = time.perf_counter() - start

    logger.info(
        f"Converted mrc to jpeg {filename} -> {outfile} in {timing:.1f} seconds",
        extra={"image-processing-time": timing},
    )
    if send_to_ispyb:
        logger.info("Sending to ISPyB")
        ispyb_command = {
            "ispyb_command": "add_program_attachment",
            "file_name": str(Path(outfile).name),
            "file_path": str(Path(outfile).parent),
        }

        try:
            plugin_params.rw.send_to(
                "ispyb",
                ispyb_command,
            )
        except Exception as e:
            logger.warning(f"{e}")
            return False
    if outfiles:
        return outfiles
    return outfile


def picked_particles(plugin_params):
    basefilename = plugin_params.parameters("file")
    if basefilename.endswith(".jpeg"):
        logger.info(f"Replacing jpeg extension with mrc extension for {basefilename}")
        basefilename = basefilename.replace(".jpeg", ".mrc")
    coords = plugin_params.parameters("coordinates")
    if not coords:
        logger.warning(f"No coordinates provided for {basefilename}")
        # If there were no coordinates don't bother nacking the message
        return True
    angpix = plugin_params.parameters("angpix")
    diam = plugin_params.parameters("diameter")
    contrast_factor = plugin_params.parameters("contrast_factor", default=6)
    outfile = plugin_params.parameters("outfile")
    if not outfile:
        logger.error(f"Outfile incorrectly specified: {outfile}")
        return False
    if not pathlib.Path(basefilename).is_file():
        logger.error(f"File {basefilename} not found")
        return False
    radius = (diam / angpix) // 2
    start = time.perf_counter()
    try:
        with mrcfile.open(basefilename) as mrc:
            data = mrc.data
    except ValueError:
        logger.error(
            f"File {basefilename} could not be opened. It may be corrupted or not in mrc format"
        )
        return False
    except FileNotFoundError:
        logger.error(f"File {basefilename} could not be opened")
        return False
    mean = np.mean(data)
    sdev = np.std(data)
    sigma_min = mean - 3 * sdev
    sigma_max = mean + 3 * sdev
    data = np.ndarray.copy(data)
    data[data < sigma_min] = sigma_min
    data[data > sigma_max] = sigma_max
    data = data - data.min()
    data = data * 255 / data.max()
    data = data.astype("uint8")
    with PIL.Image.fromarray(data).convert(mode="RGB") as bim:
        enhancer = ImageEnhance.Contrast(bim)
        enhanced = enhancer.enhance(contrast_factor)
        fim = enhanced.filter(ImageFilter.BLUR)
        dim = ImageDraw.Draw(fim)
        for x, y in coords:
            dim.ellipse(
                [
                    (float(x) - radius, float(y) - radius),
                    (float(x) + radius, float(y) + radius),
                ],
                width=8,
                outline="#f58a07",
            )
        try:
            fim.save(outfile)
        except FileNotFoundError:
            logger.error(
                f"Trying to save to file {outfile} but directory does not exist"
            )
            return False
    timing = time.perf_counter() - start
    logger.info(
        f"Particle picker image {outfile} saved in {timing:.1f} seconds",
        extra={"image-processing-time": timing},
    )
    return outfile


def mrc_central_slice(plugin_params):
    filename = plugin_params.parameters("file")
    rw = plugin_params.rw
    if not filename or filename == "None":
        logger.error("Skipping mrc to jpeg conversion: filename not specified")
        return False
    filepath = pathlib.Path(filename)
    if not filepath.is_file():
        logger.error(f"File {filepath} not found")
        return False
    start = time.perf_counter()
    try:
        with mrcfile.open(filepath) as mrc:
            data = mrc.data
    except ValueError:
        logger.error(
            f"File {filepath} could not be opened. It may be corrupted or not in mrc format"
        )
        return False
    outfile = str(filepath.with_suffix("")) + "_thumbnail.jpeg"
    if len(data.shape) != 3:
        logger.error(
            f"File {filepath} is not 3-dimensional. Cannot extract central slice"
        )
        return False

    # Extract central slice
    total_slices = data.shape[0]
    central_slice_index = int(total_slices / 2)
    central_slice_data = data[central_slice_index, :, :]

    # Write as jpeg
    central_slice_data = central_slice_data - central_slice_data[0].min()
    central_slice_data = central_slice_data * 255 / central_slice_data[0].max()
    central_slice_data = central_slice_data.astype("uint8")
    im = PIL.Image.fromarray(central_slice_data, mode="L")
    im.thumbnail((512, 512))
    try:
        im.save(outfile)
    except FileNotFoundError:
        logger.error(f"Trying to save to file {outfile} but directory does not exist")
        return False
    timing = time.perf_counter() - start

    logger.info(
        f"Converted mrc to jpeg {filename} -> {outfile} in {timing:.1f} seconds",
        extra={"image-processing-time": timing},
    )

    logger.info("Sending to ISPyB")
    ispyb_command = {
        "ispyb_command": "add_program_attachment",
        "file_name": str(Path(outfile).name),
        "file_path": str(Path(outfile).parent),
    }

    class RW_mock:
        def dummy(self, *args, **kwargs):
            pass

    if not rw:
        rw = RW_mock()
        rw.send = rw.dummy

    if isinstance(rw, RW_mock):
        rw.send("ispyb_connector", ispyb_command)
    else:
        rw.send_to(
            "ispyb",
            ispyb_command,
        )

    return outfile

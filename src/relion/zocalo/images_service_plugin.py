import logging
import pathlib

import mrcfile
import numpy as np
import PIL.Image
from PIL import ImageDraw, ImageEnhance, ImageFilter

logger = logging.getLogger("relion.zocalo.images_service_plugin")


def mrc_to_jpeg(plugin_params):
    filename = plugin_params.parameters("file")
    allframes = plugin_params.parameters("all_frames")
    if not filename or filename == "None":
        logger.debug("Skipping mrc to jpeg conversion: filename not specified")
        return None
    filepath = pathlib.Path(filename)
    if not filepath.is_file():
        logger.error(f"File {filepath} not found")
        return None
    try:
        with mrcfile.open(filepath) as mrc:
            data = mrc.data
    except ValueError:
        logger.error(
            "File {filepath} could not be opened. It may be corrupted or not in mrc format"
        )
        return None
    data = data - data.min()
    data = data * 255 / data.max()
    data = data.astype(np.uint8)
    outfile = filepath.with_suffix(".jpeg")
    outfiles = []
    if len(data.shape) == 2:
        im = PIL.Image.fromarray(data, mode="L")
        im.save(outfile)
    elif len(data.shape) == 3:
        if allframes:
            for i, frame in enumerate(data):
                im = PIL.Image.fromarray(frame, mode="L")
                frame_outfile = str(outfile).replace(".jpeg", f"_{i+1}.jpeg")
                im.save(frame_outfile)
                outfiles.append(frame_outfile)
        else:
            im = PIL.Image.fromarray(data[0], mode="L")
            im.save(outfile)

    logger.info(f"Converted mrc to jpeg {filename} -> {outfile}")
    if outfiles:
        return outfiles
    return outfile


def picked_particles(plugin_params):
    basefilename = plugin_params.parameters("file")
    coords = plugin_params.parameters("coordinates")
    angpix = plugin_params.parameters("angpix")
    diam = plugin_params.parameters("diameter")
    contrast_factor = plugin_params.parameters("contrast_factor", default=6)
    outfile = plugin_params.parameters("outfile")
    if not outfile:
        logger.warning(f"Outfile incorrectly specified: {outfile}")
        return None
    if not pathlib.Path(basefilename).is_file():
        logger.error(f"File {basefilename} not found")
        return None
    radius = (diam / angpix) // 2
    try:
        with PIL.Image.open(basefilename).convert(mode="RGB") as bim:
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
            fim.save(outfile)
            logger.info(f"Particle picker image {outfile} saved")
    except FileNotFoundError:
        logger.error(f"File {basefilename} could not be opened")
    return outfile

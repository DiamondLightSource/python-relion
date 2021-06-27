import logging
import pathlib

import mrcfile
import numpy as np
import PIL.Image

logger = logging.getLogger("relion.zocalo.images_service_plugin")


def mrc_to_jpeg(plugin_params):
    filename = plugin_params.parameters("file")
    allframes = plugin_params.parameters("all_frames")
    if not filename or filename == "None":
        logger.debug("Skipping mrc to jpeg conversion: filename not specified")
        return
    filepath = pathlib.Path(filename)
    if not filepath.is_file():
        logger.error(f"File {filepath} not found")
        return
    try:
        with mrcfile.open(filepath) as mrc:
            data = mrc.data
    except ValueError:
        logger.error(
            "File {filepath} could not be opened. It may be corrupted or not in mrc format"
        )
        return
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

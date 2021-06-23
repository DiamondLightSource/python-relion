import mrcfile
import PIL.Image
import pathlib
import numpy as np
import logging

logger = logging.getLogger("relion.zocalo.images_service_plugin")


def mrc_to_jpeg(params):
    filename = params.rw.recipe_step.get("parameters", {}).get("file")
    allframes = params.rw.recipe_step.get("parameters", {}).get("all_frames")
    if isinstance(params.message, dict) and params.message.get("file"):
        filename = params.message["file"]
        allframes = params.message.get("all_frames")
    else:
        filename = None
    if not filename or filename == "None":
        logger.debug("Skipping mrc to jpeg conversion: filename not specified")
        params.rw.transport.ack(params.header)
        return
    filepath = pathlib.Path(filename)
    if not filepath.is_file():
        logger.error(f"File {filepath} not found")
        params.rw.transport.nack(params.header)
        return
    try:
        with mrcfile.open(filepath) as mrc:
            data = mrc.data
    except ValueError:
        logger.error(
            "File {filepath} could not be opened. It may be corrupted or not in mrc format"
        )
        params.rw.transport.nack(params.header)
        return
    data = data - data.min()
    data = data * 255 / data.max()
    data = data.astype(np.uint8)
    outfile = filepath.with_suffix(".jpeg")
    if len(data.shape) == 2:
        im = PIL.Image.fromarray(data, mode="L")
        im.save(outfile)
    elif len(data.shape) == 3:
        if allframes:
            for i, frame in enumerate(data):
                im = PIL.Image.fromarray(frame, mode="L")
                frame_outfile = str(outfile).replace(".jpeg", f"_{i+1}.jpeg")
                im.save(frame_outfile)
        else:
            im = PIL.Image.fromarray(data[0], mode="L")
            im.save(outfile)

    logger.info(f"Converted mrc to jpeg {filename} -> {outfile}")
    params.rw.transport.ack(params.header)

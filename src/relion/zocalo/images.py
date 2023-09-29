from __future__ import annotations

import errno
import logging
import os
import re
import subprocess
import time
from typing import Any, Callable, Dict, NamedTuple, Protocol

import PIL.Image
import workflows.recipe
from importlib_metadata import entry_points
from workflows.services.common_service import CommonService

logger = logging.getLogger("relion.zocalo.images")


class _CallableParameter(Protocol):
    def __call__(self, key: str, default: Any = ...) -> Any:
        ...


class PluginInterface(NamedTuple):
    rw: workflows.recipe.wrapper.RecipeWrapper
    parameters: _CallableParameter
    message: Dict[str, Any]


class Images(CommonService):
    """
    A service that generates images and thumbnails.
    Plugin functions can be registered under the entry point
    'zocalo.services.images.plugins'. The contract is that a plugin function
    takes a single argument of type PluginInterface, and returns a truthy value
    to acknowledge success, and a falsy value to reject the related message.
    If a falsy value is returned that is not False then, additionally, an error
    is logged.
    Functions may choose to return a list of files that were generated, but
    this is optional at this time.
    """

    # Human readable service name
    _service_name = "Images"

    # Logger name
    _logger_name = "relion.zocalo.images"

    # Dictionary to contain functions from plugins
    image_functions: dict[str, Callable] = {}

    def initializing(self):
        """Subscribe to a queue. Received messages must be acknowledged."""
        self.log.info("Image service starting")
        self.image_functions.update(
            {
                e.name: e.load()
                for e in entry_points(group="zocalo.services.images.plugins")
            }
        )
        workflows.recipe.wrap_subscribe(
            self._transport,
            "images",
            self.image_call,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    def image_call(self, rw, header, message):
        """Pass incoming message to the relevant plugin function."""

        def parameters(key: str, default=None):
            if isinstance(message, dict) and message.get(key):
                return message[key]
            return rw.recipe_step.get("parameters", {}).get(key, default)

        command = parameters("image_command")
        if command not in self.image_functions:
            self.log.error(f"Unknown command: {command!r}")
            rw.transport.nack(header)
            return

        start = time.perf_counter()
        try:
            result = self.image_functions[command](
                PluginInterface(rw, parameters, message)
            )
        except (PermissionError, FileNotFoundError) as e:
            self.log.error(f"Command {command!r} raised {e}", exc_info=True)
            rw.transport.nack(header)
            return
        runtime = time.perf_counter() - start

        if result:
            self.log.info(f"Command {command!r} completed in {runtime:.1f} seconds")
            rw.transport.ack(header)
        elif result is False:
            # The assumption here is that if a function returns explicit
            # 'False' then it has already taken care of logging, so we
            # don't need yet another log record.
            rw.transport.nack(header)
        else:
            self.log.error(
                f"Command {command!r} returned {result!r} after {runtime:.1f} seconds"
            )
            rw.transport.nack(header)


def diffraction(plugin: PluginInterface):
    """Take a diffraction data file and transform it into JPEGs."""
    filename = plugin.parameters("file")

    imageset_index = 1
    if not filename:
        # 'file' is a filename
        # 'input' is a xia2-type string, may need to remove :x:x suffix
        filename = plugin.parameters("input")
        if ":" in filename:
            filename, imageset_index = filename.split(":")[0:2]

    if not filename or filename == "None":
        logger.debug("Skipping diffraction JPG generation: filename not specified")
        return False
    if not os.path.exists(filename):
        logger.error("File %s not found", filename)
        return False
    sizex = plugin.parameters("size-x", default=400)
    sizey = plugin.parameters("size-y", default=192)
    output = plugin.parameters("output")
    if not output:
        # split off extension
        output = filename[: filename.rindex(".")]
        # deduct image filename
        output = re.sub(
            r"(/[a-z]{2}[0-9]{4,}-[0-9]+/)", r"\g<0>jpegs/", output, count=1
        )
        output = output + ".jpeg"
        # create directory for image if necessary
        try:
            os.makedirs(os.path.dirname(output))
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
    output_small = output[: output.rindex(".")] + ".thumb.jpeg"

    start = time.perf_counter()
    result = subprocess.run(
        [
            "dials.export_bitmaps",
            filename,
            "imageset_index=%s" % imageset_index,
            "output.format=jpeg",
            "quality=95",
            "binning=4",
            "brightness=60",
            'output.file="%s"' % output,
        ]
    )
    export = time.perf_counter()
    if result.returncode:
        logger.error(
            f"Export of {filename} failed with exitcode {result.returncode}:\n"
            + result.stderr.decode("utf8", "replace")
        )
        return False
    if not os.path.exists(output):
        logger.error("Output file %s not found", output)
        return False
    with PIL.Image.open(output) as fh:
        fh.thumbnail((sizex, sizey))
        fh.save(output_small)
    done = time.perf_counter()

    logger.info(
        "Created thumbnail %s -> %s (%.1f sec) -> %s (%.1f sec)",
        filename,
        output,
        export - start,
        output_small,
        done - export,
    )
    return [output, output_small]


def thumbnail(plugin: PluginInterface):
    """Take a single file and create a smaller version of the same file."""
    filename = plugin.parameters("file")
    if not filename or filename == "None":
        logger.debug("Skipping thumbnail generation: filename not specified")
        return False
    if not os.path.exists(filename):
        logger.error("File %s not found", filename)
        return False
    sizex = plugin.parameters("size-x", default=400)
    sizey = plugin.parameters("size-y", default=192)
    output = plugin.parameters("output")
    if not output:
        # If not set add a 't' in front of the last '.' in the filename
        output = (
            filename[: filename.rindex(".")] + "t" + filename[filename.rindex(".") :]
        )

    start = time.perf_counter()
    with PIL.Image.open(filename) as fh:
        fh.thumbnail((sizex, sizey))
        fh.save(output)
    timing = time.perf_counter() - start

    logger.info("Created thumbnail %s -> %s in %.1f seconds", filename, output, timing)
    return [output]

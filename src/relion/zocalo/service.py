import pathlib
import unicodedata

import workflows.recipe
from workflows.services.common_service import CommonService


class RelionStopService(CommonService):
    """A service that creates 'stop' files that interrupt Relion processing jobs."""

    # Human readable service name
    _service_name = "Relion Stop Service"

    # Logger name
    _logger_name = "relion.zocalo.service"

    def initializing(self):
        """Subscribe to the Relion Stop queue.
        Received messages must be acknowledged."""
        self.log.debug("Relion Stop service starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "relion.dev.stop",
            self.receive_msg,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    def receive_msg(self, rw, header, message):
        """Stop a Relion processing job."""
        parameters = rw.recipe_step["parameters"]
        stop_file = parameters.get("stop_file")
        if not stop_file:
            self.log.error("No stop file defined in message")
            self._transport.nack(header)
            return
        stop_file = pathlib.Path(
            "".join(
                ch for ch in stop_file if not unicodedata.category(ch).startswith("C")
            )
        )
        if not stop_file.is_absolute():
            self.log.error(f"Stop file path '{stop_file}' is not absolute")
            self._transport.nack(header)
            return
        if not stop_file.parent.is_dir():
            self.log.error(f"Parent of stop file path '{stop_file}' does not exist")
            self._transport.nack(header)
            return
        if stop_file.exists():
            self.log.info(f"Requested stop file '{stop_file}' already exists")
            self._transport.ack(header)
            return
        self.log.info(f"Creating stop file '{stop_file}'")
        pathlib.Path(stop_file).touch()
        self._transport.ack(header)

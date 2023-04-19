from __future__ import annotations

import string
from collections import ChainMap
from pathlib import Path
from typing import Optional

import procrunner
import workflows.recipe
from pydantic import BaseModel, Field, validator
from pydantic.error_wrappers import ValidationError
from workflows.services.common_service import CommonService


class DenoiseParameters(BaseModel):
    volume: str = Field(..., min_length=1)
    output: str = Field(..., min_length=1)
    suffix: str = ".denoised"
    model: str = "unet-3d"
    even_train_path: Optional[str] = None
    odd_train_path: Optional[str] = None
    n_train: int = 1000
    n_test: int = 200
    crop: int = 96
    base_kernel_width: int = 11
    optim: str = "adagrad"
    lr: float = 0.001
    criteria: str = "L2"
    momentum: float = 0.8
    batch_size: int = 10
    num_epochs: int = 500
    weight_decay: int = 0
    save_interval: int = 10
    num_workers: int = 1
    num_threads: int = 0
    gaussian: int = 0
    patch_size: int = 96
    patch_padding: int = 48
    device: int = -2

    @validator("model")
    def saved_models(cls, v):
        if v not in ["unet-3d-10a", "unet-3d-20a", "unet-3d"]:
            raise ValueError("Model must be one of unet-3d-10a, unet-3d-20a, unet-3d")
        return v

    @validator("optim")
    def optimizers(cls, v):
        if v not in ["adam", "adagrad", "sgd"]:
            raise ValueError("Optimizer must be one of adam, adagrad, sgd")
        return v

    @validator("criteria")
    def training_criteria(cls, v):
        if v not in ["L1", "L2"]:
            raise ValueError("Optimizer must be one of L1, L2")
        return v


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


class Denoise(CommonService):
    """
    A service for denoising cryoEM tomograms using Topaz
    """

    # Human readable service name
    _service_name = "EM Denoise"

    # Logger name
    _logger_name = "relion.zocalo.denoise"

    def initializing(self):
        """Subscribe to a queue. Received messages must be acknowledged."""
        self.log.info("Denoise service starting")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "denoise",
            self.denoise,
            acknowledgement=True,
            log_extender=self.extend_log,
            allow_non_recipe_messages=True,
        )

    def denoise(self, rw, header: dict, message: dict):
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

        command = ["topaz"]

        parameter_map = ChainMapWithReplacement(
            message if isinstance(message, dict) else {},
            rw.recipe_step["parameters"],
            substitutions=rw.environment,
        )

        try:
            if isinstance(message, dict):
                d_params = DenoiseParameters(**{**dict(parameter_map), **message})
            else:
                d_params = DenoiseParameters(**{**dict(parameter_map)})
        except (ValidationError, TypeError):
            self.log.warning(
                f"Denoise parameter validation failed for message: {message} and recipe parameters: {rw.recipe_step.get('parameters', {})}"
            )
            rw.transport.nack(header)
            return
        command.extend(d_params.volume)
        denoise_flags = {
            "output": "-o",
            "suffix": "--suffix",
            "model": "-m",
            "even_train_path": "-a",
            "odd_train_path": "-b",
            "n_train": "--N-train",
            "n_test": "--N-test",
            "crop": "-c",
            "base_kernel_width": "--base-kernel-width",
            "optim": "--optim",
            "lr": "--lr",
            "criteria": "--criteria",
            "momentum": "--momentum",
            "batch_size": "--batch-size",
            "num_epochs": "--num-epochs",
            "weight_decay": "-w",
            "save_interval": "--save-interval",
            "num_workers": "--save-prefix",
            "num_threads": "-j",
            "gaussian": "-g",
            "patch_size": "-s",
            "patch_padding": "-p",
            "device": "-d",
        }

        for k, v in d_params.dict().items():
            if v and (k in denoise_flags):
                if type(v) is tuple:
                    command.extend((denoise_flags[k], " ".join(str(_) for _ in v)))
                else:
                    command.extend((denoise_flags[k], str(v)))

        self.log.info(f"Input: {d_params.volume} Output: {d_params.output}")

        result = procrunner.run(command=command)
        if result.returncode:
            self.log.error(
                f"Denoising of {d_params.volume} failed with exitcode {result.returncode}:\n"
                + result.stderr.decode("utf8", "replace")
            )
            rw.transport.nack(header)
            return

        denoised_full_path = (
            d_params.output + str(Path(d_params.volume).stem) + ".denoised"
        )
        # Forward results to images service
        self.log.info(f"Sending to images service {d_params.mrc_out}")
        if isinstance(rw, RW_mock):
            rw.transport.send(
                destination="images",
                message={
                    "parameters": {"images_command": "mrc_to_jpeg"},
                    "file": denoised_full_path,
                },
            )
        else:
            rw.send_to(
                "images",
                {
                    "parameters": {"images_command": "mrc_to_jpeg"},
                    "file": denoised_full_path,
                },
            )

        rw.transport.ack(header)

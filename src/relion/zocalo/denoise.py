from __future__ import annotations

import string
import subprocess
import time
from collections import ChainMap
from pathlib import Path
from typing import Optional

import htcondor
import workflows.recipe
from pydantic import BaseModel, Field, validator
from pydantic.error_wrappers import ValidationError
from workflows.services.common_service import CommonService


class DenoiseParameters(BaseModel):
    volume: str = Field(..., min_length=1)
    output: Optional[str] = None  # volume directory
    suffix: Optional[str] = None  # ".denoised"
    model: Optional[str] = None  # "unet-3d"
    even_train_path: Optional[str] = None
    odd_train_path: Optional[str] = None
    n_train: Optional[int] = None  # 1000
    n_test: Optional[int] = None  # 200
    crop: Optional[int] = None  # 96
    base_kernel_width: Optional[int] = None  # 11
    optim: Optional[str] = None  # "adagrad"
    lr: Optional[float] = None  # 0.001
    criteria: Optional[str] = None  # "L2"
    momentum: Optional[float] = None  # 0.8
    batch_size: Optional[int] = None  # 10
    num_epochs: Optional[int] = None  # 500
    weight_decay: Optional[int] = None  # 0
    save_interval: Optional[int] = None  # 10
    save_prefix: Optional[str] = None
    num_workers: Optional[int] = None  # 1
    num_threads: Optional[int] = None  # 0
    gaussian: Optional[int] = None  # 0
    patch_size: Optional[int] = None  # 96
    patch_padding: Optional[int] = None  # 48
    device: Optional[int] = None  # -2

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

        try:
            if isinstance(message, dict):
                d_params = DenoiseParameters(
                    **{**rw.recipe_step.get("parameters", {}), **message}
                )
            else:
                d_params = DenoiseParameters(**{**rw.recipe_step.get("parameters", {})})
        except (ValidationError, TypeError) as e:
            self.log.warning(
                f"{e} Denoise parameter validation failed for message: {message} and recipe parameters: {rw.recipe_step.get('parameters', {})}"
            )
            rw.transport.nack(header)
            return

        command = ["topaz", "denoise3d", str(Path(d_params.volume).name)]

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
            "num_workers": "--num-workers",
            "save_prefix": "--save-prefix",
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

        suffix = str(Path(d_params.volume).suffix)
        alignment_output_dir = str(Path(d_params.volume).parent)
        denoised_file = str(Path(d_params.volume).stem) + ".denoised" + suffix
        denoised_full_path = str(Path(d_params.volume).parent) + "/" + denoised_file

        self.log.info(f"Running Topaz {command}")
        self.log.info(f"Input: {d_params.volume} Output: {denoised_full_path}")

        # Set-up condor config
        htcondor.param["RELEASE_DIR"] = "/usr"
        htcondor.param["LOCAL_DIR"] = "/var"
        htcondor.param["RUN"] = "/var/run/condor"
        htcondor.param["LOG"] = "/var/log/condor"
        htcondor.param["LOCK"] = "/var/lock/condor"
        htcondor.param["SPOOL"] = "/var/lib/condor/spool"
        htcondor.param["EXECUTE"] = "/var/lib/condor/execute"
        htcondor.param["BIN"] = "/usr/bin"
        htcondor.param["LIB"] = "/usr/lib64/condor"
        htcondor.param["INCLUDE"] = "/usr/include/condor"
        htcondor.param["SBIN"] = "/usr/sbin"
        htcondor.param["LIBEXEC"] = "/usr/libexec/condor"
        htcondor.param["SHARE"] = "/usr/share/condor"
        htcondor.param["PROCD_ADDRESS"] = "/var/run/condor/procd_pipe"
        htcondor.param[
            "JAVA_CLASSPATH_DEFAULT"
        ] = "/usr/share/condor /usr/share/condor/scimark2lib.jar ."
        htcondor.param["CONDOR_HOST"] = "pool-gpu-htcondor-manager.diamond.ac.uk"
        htcondor.param["COLLECTOR_HOST"] = "pool-gpu-htcondor-manager.diamond.ac.uk"
        htcondor.param["ALLOW_READ"] = "*"
        htcondor.param["ALLOW_WRITE"] = "*"
        htcondor.param["ALLOW_NEGOTIATOR"] = "*"
        htcondor.param["ALLOW_DAEMON"] = "*"
        htcondor.param["SEC_DEFAULT_AUTHENTICATION_METHODS"] = "FS_REMOTE, PASSWORD"
        htcondor.param[
            "SEC_WRITE_AUTHENTICATION_METHODS"
        ] = "FS_REMOTE, PASSWORD, ANONYMOUS"
        htcondor.param[
            "SEC_READ_AUTHENTICATION_METHODS"
        ] = "FS_REMOTE, PASSWORD, ANONYMOUS"
        htcondor.param["FS_REMOTE_DIR"] = "/dls/tmp/htcondor"

        output_file = str(Path(d_params.volume).with_suffix("")) + "_denoise_iris_out"
        error_file = str(Path(d_params.volume).with_suffix("")) + "_denoise_iris_error"
        log_file = str(Path(d_params.volume).with_suffix("")) + "_denoise_iris_log"
        self.log.info(f"Log file: {log_file}")

        try:
            at_job = htcondor.Submit(
                {
                    "executable": "/dls/ebic/data/staff-scratch/murfey/topaz.sh",
                    "arguments": "\"'$(input_args)'\"",  # needs to be in single quotes to be interpreted as one command
                    "output": "$(output_file)",
                    "error": "$(error_file)",
                    "log": "$(log_file)",
                    "request_gpus": "1",
                    "request_memory": "15000",
                    "request_disk": "10240",
                    "should_transfer_files": "yes",
                    "transfer_input_files": "$(volume)",
                    "transfer_output_files": "$(output_files)",
                    "initial_dir": "$(initial_dir)",
                }
            )
        except Exception:
            self.log.warn("Couldn't connect submitter")
            return None

        itemdata = [
            {
                "input_args": " ".join(command),
                "volume": d_params.volume,
                "output_file": output_file,
                "error_file": error_file,
                "log_file": log_file,
                "output_files": denoised_file,  # denoised_file,
                "initial_dir": alignment_output_dir,
            }
        ]

        coll = htcondor.Collector(htcondor.param["COLLECTOR_HOST"])
        schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd)
        schedd = htcondor.Schedd(schedd_ad)
        job = schedd.submit(at_job, itemdata=iter(itemdata))
        cluster_id = job.cluster()
        self.log.info(f"Submitting to Iris, ID: {str(cluster_id)}")

        res = 1
        while res:
            try:
                res = schedd.query(
                    constraint="ClusterId=={}".format(cluster_id),
                    projection=["JobStatus"],
                )[0]["JobStatus"]
            except IndexError:
                break
            if res == 12:
                schedd.act(htcondor.JobAction.Remove, f"ClusterId == {cluster_id}")
                return subprocess.CompletedProcess(args="", returncode=res)
            time.sleep(10)

        # Forward results to images service
        self.log.info(f"Sending to images service {d_params.volume}")
        if isinstance(rw, RW_mock):
            rw.transport.send(
                destination="images",
                message={
                    "parameters": {"images_command": "mrc_central_slice"},
                    "file": denoised_full_path,
                },
            )
            rw.transport.send(
                destination="movie",
                message={
                    "parameters": {"images_command": "mrc_to_apng"},
                    "file": denoised_full_path,
                },
            )
        else:
            rw.send_to(
                "images",
                {
                    "parameters": {"images_command": "mrc_central_slice"},
                    "file": denoised_full_path,
                },
            )
            rw.send_to(
                "movie",
                {
                    "parameters": {"images_command": "mrc_to_apng"},
                    "file": denoised_full_path,
                },
            )

        rw.transport.ack(header)
        return subprocess.CompletedProcess(args="", returncode=None)

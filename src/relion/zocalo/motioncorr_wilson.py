from __future__ import annotations

import json
import os
import string
import subprocess
import time
from collections import ChainMap
from math import hypot
from pathlib import Path

from workflows.services.common_service import CommonService

from relion.zocalo.motioncorr import MotionCorr

slurm_json_template = {
    "job": {
        "partition": "em,cs05r",
        "nodes": 1,
        "tasks": 4,
        "cpus_per_task": 10,
        "gpus": 4,
        "memory_per_gpu": 7000,
        "name": "MotionCorr",
        "time_limit": "72:00:00",
        "environment": {
            "PATH": "/bin:/usr/bin/:/usr/local/bin/",
            "LD_LIBRARY_PATH": "/lib/:/lib64/:/usr/local/lib",
            "MODULEPATH": "",
            "USER": "yxd92326",
            "HOME": "/home/yxd92326",
        },
        "prefer": "em_node",
    },
    "script": (
        "#!/bin/bash\n"
        "echo \"$(date '+%Y-%m-%d %H:%M:%S.%3N'): running MotionCorr2\"\n"
        "source /etc/profile.d/modules.sh\n"
        "module load EM/MotionCor2\n"
        "unset SLURM_JWT\n"
        "srun -n 4 "
    ),
}


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


class MotionCorrWilson(MotionCorr, CommonService):
    """
    A service for submitting MotionCor2 jobs to Wilson
    """

    # Logger name
    _logger_name = "relion.zocalo.motioncorr_wilson"

    def parse_mc_output(self, mc_output_file):
        """
        Read the output logs of MotionCorr to determine
        the movement of each frame
        """
        with open(mc_output_file, "r") as mc_file:
            lines = mc_file.readlines()
            frames_line = False
            for line in lines:
                # Frame reading in MotionCorr 1.4.0
                if line.startswith("...... Frame"):
                    line_split = line.split()
                    self.x_shift_list.append(float(line_split[-2]))
                    self.y_shift_list.append(float(line_split[-1]))
                    self.each_total_motion.append(
                        hypot(float(line_split[-2]), float(line_split[-1]))
                    )

                # Alternative frame reading for MotionCorr 1.6.3
                if not line:
                    frames_line = False
                if frames_line:
                    line_split = line.split()
                    self.x_shift_list.append(float(line_split[1]))
                    self.y_shift_list.append(float(line_split[2]))
                    self.each_total_motion.append(
                        hypot(float(line_split[1]), float(line_split[2]))
                    )
                if "x Shift" in line:
                    frames_line = True

    def motioncor2(self, command: list, mrc_out: Path):
        user = "yxd92326"  # "k8s-em"
        slurm_token = os.environ["SLURM_JWT"]

        mc_output_file = f"{mrc_out}.out"
        mc_error_file = f"{mrc_out}.err"
        submission_file = f"{mrc_out}.json"
        slurm_files = {
            "standard_output": mc_output_file,
            "standard_error": mc_error_file,
            "current_working_directory": str(mrc_out),
        }
        slurm_json_job = dict(slurm_json_template["job"], **slurm_files)
        slurm_json = {
            "job": slurm_json_job,
            "script": slurm_json_template["script"] + " ".join(command),
        }
        with open(submission_file, "w") as f:
            json.dump(slurm_json, f)

        slurm_submit = [
            "curl",
            "-d",
            f"@{submission_file}",
            "-H",
            f'"X-SLURM-USER-NAME:{user}"',
            "-H",
            f'"X-SLURM-USER-TOKEN:{slurm_token}"',
            "-H",
            '"Content-Type: application/json"',
            "-X",
            "POST",
            "https://slurm-rest.diamond.ac.uk:8443/slurm/v0.0.38/job/submit",
        ]
        print(" ".join(slurm_submit))
        slurm_submit = " ".join(slurm_submit)

        slurm_submission_json = subprocess.run(
            slurm_submit, capture_output=True, shell=True
        )
        print(slurm_submission_json)
        job_id = json.loads(slurm_submission_json.stdout.decode("utf8", "replace"))[
            "job_id"
        ]

        slurm_status = [
            "curl",
            "-H",
            f'"X-SLURM-USER-NAME:{user}"',
            "-H",
            f'"X-SLURM-USER-TOKEN:{slurm_token}"',
            "-H",
            '"Content-Type: application/json"',
            "-X",
            "GET",
            f"https://slurm-rest.diamond.ac.uk:8443/slurm/v0.0.38/job/{job_id}",
        ]
        slurm_status = " ".join(slurm_status)

        slurm_status_json = json.loads(
            subprocess.run(slurm_status, capture_output=True, shell=True).stdout.decode(
                "utf8", "replace"
            )
        )
        print("!!!!", slurm_status_json["jobs"]["job_state"])
        while slurm_status_json["jobs"]["job_state"] in (
            "PENDING",
            "CONFIGURING",
            "RUNNING",
            "COMPLETING",
        ):
            time.sleep(10)
            slurm_status_json = json.loads(
                subprocess.run(
                    slurm_status, capture_output=True, shell=True
                ).stdout.decode("utf8", "replace")
            )

        if slurm_status_json["jobs"]["job_state"] == "COMPLETED":
            self.parse_mc_output(mc_output_file)
            return subprocess.CompletedProcess(args="", returncode=0)
        else:
            return subprocess.CompletedProcess(args="", returncode=1)

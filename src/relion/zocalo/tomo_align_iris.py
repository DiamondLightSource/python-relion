from __future__ import annotations

import subprocess
import tarfile
import time
from pathlib import Path

import htcondor
from workflows.services.common_service import CommonService

from relion.zocalo.tomo_align import TomoAlign


class TomoAlignIris(TomoAlign, CommonService):
    """
    A service for grouping and aligning tomography tilt-series with Newstack and AreTomo
    """

    # Logger name
    _logger_name = "relion.zocalo.tomo_align_iris"

    def parse_tomo_output(self, tomo_output_file):
        tomo_file = open(tomo_output_file, "r")
        lines = tomo_file.readlines()
        for line in lines:
            if line.startswith("Rot center Z"):
                self.rot_centre_z_list.append(line.split()[5])
            if line.startswith("Tilt offset"):
                self.tilt_offset = float(line.split()[2].strip(","))
        tomo_file.close()

    def aretomo(self, tomo_parameters):
        """
        Run AreTomo on output of Newstack
        """
        args = [
            "./AreTomo_1.3.0_Cuda112_09292022",
            "-OutMrc",
            tomo_parameters.aretomo_output_file,
            "-InMrc",
            str(Path(tomo_parameters.stack_file).name),
        ]

        if tomo_parameters.angle_file:
            args.extend(("-AngFile", tomo_parameters.angle_file))
        else:
            args.extend(
                (
                    "-TiltRange",
                    tomo_parameters.input_file_list[0][1],  # lowest tilt
                    tomo_parameters.input_file_list[-1][1],
                )
            )  # highest tilt

        if tomo_parameters.manual_tilt_offset:
            args.extend(
                (
                    "-TiltCor",
                    str(tomo_parameters.tilt_cor),
                    str(tomo_parameters.manual_tilt_offset),
                )
            )
        elif tomo_parameters.tilt_cor:
            args.extend(("-TiltCor", str(tomo_parameters.tilt_cor)))

        aretomo_flags = {
            "vol_z": "-VolZ",
            "out_bin": "-OutBin",
            "tilt_axis": "-TiltAxis",
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
            "init_val": "-initVal",
            "refine_flag": "-refineFlag",
            "out_imod": "-OutImod",
            "out_imod_xf": "-OutXf",
            "dark_tol": "-DarkTol",
        }

        for k, v in tomo_parameters.dict().items():
            if v and (k in aretomo_flags):
                args.extend((aretomo_flags[k], str(v)))

        self.log.info(f"Running AreTomo with args: {args}")
        self.log.info(
            f"Input stack: {tomo_parameters.stack_file} \nOutput file: {tomo_parameters.aretomo_output_file}"
        )

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

        output_file = self.alignment_output_dir + "/" + self.stack_name + "_iris_out"
        error_file = self.alignment_output_dir + "/" + self.stack_name + "_iris_error"
        log_file = self.alignment_output_dir + "/" + self.stack_name + "_iris_log"

        try:
            at_job = htcondor.Submit(
                {
                    "executable": "/dls/ebic/data/staff-scratch/murfey/aretomo.sh",
                    "arguments": "\"'$(input_args)'\"",  # needs to be in single quotes to be interpreted as one command
                    "output": "$(output_file)",
                    "error": "$(error_file)",
                    "log": "$(log_file)",
                    "request_gpus": "1",
                    "request_memory": "10240",
                    "request_disk": "10240",
                    "should_transfer_files": "yes",
                    "transfer_input_files": "$(stack_file), /dls/ebic/data/staff-scratch/murfey/AreTomo_1.3.0_Cuda112_09292022",
                    "initialdir": "$(initial_dir)",
                }
            )
        except Exception:
            self.log.warn("Couldn't connect submitter")
            return None

        if tomo_parameters.out_imod:
            itemdata = [
                {
                    "input_args": " ".join(args),
                    "stack_file": tomo_parameters.stack_file,
                    "output_file": output_file,
                    "error_file": error_file,
                    "log_file": log_file,
                    "initial_dir": self.alignment_output_dir,
                }
            ]
        else:
            itemdata = [
                {
                    "input_args": " ".join(args),
                    "stack_file": tomo_parameters.stack_file,
                    "output_file": output_file,
                    "error_file": error_file,
                    "log_file": log_file,
                    "initial_dir": self.alignment_output_dir,
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

        if tomo_parameters.tilt_cor:
            self.parse_tomo_output(output_file)

        tar_imod_dir = str(Path(self.imod_directory).with_suffix(".tar.gz"))
        file = tarfile.open(tar_imod_dir)
        file.extractall(self.alignment_output_dir)
        file.close()

        return subprocess.CompletedProcess(args="", returncode=None)

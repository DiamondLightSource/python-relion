from __future__ import annotations

import argparse
import os

import yaml

from relion.cryolo_relion_it import dls_options
from relion.cryolo_relion_it.cryolo_relion_it import RelionItOptions


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-f",
        "--file",
        help="Print options to file. Default is relion_it_options.py. Extension should be .py",
        dest="options_file",
        default="relion_it_options.py",
    )
    parser.add_argument(
        "--gpus",
        help="Number of GPUs available on a local system",
        dest="gpus",
        type=int,
        default=0,
    )
    parser.add_argument(
        "--offline",
        help="Run the pipeline from a personal workstation",
        dest="offline",
        action="store_true",
    )
    args = parser.parse_args()
    opts = RelionItOptions()
    opts.update_from(vars(dls_options))
    if os.getenv("RELION_CLUSTER_CONFIG") and not args.offline:
        with open(os.getenv("RELION_CLUSTER_CONFIG"), "r") as config:
            cluster_config = yaml.safe_load(config)
        opts.update_from(cluster_config)
    if args.gpus:
        opts.motioncor_gpu = ":".join(str(i) for i in range(args.gpus))
        opts.motioncor_mpi = args.gpus
        opts.refine_gpu = ":".join(str(i) for i in range(args.gpus))
        opts.refine_mpi = args.gpus + 1
        opts.extract_mpi = 4
        opts.ctffind_mpi = 4
    opts.print_options(out_file=open(args.options_file, "w"))

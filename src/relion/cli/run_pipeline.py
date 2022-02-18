import argparse
import os
import pathlib

from relion.cryolo_relion_it import cryolo_relion_it, dls_options
from relion.cryolo_relion_it.cryolo_relion_it import RelionItOptions
from relion.pipeline import PipelineRunner


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--dir",
        help="Working directory where Relion will run",
        dest="working_directory",
        default=".",
    )
    parser.add_argument(
        "-p",
        "--param",
        help="Set a key, value pair of options as key=value",
        action="append",
        dest="params",
    )
    parser.add_argument(
        "-f", "--file", help="Load options from file", dest="options_file"
    )
    parser.add_argument(
        "-m",
        "--movies",
        help="Directory to the directory to make Movies symlink to",
        dest="movies_dir",
    )
    parser.add_argument(
        "--version",
        help="Relion version; options are 3.1 or 4",
        dest="version",
        type=float,
        default=3.1,
    )
    parser.add_argument(
        "--timeout",
        help="Stop importing movies if no new movies have been seen after this many seconds",
        dest="timeout",
        type=int,
        default=2 * 24 * 3600,
    )
    args = parser.parse_args()

    opts = RelionItOptions()
    opts.update_from(vars(dls_options))

    if args.options_file:
        opts.update_from_file(args.options_file)

    if args.params:
        params_list = [p.split("=") for p in args.params]
        params_dict = {p[0]: p[1] for p in params_list}
        for k, v in params_dict.items():
            if v.isnumeric():
                params_dict[k] = int(v)
            elif v.lower() == "true":
                params_dict[k] = True
            elif v.lower() == "false":
                params_dict[k] = False
            else:
                try:
                    params_dict[k] = float(v)
                except ValueError:
                    pass
    else:
        params_dict = {}

    opts.update_from(params_dict)
    options_file = pathlib.Path(args.working_directory) / cryolo_relion_it.OPTIONS_FILE
    if os.path.isfile(options_file):
        os.rename(options_file, f"{options_file}~")
    with open(options_file, "w") as optfile:
        opts.print_options(optfile)

    if args.movies_dir is not None:
        (pathlib.Path(args.working_directory) / "Movies").symlink_to(args.movies_dir)

    os.chdir(args.working_directory)

    if args.version == 3.1:
        cryolo_relion_it.run_pipeline(opts)
    elif args.version == 4:
        movietype = list(
            (pathlib.Path(args.working_directory) / "Movies").glob("**/*")
        )[0].suffix
        pipeline = PipelineRunner(
            pathlib.Path(args.working_directory),
            pathlib.Path(args.working_directory) / "stop.stop",
            opts,
            movietype=movietype,
        )
        pipeline.run(args.timeout)

from __future__ import annotations

import argparse
from pathlib import Path

import procrunner


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p",
        "--project",
        help="Path to original RELION project directory",
        dest="project",
    )
    parser.add_argument(
        "-d",
        "--destination",
        help="Path to directory where new project should be created",
        dest="destination",
        default=".",
    )
    parser.add_argument(
        "--hardlink-cutoff",
        type=int,
        help="Size in bytes past which a hardlink will be made to a file rather than copying",
        default=5e6,
    )
    args = parser.parse_args()

    required_dirs = (".Nodes", ".TMP_runfiles")

    ignore_file_extensions = {"Class2D": [".jpeg"]}

    if not Path(args.destination).exists():
        Path(args.destination).mkdir()

    for f in Path(args.project).glob("*"):
        if (f.is_dir() and f in required_dirs) or f.is_file():
            procrunner.run(
                [
                    "rsync",
                    "--recursive",
                    str(f),
                    f"{Path(args.destination) / f.relative_to(Path(args.project))}",
                ]
            )
        elif f.is_dir():
            job_type_dir = Path(args.destination) / f.relative_to(Path(args.project))
            job_type_dir.mkdir()
            for job_dir in f.glob("*"):
                if not job_dir.is_symlink():
                    new_job_dir = Path(args.destination) / job_dir.relative_to(
                        Path(args.project)
                    )
                    new_job_dir.mkdir()
                    for jf in job_dir.glob("*"):
                        if jf.is_file():
                            if jf.suffix in ignore_file_extensions.get(f.name, []):
                                continue
                            file_linked = False
                            if jf.stat().st_size > args.hardlink_cutoff:
                                try:
                                    jf.link_to(
                                        f"{Path(args.destination) / jf.relative_to(Path(args.project))}"
                                    )
                                    file_linked = True
                                except OSError:
                                    file_linked = False
                            if not file_linked:
                                procrunner.run(
                                    [
                                        "rsync",
                                        str(jf),
                                        f"{Path(args.destination) / jf.relative_to(Path(args.project))}",
                                    ]
                                )
                        else:
                            (
                                Path(args.destination)
                                / jf.relative_to(Path(args.project))
                            ).symlink_to(jf)

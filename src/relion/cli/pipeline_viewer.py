import webbrowser
import platform
import pathlib
import subprocess
from relion import Project
import argparse


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--project", required=True, dest="project")
    args = parser.parse_args()
    relion_dir = pathlib.Path(args.project)
    proj = Project(relion_dir)
    proj.show_job_nodes()
    if platform.system() == "Linux" and "WSL" in platform.release():
        subprocess.run(
            ["wslview", relion_dir / "Pipeline" / "relion_pipeline_jobs.gv.svg"],
            check=True,
        )
    else:
        webbrowser.open(relion_dir / "Pipeline" / "relion_pipeline_jobs.gv.svg")


if __name__ == "__main__":
    run()

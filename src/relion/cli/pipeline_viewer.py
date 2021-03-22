import sys
import webbrowser
import platform
import pathlib
import subprocess
from relion import Project


def run():
    relion_dir = pathlib.Path(sys.argv[1])
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

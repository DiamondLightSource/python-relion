from relion import Project
import argparse
import pathlib


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("proj_path")
    args = parser.parse_args()
    relion_dir = pathlib.Path(args.proj_path)
    proj = Project(relion_dir)
    current_jobs = proj.current_jobs
    print("These jobs are currently running: \n")
    for current_job in current_jobs:
        alias = current_job.attributes.get("alias")
        if alias is not None:
            print(f"Current job: {current_job._path} [alias={alias}]")
        else:
            print(f"Current job: {current_job._path}")
        print(f"Job started at: {current_job.attributes['start_time_stamp']}")
        print(f"Job has been run {current_job.attributes['job_count']} time(s)")
        print()


if __name__ == "__main__":
    run()

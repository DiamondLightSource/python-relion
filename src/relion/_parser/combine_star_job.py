from __future__ import annotations

from pathlib import Path

from pipeliner.data_structure import SELECT_DIR
from pipeliner.job_options import BooleanJobOption, IntJobOption, PathJobOption
from pipeliner.nodes import NODE_PARTICLESDATA, Node
from pipeliner.pipeliner_job import ExternalProgram, PipelinerJob

COMBINE_STAR_NAME = "combine_star_files.py"


class ProcessStarFiles(PipelinerJob):
    PROCESS_NAME = "combine_star_files_job"
    OUT_DIR = SELECT_DIR

    def __init__(self):
        super().__init__()

        self.jobinfo.display_name = "Particle star file merging"
        self.jobinfo.short_desc = "Combine and split star files of particles"
        self.jobinfo.long_desc = (
            "Combine star files of particles," " then optionally split them again."
        )

        self.jobinfo.programs = [ExternalProgram(command=COMBINE_STAR_NAME)]

        self.joboptions["folder_to_process"] = PathJobOption(
            label="Directory containing particle star files to combine",
            is_required=True,
        )
        self.joboptions["do_split"] = BooleanJobOption(
            label="Whether to split the combined star file", default_value=False
        )
        self.joboptions["n_files"] = IntJobOption(
            label="Number of files to split the combined file into",
            default_value=-1,
            help_text=(
                "Provide either the number of files to split into,"
                " or the number of particles per file."
            ),
            deactivate_if=[("do_split", "is", "False")],
        )
        self.joboptions["split_size"] = IntJobOption(
            label="Number of particles to put in each split",
            default_value=-1,
            help_text=(
                "Provide either the number of files to split into,"
                " or the number of particles per file."
            ),
            deactivate_if=[("do_split", "is", "False")],
        )

        self.set_joboption_order(
            ["folder_to_process", "do_split", "n_files", "split_size"]
        )

    def get_commands(self):
        """Construct the command for combining and splitting star files"""
        command = [
            COMBINE_STAR_NAME,
            self.joboptions["folder_to_process"].get_string(),
            "--output_dir",
            str(self.output_dir),
        ]

        if self.joboptions["do_split"].get_boolean():
            command.extend(["--split"])

            if (
                self.joboptions["n_files"].get_number() <= 0
                and self.joboptions["split_size"].get_number() <= 0
            ):
                raise ValueError(
                    "ERROR: When splitting the combined STAR file into subsets,"
                    " set n_files or split_size to a positive value"
                )

            if self.joboptions["n_files"].get_number() > 0:
                command.extend(["--n_files", self.joboptions["n_files"].get_string()])

            if self.joboptions["split_size"].get_number() > 0:
                command.extend(
                    ["--split_size", self.joboptions["split_size"].get_string()]
                )

        self.output_nodes.append(
            Node(self.output_dir + "particles_all.star", NODE_PARTICLESDATA)
        )

        return [command]

    def post_run_actions(self):
        """Find any output files produced by the splitting"""
        output_files = Path(self.output_dir).glob("particles_split*.star")
        for split in output_files:
            self.output_nodes.append(Node(split, NODE_PARTICLESDATA))

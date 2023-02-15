from __future__ import annotations

from pathlib import Path

from pipeliner.data_structure import SELECT_DIR
from pipeliner.job_options import BooleanJobOption, IntJobOption, PathJobOption
from pipeliner.nodes import Node
from pipeliner.pipeliner_job import ExternalProgram, PipelinerJob

COMBINE_STAR_NAME = "combine_star_files_job"


class ProcessStarFiles(PipelinerJob):
    PROCESS_NAME = "relion.select.process_star"
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
            self.joboption["folder_to_process"].get_string(),
            "--output_dir",
            str(self.output_dir),
        ]

        if self.joboption["do_split"].get_bool():
            command.append(["--split"])

            if (
                self.joboption["n_files"].get_number() <= 0
                and self.joboption["split_size"].get_number() <= 0
            ):
                raise ValueError(
                    "ERROR: When splitting the combined STAR file into subsets,"
                    " set n_files or split_size to a positive value"
                )

            if self.joboption["n_files"].get_number() > 0:
                command.append(["--n_files", self.joboption["n_files"].get_string()])

            if self.joboption["split_size"].get_number() > 0:
                command.append(
                    ["--split_size", self.joboption["split_size"].get_string()]
                )

        self.output_nodes.append(Node("particles_all.star", "particles"))

        return [command]

    def post_run_actions(self):
        """Find any output files produced by the splitting"""
        output_files = Path(self.output_dir).glob("particles_split*.star")

        for split in output_files:
            self.output_nodes.append(Node(split, "particles"))

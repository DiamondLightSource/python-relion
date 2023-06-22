from __future__ import annotations

import logging
from typing import NamedTuple, Optional

from relion._parser.jobtype import JobType

logger = logging.getLogger("relion._parser.select")


class SelectedClass(NamedTuple):
    class2d_job_string: str
    selected_class: str
    select_completion_time: Optional[float]


SelectedClass.__doc__ = "Automated 2D class selection stage."
SelectedClass.selected_class.__doc__ = "Name of a selected class."


class Select(JobType):
    def __eq__(self, other):
        if isinstance(other, Select):  # check this
            return self._basepath == other._basepath
        return False

    def __hash__(self):
        return hash(("relion._parser.Select", self._basepath))

    def __repr__(self):
        return f"Select({repr(str(self._basepath))})"

    def __str__(self):
        return f"<Select parser at {self._basepath}>"

    @property
    def job_number(self):
        jobs = [x.name for x in self._basepath.iterdir()]
        return jobs

    def _get_class2d_job_string(self, jobdir: str, param_file_name: str) -> str:
        paramfile = self._read_star_file(jobdir, param_file_name)
        info_table = self._find_table_from_column_name(
            "_rlnJobOptionVariable", paramfile
        )
        variables = [
            p.strip("'")
            for p in self.parse_star_file(
                "_rlnJobOptionVariable", paramfile, info_table
            )
        ]
        class2d_index = variables.index("fn_model")
        class2d_job_string = (
            self.parse_star_file("_rlnJobOptionValue", paramfile, info_table)[
                class2d_index
            ]
            .strip("'")
            .split("/")[1]
        )
        return class2d_job_string

    def _load_job_directory(self, jobdir):
        try:
            file = self._read_star_file(jobdir, "class_averages.star")
        except (FileNotFoundError, OSError, RuntimeError, ValueError):
            return []

        info_table = self._find_table_from_column_name("_rlnReferenceImage", file)
        if info_table is None:
            return []

        names = self.parse_star_file("_rlnReferenceImage", file, info_table)

        class2d_job_string = self._get_class2d_job_string(jobdir, "job.star")
        select_mod_time = (
            (self._basepath / jobdir / "RELION_JOB_EXIT_SUCCESS").stat().st_mtime
        )

        class_list = [
            SelectedClass(
                selected_class=n.split("@")[0],
                class2d_job_string=class2d_job_string,
                select_completion_time=select_mod_time,
            )
            for n in names
        ]
        return class_list

    @staticmethod
    def db_unpack(selected_class_list):
        res = [
            {
                "job_string": cl.class2d_job_string,
                "selected": True,
                "class_number": int(cl.selected_class),
                "class_images_modification_time": cl.select_completion_time,
            }
            for cl in selected_class_list
        ]
        return res

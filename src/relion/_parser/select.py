from __future__ import annotations

import logging
from typing import NamedTuple

from relion._parser.jobtype import JobType

logger = logging.getLogger("relion._parser.select")


class SelectedClass(NamedTuple):
    selected_class: str


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

    def _load_job_directory(self, jobdir):
        try:
            file = self._read_star_file(jobdir, "class_averages.star")
        except (FileNotFoundError, OSError, RuntimeError, ValueError):
            return []

        info_table = self._find_table_from_column_name("_rlnReferenceImage", file)
        if info_table is None:
            return []

        names = self.parse_star_file("_rlnReferenceImage", file, info_table)

        class_list = [SelectedClass(selected_class=n.split("@")[0]) for n in names]
        return class_list

    @staticmethod
    def db_unpack(selected_class_list):
        res = [
            {
                "selected": True,
                "class_number": int(cl.selected_class),
            }
            for cl in selected_class_list
        ]
        return res

from __future__ import annotations

import logging
from collections import namedtuple

from relion._parser.jobtype import JobType

logger = logging.getLogger("relion._parser.initalmodel")

InitialModelInfo = namedtuple(
    "InitialModelInfo",
    [
        "number_of_particles",
    ],
)


class InitialModel(JobType):
    def __eq__(self, other):
        if isinstance(other, InitialModel):  # check this
            return self._basepath == other._basepath
        return False

    def __hash__(self):
        return hash(("relion._parser.InitialModel", self._basepath))

    def __repr__(self):
        return f"InitialModel({repr(str(self._basepath))})"

    def __str__(self):
        return f"<InitialModel parser at {self._basepath}>"

    def _load_job_directory(self, jobdir):
        try:
            model_info_name = self._final_data(jobdir)
        except (RuntimeError, FileNotFoundError, OSError):
            return []

        try:
            model_info_file = self._read_star_file(jobdir, model_info_name)
        except (RuntimeError, FileNotFoundError, OSError):
            return []

        info_table = self._find_table_from_column_name(
            "_rlnClassNumber", model_info_file
        )
        if info_table is None:
            logger.debug(f"_rlnClassNumber not found in file {model_info_file}")
            return []

        num_particles = {}
        class_numbers = self.parse_star_file(
            "_rlnClassNumber", model_info_file, info_table
        )
        for n in class_numbers:
            num_particles[int(n)] = num_particles.get(int(n), 0) + 1

        return [InitialModelInfo(num_particles)]

    def _final_data(self, job_path):
        number_list = [
            entry.stem[6:9]
            for entry in (self._basepath / job_path).glob("run_it*.star")
        ]
        last_iteration_number = max(
            (int(n) for n in number_list if n.isnumeric()), default=0
        )
        if not last_iteration_number:
            raise ValueError(f"No result files found in {job_path}")
        data_file = f"run_it{last_iteration_number:03d}_data.star"
        for check_file in (self._basepath / job_path / data_file,):
            if not check_file.exists():
                raise ValueError(f"File {check_file} missing from job directory")
        return data_file

    @staticmethod
    def for_cache(initmodelinfo):
        return initmodelinfo.number_of_particles

    @staticmethod
    def db_unpack(initmodelinfo):
        res = {
            "init_model_number_of_particles": initmodelinfo[0].number_of_particles,
        }

        return res

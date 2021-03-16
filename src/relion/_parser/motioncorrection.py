import collections.abc
from gemmi import cif

import os
import functools
from collections import namedtuple

MCMicrograph = namedtuple(
    "MCMicrograph", ["micrograph_name", "total_motion", "early_motion", "late_motion"]
)

MCMicrograph.__doc__ = "Motion Correction stage."
MCMicrograph.micrograph_name.__doc__ = "Micrograph name. Useful for reference."
MCMicrograph.total_motion.__doc__ = (
    "Total motion. The amount the sample moved during exposure. Units angstrom (A)."
)
MCMicrograph.early_motion.__doc__ = "Early motion."
MCMicrograph.late_motion.__doc__ = "Late motion."


class MotionCorr(collections.abc.Mapping):
    def __eq__(self, other):
        if isinstance(other, MotionCorr):
            return self._basepath == other._basepath
        return False

    def __hash__(self):
        return hash(("relion._parser.MotionCorr", self._basepath))

    def __init__(self, path):
        self._basepath = path
        self._jobcache = {}

    def __iter__(self):
        return iter(self.jobs)

    def __len__(self):
        return sum(1 for d in self.jobs)

    def __repr__(self):
        return f"MotionCorr({repr(str(self._basepath))})"

    def __str__(self):
        return f"<MotionCorr parser at {self._basepath}>"

    @property
    def jobs(self):
        return sorted(
            d.stem
            for d in self._basepath.iterdir()
            if d.is_dir() and not d.is_symlink()
        )

    def __getitem__(self, key):
        if not isinstance(key, str):
            raise KeyError(f"Invalid argument {key!r}, expected string")
        if key not in self._jobcache:
            job_path = self._basepath / key
            if not job_path.is_dir():
                raise KeyError(
                    f"no job directory present for {key} in {self._basepath}"
                )
            self._jobcache[key] = self._load_job_directory(key)
        return self._jobcache[key]

    def parse_star_file(self, loop_name, star_doc, block_number):
        data_block = star_doc[block_number]
        values = data_block.find_loop(loop_name)
        values_list = list(values)
        if not values_list:
            print("Warning - no values found for", loop_name)
        return values_list

    @functools.lru_cache(maxsize=None)
    def _read_star_file(self, job_num):
        full_path = self._basepath / job_num / "corrected_micrographs.star"
        gemmi_readable_path = os.fspath(full_path)
        star_doc = cif.read_file(gemmi_readable_path)
        return star_doc

    def _load_job_directory(self, jobdir):
        file = self._read_star_file(jobdir)
        accum_motion_total = self.parse_star_file("_rlnAccumMotionTotal", file, 1)
        accum_motion_late = self.parse_star_file("_rlnAccumMotionLate", file, 1)
        accum_motion_early = self.parse_star_file("_rlnAccumMotionEarly", file, 1)
        micrograph_name = self.parse_star_file("_rlnMicrographName", file, 1)

        micrograph_list = []
        for j in range(len(micrograph_name)):
            micrograph_list.append(
                MCMicrograph(
                    micrograph_name[j],
                    accum_motion_total[j],
                    accum_motion_early[j],
                    accum_motion_late[j],
                )
            )
        return micrograph_list

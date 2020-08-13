from gemmi import cif
import os
import functools
from collections import namedtuple

CTFMicrograph = namedtuple(
    "CTFMicrograph",
    [
        "astigmatism",
        "defocus_u",
        "defocus_v",
        "defocus_angle",
        "max_resolution",
        "fig_of_merit",
    ],
)


class CTFFind:
    def __init__(self, path):
        self._basepath = path
        self._jobcache = {}

    def __str__(self):
        return f"I'm a CTFFind instance at {self._basepath}"

    @property
    def jobs(self):
        return sorted(d.stem for d in self._basepath.iterdir() if d.is_dir())

    def __getitem__(self, key):
        if not isinstance(key, str):
            raise KeyError(f"Invalid argument {key!r}, expected string")
        if key not in self._jobcache:
            job_path = self._basepath / key
            if not job_path.is_dir():
                raise KeyError(
                    f"no job directory present for {key} in {self._basepath}"
                )
            self._jobcache[key] = job_path
        return self._jobcache[key]

    @property
    def astigmatism(self):
        return self._find_values("_rlnCtfAstigmatism")

    @property
    def defocus_u(self):
        return self._find_values("_rlnDefocusU")

    @property
    def defocus_v(self):
        return self._find_values("_rlnDefocusV")

    @property
    def defocus_angle(self):
        return self._find_values("_rlnDefocusAngle")

    @property
    def max_resolution(self):
        return self._find_values("_rlnCtfMaxResolution")

    @property
    def fig_of_merit(self):
        return self._find_values("_rlnCtfFigureOfMerit")

    @property
    def micrograph_name(self):
        return self._find_values("_rlnMicrographName")

    def parse_star_file(self, loop_name, star_doc, block_number):
        data_block = star_doc[block_number]
        values = data_block.find_loop(loop_name)
        values_list = list(values)
        if not values_list:
            print("Warning - no values found for", loop_name)
        return values_list

    def _find_values(self, value):
        final_list = []
        for x in self._basepath.iterdir():
            if "job" in x.name:
                job = x.name
                doc = self._read_star_file(job)
                val_list = list(self.parse_star_file(value, doc, 1))
                final_list.extend(val_list)
        return final_list

    @functools.lru_cache(maxsize=None)
    def _read_star_file(self, job_num):
        full_path = self._basepath / job_num / "micrographs_ctf.star"
        gemmi_readable_path = os.fspath(full_path)
        star_doc = cif.read_file(gemmi_readable_path)
        return star_doc

    def construct_dict(
        self,
        micrograph_name_list,
        astigmatism_list,
        defocus_u_list,
        defocus_v_list,
        defocus_angle_list,
        max_res_list,
        fig_of_merit_list,
    ):  # *args):
        final_dict = {
            name: CTFMicrograph(
                astigmatism_list[i],
                defocus_u_list[i],
                defocus_v_list[i],
                defocus_angle_list[i],
                max_res_list[i],
                fig_of_merit_list[i],
            )
            for i, name in enumerate(micrograph_name_list)
        }
        return final_dict

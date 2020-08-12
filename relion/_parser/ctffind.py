from gemmi import cif
from pathlib import Path
import os
import functools
from collections import namedtuple

CTFMicrograph = namedtuple(
    "CTFMicrograph", ["total_motion", "early_motion", "late_motion"]
)


class CTFFind:
    def __init__(self, path):
        self._basepath = path
        self._jobcache = {}
        self.val_astigmatism = None
        self.val_defocus_u = None
        self.val_defocus_v = None
        self.val_defocus_angle = None
        self.val_max_resolution = None
        self.val_fig_or_merit = None

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
        return self.val_astigmatism

    @property
    def defocus_u(self):
        return self.val_defocus_u

    @property
    def defocus_v(self):
        return self.val_defocus_v

    @property
    def defocus_angle(self):
        return self.val_defocus_angle

    @property
    def max_resolution(self):
        return self.val_max_resolution

    @property
    def fig_of_merit(self):
        return self.val_fig_or_merit

    def set_astigmatism(self):
        values = self.find_values("_rlnCtfAstigmatism")
        self.val_astigmatism = values

    def set_defocus_u(self):
        values = self.find_values("_rlnDefocusU")
        self.val_defocus_u = values

    def set_defocus_v(self):
        values = self.find_values("_rlnDefocusV")
        self.val_defocus_v = values

    def set_defocus_angle(self):
        values = self.find_values("_rlnDefocusAngle")
        self.val_defocus_angle = values

    def set_max_resolution(self):
        values = self.find_values("_rlnCtfMaxResolution")
        self.val_max_resolution = values

    def set_fig_of_merit(self):
        values = self.find_values("_rlnCtfFigureOfMerit")
        self.val_fig_or_merit = values

    def parse_star_file(self, loop_name, star_doc, block_number):
        data_block = star_doc[block_number]
        values = data_block.find_loop(loop_name)
        values_list = list(values)
        if not values_list:
            print("Warning - no values found for", loop_name)
        return values_list

    def find_values(self, value):
        file_path = Path(self._basepath) / "CTFFind"
        final_list = []
        for x in file_path.iterdir():
            if "job" in x.name:
                job = x.name
                doc = self._read_star_file(job)
                val_list = list(self.parse_star_file(value, doc, 1))
                final_list.extend(val_list)
        return final_list

    @functools.lru_cache(maxsize=None)
    def _read_star_file(self, job_num):
        full_path = Path(self._basepath) / "CTFFind" / job_num / "micrographs_ctf.star"
        gemmi_readable_path = os.fspath(full_path)
        star_doc = cif.read_file(gemmi_readable_path)
        return star_doc

    def construct_dict(
        self,
        micrograph_name_list,
        total_motion_list,
        early_motion_list,
        late_motion_list,
    ):  # *args):
        final_dict = {
            name: CTFMicrograph(
                total_motion_list[i], early_motion_list[i], late_motion_list[i]
            )
            for i, name in enumerate(micrograph_name_list)
        }
        print(final_dict)
        return final_dict

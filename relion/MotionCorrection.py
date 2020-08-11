from gemmi import cif
from pathlib import Path
import os
import functools
from collections import namedtuple


class MotionCorrection:
    def __init__(self, data_dir):
        self.relion_dir = data_dir
        self.directory = str(self.relion_dir)
        self.file_name = None
        # self.job_num = ""
        self.val_accum_motion_total = None
        self.val_accum_motion_early = None
        self.val_accum_motion_late = None
        self.val_micrograph_name = None

    @property
    def accum_motion_total(self):
        return self.val_accum_motion_total

    @property
    def accum_motion_late(self):
        return self.val_accum_motion_late

    @property
    def accum_motion_early(self):
        return self.val_accum_motion_early

    @property
    def micrograph_name(self):
        return self.val_micrograph_name

    def set_total_accum_motion(self):
        values = self.find_values("_rlnAccumMotionTotal")
        self.val_accum_motion_total = values

    def set_late_accum_motion(self):
        values = self.find_values("_rlnAccumMotionLate")
        self.val_accum_motion_late = values

    def set_early_accum_motion(self):
        values = self.find_values("_rlnAccumMotionEarly")
        self.val_accum_motion_early = values

    def set_micrograph_name(self):
        values = self.find_values("_rlnMicrographName")
        self.val_micrograph_name = values

    def parse_star_file(self, loop_name, star_doc, block_number):
        data_block = star_doc[block_number]
        values = data_block.find_loop(loop_name)
        values_list = list(values)
        if not values_list:
            print("Warning - no values found for", loop_name)
        return values_list

    def find_values(self, value):
        file_path = Path(self.directory) / "MotionCorr"
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
        full_path = (
            Path(self.directory) / "MotionCorr" / job_num / "corrected_micrographs.star"
        )
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
        final_dict = {}
        MCMicrographs = namedtuple(
            "Micrograph", ["Total_motion", "Early_motion", "Late_motion"]
        )
        for i in range(len(micrograph_name_list)):
            p = MCMicrographs(
                total_motion_list[i], early_motion_list[i], late_motion_list[i]
            )
            final_dict[micrograph_name_list[i]] = {
                p.Total_motion,
                p.Early_motion,
                p.Late_motion,
            }
        print(final_dict)

from gemmi import cif
from pathlib import Path
import os


class MotionCorrection:
    def __init__(self, data_dir):
        self.relion_dir = data_dir
        self.directory = str(self.relion_dir)
        self.file_name = None
        self.job_num = ""
        self.accum_motion_total = None
        self.accum_motion_early = None
        self.accum_motion_late = None

    @property
    def get_accum_motion_total(self):
        return self.accum_motion_total

    @property
    def get_accum_motion_late(self):
        return self.accum_motion_late

    @property
    def get_accum_motion_early(self):
        return self.accum_motion_early

    def set_total_accum_motion(self):
        file_path = Path(self.directory) / "MotionCorr"
        final_list = []
        list = []
        for x in file_path.iterdir():
            if "job" in x.name:
                self.job_num = x.name
                AMT_list = self.parse_star_file("_rlnAccumMotionTotal", 1)
                list = [self.job_num] + AMT_list
            final_list.append(list)
        self.accum_motion_total = final_list

    def set_late_accum_motion(self):
        file_path = Path(self.directory) / "MotionCorr"
        final_list = []
        list = []
        for x in file_path.iterdir():
            if "job" in x.name:
                self.job_num = x.name
                AML_list = self.parse_star_file("_rlnAccumMotionLate", 1)
                list = [self.job_num] + AML_list
            final_list.append(list)
        self.accum_motion_late = final_list

    def set_early_accum_motion(self):
        file_path = Path(self.directory) / "MotionCorr"
        final_list = []
        list = []
        for x in file_path.iterdir():
            if "job" in x.name:
                self.job_num = x.name
                AME_list = self.parse_star_file("_rlnAccumMotionEarly", 1)
                list = [self.job_num] + AME_list
            final_list.append(list)
        self.accum_motion_early = final_list

    def parse_star_file(self, loop_name, block_number):
        values_list = []
        full_path = (
            Path(self.directory)
            / "MotionCorr"
            / self.job_num
            / "corrected_micrographs.star"
        )
        gemmi_readable_path = os.fspath(full_path)
        star_doc = cif.read_file(gemmi_readable_path)
        data_block = star_doc[block_number]
        values = data_block.find_loop(loop_name)
        for x in values:
            values_list.append(x)
        if not values_list:
            print("Warning - no values found for", loop_name)
        return values_list

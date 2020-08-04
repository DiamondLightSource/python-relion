from gemmi import cif
from pathlib import Path
import os

# This class aims to be as unspecific as possible, a general tool for extracting data from files.
# The main use case in mind is where there is information in several files in different directories.
# The input parameters dictate where the information is extracted from and how it is taken.

# The get_data() method returns a dictionary of values. This could be passed to a class which puts these results into a database.

# The input_dict is where the specifics of a search are laid out.
# This should make it easy for users to add more things to find - a new section in the input directory can be added, rather than writing a whole new class or something.

# The data_dir is the overall place where data is stored.
# There may be multiple subdirectories which hold different files - there should be no desired information higher in the directory than the data_dir.


class FindData:
    def __init__(self, data_dir, input_dict):
        self.relion_dir = data_dir
        self.directory = str(self.relion_dir)
        self.file_type = None
        self.input_dict = input_dict
        self.folder = None
        self.file_path = None
        self.star_doc = None
        self.file_name = None
        self.value_name = None
        self.folder_string = None
        self.line_count = None
        self.job_num = ""
        self.relion_job_mode = True

    def extract_per_section(self, section):
        self.folder = self.input_dict[section]["name"]
        self.folder_string = str(self.folder)
        self.file_path = Path(self.directory) / self.folder_string

    def extract_per_value(self, section, count):
        self.value_name = self.input_dict[section][count][0]
        if self.relion_job_mode:
            self.value_name = self.value_name[4:]
        self.file_name = self.input_dict[section][count][1]
        self.file_path = Path(self.directory) / self.folder_string
        self.file_type = Path(self.file_name).suffix
        self.line_count = 0

    def get_data(self):
        output_list = []
        for section in self.input_dict:
            self.extract_per_section(section)
            section_list = []
            section_list.append(self.folder)
            count = 1
            while count < len(self.input_dict[section]):
                self.extract_per_value(section, count)

                if self.file_type == ".out":
                    self.file_path = (
                        Path(self.directory) / self.folder_string / self.file_name
                    )
                    result = self.parse_out_file()
                    return result

                if self.file_type == ".star":
                    for x in self.file_path.iterdir():
                        if self.relion_job_mode:
                            if "job" in x.name:
                                self.job_num = x.name

                                if "run" in self.file_name:
                                    self.file_name = self.find_last_iteration()
                                list = self.parse_star_file(section, count)
                                section_list.append(list)
                            else:
                                pass

                        if not self.relion_job_mode:
                            list = self.parse_star_file(section, count)
                            section_list.append(list)
                        # print('particle count:', self.line_count, self.file_name, self.job_num)

                count += 1
            output_list.append(section_list)
        return output_list

    def parse_star_file(self, section, count):
        values_list = []
        block_number = self.input_dict[section][count][2]
        loop_name = self.input_dict[section][count][0]
        self.file_path = (
            Path(self.directory) / self.folder_string / self.job_num / self.file_name
        )
        gemmi_readable_path = os.fspath(self.file_path)
        self.star_doc = cif.read_file(gemmi_readable_path)
        data_block = self.star_doc[block_number]
        values = data_block.find_loop(loop_name)
        for x in values:
            values_list.append(x)
            self.line_count += 1
        if not values_list:
            print("Warning - no values found for", self.value_name)
        name_job_title = str(self.value_name + "/" + self.job_num)
        line_entry = str(self.line_count) + " lines"
        final_list = [name_job_title] + [line_entry] + values_list
        return final_list

    def parse_out_file(self):  # section, count):
        f = open(self.file_path)
        for line in f:
            if self.value_name in line:
                print("Match found")
                return True
            # return a final list

    def find_last_iteration(self):
        file_list = list(self.file_path.glob("**/*.star"))
        filename_list = []
        for x in file_list:
            filename_list.append(x.name)
        for x in filename_list:

            if "run" in x:
                if "data" or "model" in x:
                    if self.has_numbers(x):
                        nlist = []
                        nlist.append(x)
                        number_list = []
                        number = int(x[6:9])
                        number_list.append(number)
                        number_list.sort()
                        last_iteration_number = number_list[-1]
                        if "data" in self.file_name:
                            self.file_name = (
                                "run_it"
                                + str(last_iteration_number).zfill(3)
                                + "_data.star"
                            )
                        if "model" in self.file_name:
                            self.file_name = (
                                "run_it"
                                + str(last_iteration_number).zfill(3)
                                + "_model.star"
                            )
        return self.file_name

    def has_numbers(self, input_string):
        return any(char.isdigit() for char in input_string)

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

# The file_type is whether the file is .star, a log file, .txt etc.
# Specifiying is necessary because different file types are parsed in different ways.
# The tool currently only deals with star files but could be expanded to cover other file types.

# There are many things which could be improved but the main one would probably be including more checks - currently this program is pretty fragile and breaks if it can't find a given field..


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

    def extract_per_section(self, section):
        self.folder = self.input_dict[section]["name"]
        self.folder_string = str(self.folder)

    def extract_per_value(self, section, count):
        self.value_name = self.input_dict[section][count][0]
        self.file_name = self.input_dict[section][count][2]
        self.file_path = Path(self.directory) / self.folder_string / self.file_name
        self.file_type = self.input_dict[section][count][1]

    def get_data(self):
        output_list = []
        for section in self.input_dict:
            self.extract_per_section(section)
            section_list = []
            section_list.append(self.folder)
            count = 1
            while count < len(self.input_dict[section]):
                self.extract_per_value(section, count)

                if self.file_type == "out":
                    result = self.parse_out_file()
                    return result

                if self.file_type == "star":
                    list = self.parse_star_file(section, count)
                    section_list.append(list)

                count += 1
            output_list.append(section_list)
        return output_list

    def parse_star_file(self, section, count):
        values_list = []
        block_number = self.input_dict[section][count][3]
        loop_name = self.input_dict[section][count][4]
        gemmi_readable_path = os.fspath(self.file_path)
        self.star_doc = cif.read_file(gemmi_readable_path)
        data_block = self.star_doc[block_number]
        values = data_block.find_loop(loop_name)
        for x in values:
            values_list.append(x)
        if not values_list:
            print("Warning - no values found for", self.value_name)
        final_list = [self.value_name] + values_list
        return final_list

    def parse_out_file(self):  # section, count):
        f = open(self.file_path)
        for line in f:
            if self.value_name in line:
                print("Match found")
                return True
            # return a final list

from gemmi import cif
from pprint import pprint

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
    def __init__(self, data_dir, file_type, input_dict):
        self.relion_dir = data_dir
        self.file_type = file_type
        self.input_dict = input_dict
        self.folder = None
        self.file_path = None
        self.star_doc = None

    def get_data(self):
        if self.file_type == "star":
            output_list = []
            for section in self.input_dict:
                self.folder = self.input_dict[section]["name"]
                section_list = []
                section_list.append(self.folder)
                count = 1
                while count < len(self.input_dict[section]):
                    values_list = []

                    # extract values from the input dictionary
                    value_name = self.input_dict[section][count][0]
                    file_name = self.input_dict[section][count][1]
                    block_number = self.input_dict[section][count][2]
                    loop_name = self.input_dict[section][count][3]

                    values_list.append(value_name)

                    self.file_path = (
                        str(self.relion_dir)
                        + "/"
                        + str(self.folder)
                        + "/"
                        + "/"
                        + file_name
                    )  # Will need to add the job folder automatically as well somehow
                    self.star_doc = cif.read_file(self.file_path)
                    data_block = self.star_doc[block_number]
                    values = data_block.find_loop(loop_name)

                    for x in values:
                        values_list.append(x)
                    section_list.append(values_list)
                    count += 1
                output_list.append(section_list)
            pprint(output_list)
            return output_list

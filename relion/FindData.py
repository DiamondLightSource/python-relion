from gemmi import cif

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

            for section in self.input_dict:

                self.folder = self.input_dict[section]["name"]
                count = 0
                while count < len(self.input_dict[section]["values"]):

                    self.file_path = (
                        str(self.relion_dir)
                        + "/"
                        + str(self.folder)
                        + "/"
                        + "/"
                        + self.input_dict[section]["file_name"][count]
                    )  # Will need to add the job folder automatically as well somehow
                    self.star_doc = cif.read_file(self.file_path)
                    block_number = self.input_dict[section]["block_number"][count]
                    data_block = self.star_doc[block_number]
                    values = data_block.find_loop(
                        self.input_dict[section]["loop_name"][count]
                    )

                    values_list = []
                    for x in values:
                        values_list.append(x)
                    value_name = str(self.input_dict[section]["values"][count])
                    print(value_name, ": ", values_list, sep="")

                    count += 1

            return values_list

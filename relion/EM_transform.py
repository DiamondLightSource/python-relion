# 2D - find particles from the same class
# 2D - sort by particles per class
# 2D - percentage of particles in all data in each class
# 2D - percentage of particles in all data in the 20 classes shown

from collections import Counter


class EMTransform:
    def __init__(self, extracted_data_list):
        self.data = extracted_data_list

    def group_by_class_number(self):
        final_dict = {}
        for x in self.data:
            dict = {}
            if "ClassNumber" in x[0]:
                for i in range(len(x)):
                    key = str(x[i])
                    try:
                        float(key)
                        dict.setdefault(key, [])
                        dict[key].append(i)
                    except ValueError:
                        pass
                final_dict[x[0]] = dict
        return final_dict

    def show_class_num_and_particle_count(self, grouped_dict):
        for key in grouped_dict:
            for nest_key in grouped_dict[key]:
                pass
                # print('Class number:', nest_key, 'Particle count', len(grouped_dict[key][nest_key]))

    def try_counter(self):
        for x in self.data:
            if "ClassNumber" in x[0]:
                count = Counter(
                    x[2:]
                )  # from second to end, to exclude the string values
                return count

    def sort_particles_per_class(self, grouped_dict):
        joined_list = []
        for key in grouped_dict:
            per_job_list = []
            for nest_key in grouped_dict[key]:
                per_job_list.append([nest_key, len(grouped_dict[key][nest_key])])
                per_job_list.sort(key=lambda x: x[1], reverse=True)
            joined_list.append([key, per_job_list])
        return joined_list

    def sum_all_particles_per_section(self):
        section_list = []
        for x in self.data[1:]:
            section_list.append([x[0], len(x[1:])])
        return section_list

    def percent_all_particles_per_class(self, grouped_dict):
        per_class_list = self.sort_particles_per_class(grouped_dict)
        per_section_list = self.sum_all_particles_per_section()

        for item1 in per_class_list:
            for item2 in per_section_list:
                if item1[0] == item2[0]:
                    for i in range(1, len(item1), 2):
                        for list in item1[i]:
                            print("Class:", list[0], "Num. particles:", list[1])
                            percentage = (list[1] / item2[1]) * 100
                            print(
                                "Percent of data from job in this class:",
                                round(percentage, 2),
                            )
                    print("\n")

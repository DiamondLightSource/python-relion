# 2D - find particles from the same class
# 2D - sort by particles per class
# 2D - percentage of particles in all data in each class
# 2D - percentage of particles in all data in the 20 classes shown

from pprint import pprint
from collections import Counter


class EMTransform:
    def __init__(self, extracted_data_list):
        self.data = extracted_data_list

    def group_by_class_number(self):
        final_dict = {}
        count = 1
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

                final_dict[count] = dict
                count += 1
        return final_dict

    def show_class_num_and_particle_count(self, grouped_dict):
        for key in grouped_dict:
            for nest_key in grouped_dict[key]:
                pass
                # print('Class number:', nest_key, 'Particle count', len(grouped_dict[key][nest_key]))

    def try_counter(self):
        count = None
        for x in self.data:
            if "ClassNumber" in x[0]:
                count = Counter(
                    x[2:]
                )  # from second to end, to exclude the string values
                return count

    def sort_particles_per_class(self, grouped_dict):
        joined_list = []
        for key in grouped_dict:
            print(key, grouped_dict[key])
            per_job_list = []
            for nest_key in grouped_dict[key]:
                per_job_list.append([nest_key, len(grouped_dict[key][nest_key])])
                per_job_list.sort(key=lambda x: x[1], reverse=True)
            joined_list.append(per_job_list)
        pprint(joined_list)
        for item in joined_list:
            for list in item:
                print("Class:", list[0], "Num. particles:", list[1])
            print("\n")
        return joined_list

    def sum_all_particles(self):
        count = 0
        for x in self.data[
            1:
        ]:  # including the first element meant if it contains a number it will count it as a particle
            for y in x:
                try:
                    float(
                        y
                    )  # should later add something to catch particles which have 'null' or 'n/a' as values
                    print(y)
                    count += 1
                except ValueError:
                    pass
        # print(count)
        return count

    def percent_all_particles_per_class(self, grouped_dict):
        pass

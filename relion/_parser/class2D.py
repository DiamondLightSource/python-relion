from gemmi import cif
import os
import functools
from collections import namedtuple
from collections import Counter

Class2DMicrograph = namedtuple(
    "Class2DMicrograph",
    [
        "reference_image",
        "class_distribution",
        "accuracy_rotations",
        "accuracy_translations_angst",
        "estimated_resolution",
        "overall_fourier_completeness",
    ],
)


class Class2D:
    def __init__(self, path):
        self._basepath = path
        self._jobcache = {}

    def __str__(self):
        return f"I'm a Class2D instance at {self._basepath}"

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
    def job_number(self):
        jobs = sorted([x.name for x in self._basepath.iterdir()])
        return jobs

    @property
    def class_number(self):
        return self._find_values("_rlnClassNumber", "data")

    @property
    def class_distribution(self):
        return self._find_values("_rlnClassDistribution", "model")

    @property
    def accuracy_rotations(self):
        return self._find_values("_rlnAccuracyRotations", "model")

    @property
    def accuracy_translations_angst(self):
        return self._find_values("_rlnAccuracyTranslationsAngst", "model")

    @property
    def estimated_resolution(self):
        return self._find_values("_rlnEstimatedResolution", "model")

    @property
    def overall_fourier_completeness(self):
        return self._find_values("_rlnOverallFourierCompleteness", "model")

    @property
    def micrograph_name(self):
        return self._find_values("_rlnMicrographName", "data")

    @property
    def reference_image(self):
        return self._find_values("_rlnReferenceImage", "model")

    def _find_values(self, value, data_or_model):
        final_list = []
        for x in self._basepath.iterdir():
            if "job" in x.name:
                job = x.name
                val_list = []
                if x.name not in self._jobcache:
                    file = self.find_last_iteration(data_or_model)
                    doc = self._read_star_file(job, file)
                    val_list = list(self.parse_star_file(value, doc, 1))
                final_list.append(val_list)
        return final_list

    def find_last_iteration(self, type):
        file_list = list(self._basepath.glob("**/*.star"))
        filename_list = [x.name for x in file_list]
        filename = None
        for x in filename_list:
            if "run" in x:
                if type in x:
                    if self.has_numbers(x):
                        nlist = []
                        nlist.append(x)
                        number_list = []
                        number = int(x[6:9])
                        number_list.append(number)
                        number_list.sort()
                        last_iteration_number = number_list[-1]

                        filename = (
                            "run_it"
                            + str(last_iteration_number).zfill(3)
                            + "_"
                            + type
                            + ".star"
                        )
        return filename

    @functools.lru_cache(maxsize=None)
    def _read_star_file(self, job_num, file_name):
        full_path = self._basepath / job_num / file_name
        gemmi_readable_path = os.fspath(full_path)
        star_doc = cif.read_file(gemmi_readable_path)
        return star_doc

    def parse_star_file(self, loop_name, star_doc, block_number):
        data_block = star_doc[block_number]
        values = data_block.find_loop(loop_name)
        values_list = list(values)
        if not values_list:
            print("Warning - no values found for", loop_name)
        return values_list

    def has_numbers(self, input_string):
        return any(char.isdigit() for char in input_string)

    def construct_dict(
        self,
        job_nums,
        ref_image_list,
        class_dist_list,
        accuracy_rotation_list,
        accuracy_translation_list,
        estimated_res_list,
        overall_fourier_list,
    ):  # *args):
        final_dict = {}
        for i in range(len(job_nums)):
            micrographs_list = []
            for j in range(len(ref_image_list[i])):
                micrographs_list.append(
                    [
                        Class2DMicrograph(
                            ref_image_list[i][j],
                            class_dist_list[i][j],
                            accuracy_rotation_list[i][j],
                            accuracy_translation_list[i][j],
                            estimated_res_list[i][j],
                            overall_fourier_list[i][j],
                        )
                    ]
                )
            final_dict[job_nums[i]] = micrographs_list
        return final_dict

    def _count_all(self, list):
        count = Counter(list)
        return count

    def _sum_all_particles(self, list):
        counted = self._count_all(list)
        return sum(counted.values())

    def _sum_top_twenty_particles(self, list):
        top_twenty_list = self.top_twenty_most_populated(list)
        sum_twenty = sum(x[1] for x in top_twenty_list)
        return sum_twenty

    def top_twenty_most_populated(self, list):
        counted = self._count_all(list)
        return counted.most_common(20)

    def percent_all_particles_per_class(self, list):
        top_twenty = self._count_all(list).most_common(20)
        sum_all = self._sum_all_particles(list)
        percent_list = []
        for x in top_twenty:
            percent_list.append(((x[0], (x[1] / sum_all) * 100)))
        return percent_list

    def percent_all_particles_in_top_twenty_classes(
        self, list,
    ):
        top_twenty = self._count_all(list).most_common(20)
        sum_top_twenty_particles = self._sum_top_twenty_particles(list)
        percent_list = []
        for x in top_twenty:
            percent_list.append(((x[0], (x[1] / sum_top_twenty_particles) * 100)))
        return percent_list

    def separate_jobs(self):
        pass

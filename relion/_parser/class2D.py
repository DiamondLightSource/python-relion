import collections.abc
import os
import functools
from collections import namedtuple
from collections import Counter

from gemmi import cif

Class2DParticleClass = namedtuple(
    "Class2DParticleClass",
    [
        "particle_sum",
        "reference_image",
        "class_distribution",
        "accuracy_rotations",
        "accuracy_translations_angst",
        "estimated_resolution",
        "overall_fourier_completeness",
    ],
)


class Class2D(collections.abc.Mapping):
    def __eq__(self, other):
        if isinstance(other, Class2D):
            return self._basepath == other._basepath
        return False

    def __hash__(self):
        return hash(("relion._parser.Class2D", self._basepath))

    def __init__(self, path):
        self._basepath = path
        self._jobcache = {}

    def __iter__(self):
        return (x.name for x in self._basepath.iterdir())

    def __len__(self):
        return len(self._basepath.iterdir())

    def __repr__(self):
        return f"Class2D({repr(str(self._basepath))})"

    def __str__(self):
        return f"<Class2D parser at {self._basepath}>"

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
            self._jobcache[key] = self._load_job_directory(key)
        return self._jobcache[key]

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

    def _load_job_directory(self, jobdir):
        # these are independent of jobdir, ie. this is a bug
        dfile = self.find_last_iteration("data")
        mfile = self.find_last_iteration("model")
        # print(dfile, mfile)

        sdfile = self._read_star_file(jobdir, dfile)
        smfile = self._read_star_file(jobdir, mfile)
        # print(smfile)

        class_distribution = self.parse_star_file("_rlnClassDistribution", smfile, 1)
        accuracy_rotations = self.parse_star_file("_rlnAccuracyRotations", smfile, 1)
        accuracy_translations_angst = self.parse_star_file(
            "_rlnAccuracyTranslationsAngst", smfile, 1
        )
        estimated_resolution = self.parse_star_file(
            "_rlnEstimatedResolution", smfile, 1
        )
        overall_fourier_completeness = self.parse_star_file(
            "_rlnOverallFourierCompleteness", smfile, 1
        )
        reference_image = self.parse_star_file("_rlnReferenceImage", smfile, 1)

        class_numbers = self.parse_star_file("_rlnClassNumber", sdfile, 1)
        particle_sum = self._sum_all_particles(class_numbers)
        int_particle_sum = [(int(name), value) for name, value in particle_sum.items()]
        checked_particle_list = self._class_checker(
            sorted(int_particle_sum), len(reference_image)
        )

        particle_class_list = []
        for j in range(len(reference_image)):
            particle_class_list.append(
                Class2DParticleClass(
                    checked_particle_list[j],
                    reference_image[j],
                    float(class_distribution[j]),
                    accuracy_rotations[j],
                    accuracy_translations_angst[j],
                    estimated_resolution[j],
                    overall_fourier_completeness[j],
                )
            )
        return particle_class_list

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

    def _count_all(self, list):
        count = Counter(list)
        return count

    def _sum_all_particles(self, list):
        counted = self._count_all(
            list
        )  # This sorts them into classes and the number of associated particles before summing them all
        return counted
        # return sum(counted.values())

    def _class_checker(
        self, tuple_list, length
    ):  # Makes sure every class has a number of associated particles
        for i in range(1, length):
            if i not in tuple_list[i - 1]:
                tuple_list.insert(i - 1, (i, 0))
                print("No values found for class", i)
        return tuple_list

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

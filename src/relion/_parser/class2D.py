import collections.abc
import os
import functools
from collections import namedtuple
from collections import Counter
from operator import attrgetter
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

Class2DParticleClass.__doc__ = "2D Classification stage."
Class2DParticleClass.particle_sum.__doc__ = "Sum of all particles in the class. Gives a tuple with the class number first, then the particle sum."
Class2DParticleClass.reference_image.__doc__ = "Reference image."
Class2DParticleClass.class_distribution.__doc__ = (
    "Class Distribution. Proportional to the number of particles per class."
)
Class2DParticleClass.accuracy_rotations.__doc__ = "Accuracy rotations."
Class2DParticleClass.accuracy_translations_angst.__doc__ = (
    "Accuracy translations angst."
)
Class2DParticleClass.estimated_resolution.__doc__ = "Estimated resolution."
Class2DParticleClass.overall_fourier_completeness.__doc__ = (
    "Overall Fourier completeness."
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
        return iter(self.jobs)

    def __len__(self):
        return len(self.jobs)

    def __repr__(self):
        return f"Class2D({repr(str(self._basepath))})"

    def __str__(self):
        return f"<Class2D parser at {self._basepath}>"

    @property
    def jobs(self):
        return sorted(
            d.name
            for d in self._basepath.iterdir()
            if d.is_dir() and not d.is_symlink()
        )

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

    def _load_job_directory(self, jobdir):

        dfile, mfile = self._final_data_and_model(self._basepath / jobdir)

        sdfile = self._read_star_file(jobdir, dfile)
        smfile = self._read_star_file(jobdir, mfile)

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

    def _final_data_and_model(self, job_path):
        number_list = [entry.stem[6:9] for entry in job_path.glob("run_it*.star")]
        last_iteration_number = max(
            (int(n) for n in number_list if n.isnumeric()), default=0
        )
        if not last_iteration_number:
            raise ValueError(f"No result files found in {job_path}")
        data_file = job_path / f"run_it{last_iteration_number:03d}_data.star"
        model_file = job_path / f"run_it{last_iteration_number:03d}_model.star"
        for check_file in (data_file, model_file):
            if not check_file.exists():
                raise ValueError(f"File {check_file} missing from job directory")
        return data_file, model_file

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

    def top_twenty(self, dictionary):
        return_dict = {}
        for item in dictionary:
            temp_list = sorted(dictionary[item], key=attrgetter("class_distribution"))[
                -20:
            ]
            temp_list.reverse()
            return_dict[item] = temp_list
        return return_dict

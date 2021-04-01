from collections import namedtuple
from collections import Counter
from relion._parser.jobtype import JobType


Class3DParticleClass = namedtuple(
    "Class3DParticleClass",
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

Class3DParticleClass.__doc__ = "3D Classification stage."
Class3DParticleClass.particle_sum.__doc__ = "Sum of all particles in the class. Gives a tuple with the class number first, then the particle sum."
Class3DParticleClass.reference_image.__doc__ = "Reference image."
Class3DParticleClass.class_distribution.__doc__ = (
    "Class Distribution. Proportional to the number of particles per class."
)
Class3DParticleClass.accuracy_rotations.__doc__ = "Accuracy rotations."
Class3DParticleClass.accuracy_translations_angst.__doc__ = (
    "Accuracy translations angst."
)
Class3DParticleClass.estimated_resolution.__doc__ = "Estimated resolution."
Class3DParticleClass.overall_fourier_completeness.__doc__ = (
    "Overall Fourier completeness."
)


class Class3D(JobType):
    def __eq__(self, other):
        if isinstance(other, Class3D):  # check this
            return self._basepath == other._basepath
        return False

    def __hash__(self):
        return hash(("relion._parser.Class3D", self._basepath))

    def __repr__(self):
        return f"Class3D({repr(str(self._basepath))})"

    def __str__(self):
        return f"<Class3D parser at {self._basepath}>"

    @property
    def job_number(self):
        jobs = sorted(x.name for x in self._basepath.iterdir())
        return jobs

    def _load_job_directory(self, jobdir):

        dfile, mfile = self._final_data_and_model(jobdir)

        sdfile = self._read_star_file(jobdir, dfile)
        smfile = self._read_star_file(jobdir, mfile)

        info_table = self._find_table_from_column_name("_rlnClassDistribution", smfile)

        class_distribution = self.parse_star_file(
            "_rlnClassDistribution", smfile, info_table
        )
        accuracy_rotations = self.parse_star_file(
            "_rlnAccuracyRotations", smfile, info_table
        )
        accuracy_translations_angst = self.parse_star_file(
            "_rlnAccuracyTranslationsAngst", smfile, info_table
        )
        estimated_resolution = self.parse_star_file(
            "_rlnEstimatedResolution", smfile, info_table
        )
        overall_fourier_completeness = self.parse_star_file(
            "_rlnOverallFourierCompleteness", smfile, info_table
        )
        reference_image = self.parse_star_file("_rlnReferenceImage", smfile, info_table)

        class_numbers = self.parse_star_file("_rlnClassNumber", sdfile, info_table)
        particle_sum = self._sum_all_particles(class_numbers)
        int_particle_sum = [(int(name), value) for name, value in particle_sum.items()]
        checked_particle_list = self._class_checker(
            sorted(int_particle_sum), len(reference_image)
        )

        particle_class_list = []
        for j in range(len(reference_image)):
            particle_class_list.append(
                Class3DParticleClass(
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
        number_list = [
            entry.stem[6:9]
            for entry in (self._basepath / job_path).glob("run_it*.star")
        ]
        last_iteration_number = max(
            (int(n) for n in number_list if n.isnumeric()), default=0
        )
        if not last_iteration_number:
            raise ValueError(f"No result files found in {job_path}")
        data_file = f"run_it{last_iteration_number:03d}_data.star"
        model_file = f"run_it{last_iteration_number:03d}_model.star"
        for check_file in (
            self._basepath / job_path / data_file,
            self._basepath / job_path / model_file,
        ):
            if not check_file.exists():
                raise ValueError(f"File {check_file} missing from job directory")
        return data_file, model_file

    def _class_checker(
        self, tuple_list, length
    ):  # Makes sure every class has a number of associated particles
        for i in range(1, length):
            if i not in tuple_list[i - 1]:
                tuple_list.insert(i - 1, (i, 0))
                print("No values found for class", i)
        return tuple_list

    def _count_all(self, list):
        count = Counter(list)
        return count

    def _sum_all_particles(self, list):
        counted = self._count_all(list)
        return counted

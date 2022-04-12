from __future__ import annotations

import logging
import pathlib
from collections import Counter, namedtuple
from operator import attrgetter

from relion._parser.jobtype import JobType

logger = logging.getLogger("relion._parser.class2D")

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
        "job",
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
Class2DParticleClass.job.__doc__ = "Job number of the Class2D job."


class Class2D(JobType):
    def __eq__(self, other):
        if isinstance(other, Class2D):  # check this
            return self._basepath == other._basepath
        return False

    def __hash__(self):
        return hash(("relion._parser.Class2D", self._basepath))

    def __repr__(self):
        return f"Class2D({repr(str(self._basepath))})"

    def __str__(self):
        return f"<Class2D parser at {self._basepath}>"

    def _load_job_directory(self, jobdir):

        try:
            dfile, mfile = self._final_data_and_model(jobdir)
        except ValueError as e:
            logger.debug(
                f"The exception {e} was caught while trying to get data and model files. Returning an empty list",
                exc_info=True,
            )
            return []

        try:
            sdfile = self._read_star_file(jobdir, dfile)
            smfile = self._read_star_file(jobdir, mfile)
        except (FileNotFoundError, OSError, RuntimeError, ValueError):
            logger.debug(
                "gemmi could not open file while trying to get data and model files. Returning an empty list",
                exc_info=True,
            )
            return []

        info_table = self._find_table_from_column_name("_rlnClassDistribution", smfile)
        if info_table is None:
            logger.debug(f"_rlnClassDistribution not found in file {mfile}")
            return []

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
        # something probably went wrong with file reading if this is the case
        # return empty list and hope to recover later
        if not int_particle_sum:
            return []
        try:
            checked_particle_list = self._class_checker(
                sorted(int_particle_sum), len(reference_image)
            )
        except IndexError:
            logger.debug(
                f"IndexErorr encountered in _class_checker for {jobdir}", exc_info=True
            )
            return []

        if len(reference_image) != len(checked_particle_list):
            logger.debug(
                f"Number of reference images did not match number of classes for {jobdir}"
            )

        particle_class_list = []

        try:
            for j in range(len(reference_image)):
                particle_class_list.append(
                    Class2DParticleClass(
                        checked_particle_list[j],
                        str(self._basepath.parent / reference_image[j].split("@")[1]),
                        float(class_distribution[j]),
                        accuracy_rotations[j],
                        accuracy_translations_angst[j],
                        estimated_resolution[j],
                        overall_fourier_completeness[j],
                        jobdir,
                    )
                )
        except IndexError:
            logger.debug(
                "An IndexError was encountered while collecting 2D classification data: there was possibly a mismatch between data from different files"
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
        if not tuple_list:
            raise IndexError
        for i in range(1, length + 1):
            try:
                if i not in tuple_list[i - 1]:
                    tuple_list.insert(i - 1, (i, 0))
                    # print("No values found for class", i)
            except IndexError:
                tuple_list.insert(i - 1, (i, 0))
                # print("No values found for class", i)
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

    @staticmethod
    def db_unpack(particle_class):
        def _get_img_mod_time(img):
            if not img:
                return None
            imgpath = pathlib.Path(img)
            if not imgpath.exists():
                return None
            return imgpath.stat().st_mtime

        res = [
            {
                "type": "2D",
                "class_number": cl.particle_sum[0],
                "particles_per_class": cl.particle_sum[1],
                "rotation_accuracy": cl.accuracy_rotations,
                "translation_accuracy": cl.accuracy_translations_angst,
                "estimated_resolution": cl.estimated_resolution,
                "overall_fourier_completeness": cl.overall_fourier_completeness,
                "class_image_full_path": cl.reference_image.replace(
                    ".mrcs", f"_{cl.particle_sum[0]}.jpeg"
                ),
                "job_string": cl.job,
                "class_images_stack": cl.reference_image,
                "class_images_modification_time": _get_img_mod_time(cl.reference_image),
                "class_distribution": cl.class_distribution,
            }
            for cl in particle_class
        ]
        return res

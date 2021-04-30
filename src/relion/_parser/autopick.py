from collections import namedtuple
from relion._parser.jobtype import JobType

ParticlePickerInfo = namedtuple(
    "ParticlePickerInfo",
    [
        "number_of_particles",
    ],
)


class AutoPick(JobType):
    def __eq__(self, other):
        if isinstance(other, AutoPick):  # check this
            return self._basepath == other._basepath
        return False

    def __hash__(self):
        return hash(("relion._parser.AutoPick", self._basepath))

    def __repr__(self):
        return f"AutoPick({repr(str(self._basepath))})"

    def __str__(self):
        return f"<AutoPick parser at {self._basepath}>"

    def _load_job_directory(self, jobdir):
        file = self._read_star_file(jobdir, "summary.star")

        info_table = self._find_table_from_column_name("_rlnGroupNrParticles", file)

        all_particles = self.parse_star_file("_rlnGroupNrParticles", file, info_table)
        num_particles = sum([int(n) for n in all_particles])

        return [ParticlePickerInfo(num_particles)]

    @staticmethod
    def for_cache(partpickinfo):
        return str(partpickinfo.number_of_particles)

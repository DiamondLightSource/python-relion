from collections import namedtuple
from relion._parser.jobtype import JobType
import logging

logger = logging.getLogger("relion._parser.autopick")

ParticlePickerInfo = namedtuple(
    "ParticlePickerInfo",
    [
        "number_of_particles",
        "first_micrograph_name",
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
        try:
            file = self._read_star_file(jobdir, "summary.star")
        except (RuntimeError, FileNotFoundError):
            return []

        info_table = self._find_table_from_column_name("_rlnGroupNrParticles", file)
        if info_table is None:
            logger.debug(f"_rlnGroupNrParticles not found in file {file}")
            return []

        all_particles = self.parse_star_file("_rlnGroupNrParticles", file, info_table)
        num_particles = sum([int(n) for n in all_particles])

        first_mc_micrograph = self.parse_star_file(
            "_rlnMicrographName", file, info_table
        )[0]

        return [ParticlePickerInfo(num_particles, first_mc_micrograph)]

    @staticmethod
    def for_cache(partpickinfo):
        return str(partpickinfo.number_of_particles)

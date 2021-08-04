import logging
from collections import namedtuple

from relion._parser.jobtype import JobType

logger = logging.getLogger("relion._parser.autopick")

ParticlePickerInfo = namedtuple(
    "ParticlePickerInfo",
    [
        "number_of_particles",
        "micrograph_full_path",
        "first_micrograph_name",
        "job",
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
        except (RuntimeError, FileNotFoundError, OSError, ValueError):
            return []

        info_table = self._find_table_from_column_name("_rlnGroupNrParticles", file)
        if info_table is None:
            logger.debug(f"_rlnGroupNrParticles not found in file {file}")
            return []

        all_particles = self.parse_star_file("_rlnGroupNrParticles", file, info_table)
        # num_particles = sum([int(n) for n in all_particles])

        mc_micrographs = self.parse_star_file("_rlnMicrographName", file, info_table)

        first_mc_micrograph = mc_micrographs[0]

        particle_picker_info = []
        for mic, np in zip(mc_micrographs, all_particles):
            particle_picker_info.append(
                ParticlePickerInfo(int(np), mic, first_mc_micrograph, jobdir)
            )

        return particle_picker_info

    @staticmethod
    def for_cache(partpickinfo):
        return str(partpickinfo.number_of_particles)

    @staticmethod
    def db_unpack(partpickinfo):
        res = [
            {
                "number_of_particles": pi.number_of_particles,
                "job_string": pi.job,
                "micrograph_full_path": pi.micrograph_full_path,
            }
            for pi in partpickinfo
        ]
        return res

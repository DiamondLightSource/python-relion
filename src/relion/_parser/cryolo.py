import logging
import pathlib

from relion._parser.autopick import ParticlePickerInfo
from relion._parser.jobtype import JobType

logger = logging.getLogger("relion._parser.cryolo")


class Cryolo(JobType):
    def __eq__(self, other):
        if isinstance(other, Cryolo):  # check this
            return self._basepath == other._basepath
        return False

    def __hash__(self):
        return hash(("relion._parser.Cryolo", self._basepath))

    def __repr__(self):
        return f"Cryolo({repr(str(self._basepath))})"

    def __str__(self):
        return f"<Cryolo parser at {self._basepath}>"

    @property
    def jobs(self):
        return sorted(
            d.resolve().name
            for d in self._basepath.iterdir()
            if d.is_symlink() and "crYOLO" in str(d)
        )

    def _load_job_directory(self, jobdir):
        num_particles = 0
        particles_per_micrograph = {}
        first_mic = ""
        for star_file in (self._basepath / jobdir / "Movies").glob("**/*"):
            if star_file.is_file() and "gain" not in str(star_file):
                try:
                    file = self._read_star_file(
                        jobdir, star_file.relative_to(self._basepath / jobdir)
                    )
                except (RuntimeError, FileNotFoundError, OSError, ValueError):
                    return []

                info_table = self._find_table_from_column_name("_rlnCoordinateX", file)
                if info_table is None:
                    logger.debug(f"_rlnCoordinateX not found in file {file}")
                    return []

                all_particles = self.parse_star_file(
                    "_rlnCoordinateX", file, info_table
                )
                if particles_per_micrograph == {}:
                    first_mic = star_file
                particles_per_micrograph[star_file] = len(all_particles)
                num_particles += len(all_particles)

        # all of this just tracks back to a micrograph name from the MotionCorrection job
        try:
            jobfile = self._read_star_file(jobdir, "job.star")
        except (RuntimeError, FileNotFoundError, OSError, ValueError):
            return []
        info_table = self._find_table_from_column_name("_rlnJobOptionVariable", jobfile)
        if info_table is None:
            logger.debug(f"_rlnJobOptionVariable not found in file {jobfile}")
            return []
        variables = self.parse_star_file("_rlnJobOptionVariable", jobfile, info_table)
        inmicindex = variables.index("in_mic")
        ctffilename = pathlib.Path(
            self.parse_star_file("_rlnJobOptionValue", jobfile, info_table)[inmicindex]
        )
        ctffile = self._read_star_file_from_proj_dir(
            ctffilename.parts[0], ctffilename.relative_to(ctffilename.parts[0])
        )
        info_table = self._find_table_from_column_name("_rlnMicrographName", ctffile)
        if info_table is None:
            logger.debug(f"_rlnMicrographName not found in file {ctffile}")
            return []
        micrograph_names = self.parse_star_file(
            "_rlnMicrographName", ctffile, info_table
        )

        particle_picker_info = []
        for mic, num_particles in particles_per_micrograph.items():
            particle_picker_info.append(
                ParticlePickerInfo(
                    num_particles,
                    self._get_micrograph_name(mic, micrograph_names, jobdir),
                    str(first_mic.relative_to(self._basepath / jobdir)).replace(
                        "_autopick.star", ".mrc"
                    ),
                    jobdir,
                )
            )

        return particle_picker_info

    def _get_micrograph_name(self, micrograph, micrograph_names, jobdir):
        for x in micrograph_names:
            if (
                str(micrograph.relative_to(self._basepath / jobdir)).replace(
                    "_autopick.star", ".mrc"
                )
                in x
            ):
                return x
        return None

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
                "first_motion_correction_micrograph": pi.first_micrograph_name,
            }
            for pi in partpickinfo
        ]
        return res

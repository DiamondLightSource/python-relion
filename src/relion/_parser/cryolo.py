from relion._parser.jobtype import JobType
from relion._parser.autopick import ParticlePickerInfo
import pathlib


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
        for star_file in (self._basepath / jobdir / "picked_stars").glob("*"):
            try:
                file = self._read_star_file(
                    jobdir, pathlib.Path("picked_stars") / star_file.name
                )
            except (RuntimeError, IOError):
                return []

            info_table = self._find_table_from_column_name("_rlnCoordinateX", file)

            all_particles = self.parse_star_file("_rlnCoordinateX", file, info_table)
            num_particles += len(all_particles)

        # all of this just tracks back to a micrograph name from the MotionCorrection job
        try:
            jobfile = self._read_star_file(jobdir, "job.star")
        except (RuntimeError, IOError):
            return []
        info_table = self._find_table_from_column_name("_rlnJobOptionVariable", jobfile)
        variables = self.parse_star_file("_rlnJobOptionVariable", jobfile, info_table)
        inmicindex = variables.index("in_mic")
        ctffilename = pathlib.Path(
            self.parse_star_file("_rlnJobOptionValue", jobfile, info_table)[inmicindex]
        )
        ctffile = self._read_star_file_from_proj_dir(
            ctffilename.parts[0], ctffilename.relative_to(ctffilename.parts[0])
        )
        info_table = self._find_table_from_column_name("_rlnMicrographName", ctffile)
        micrograph_name = self.parse_star_file(
            "_rlnMicrographName", ctffile, info_table
        )[0]

        return [ParticlePickerInfo(num_particles, micrograph_name)]

    @staticmethod
    def for_cache(partpickinfo):
        return str(partpickinfo.number_of_particles)

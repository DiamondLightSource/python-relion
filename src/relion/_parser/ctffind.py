from collections import namedtuple
from relion._parser.jobtype import JobType

CTFMicrograph = namedtuple(
    "CTFMicrograph",
    [
        "micrograph_name",
        "astigmatism",
        "defocus_u",
        "defocus_v",
        "defocus_angle",
        "max_resolution",
        "fig_of_merit",
    ],
)
CTFMicrograph.__doc__ = "Contrast Transfer Function stage."
CTFMicrograph.astigmatism.__doc__ = "Estimated astigmatism. Units angstrom (A)."
CTFMicrograph.micrograph_name.__doc__ = "Micrograph name. Useful for reference."
CTFMicrograph.defocus_u.__doc__ = (
    "Averaged with Defocus V to give estimated defocus. Units angstrom (A)."
)
CTFMicrograph.defocus_v.__doc__ = (
    "Averaged with Defocus U to give estimated defocus. Units angstrom (A)."
)
CTFMicrograph.defocus_angle.__doc__ = "Estimated angle of astigmatism."
CTFMicrograph.max_resolution.__doc__ = (
    "Maximum resolution that the software can detect. Units angstrom (A)."
)
CTFMicrograph.fig_of_merit.__doc__ = (
    "Figure of merit/CC/correlation value. Confidence of the defocus estimation."
)


class CTFFind(JobType):
    def __eq__(self, other):
        if isinstance(other, CTFFind):  # check this
            return self._basepath == other._basepath
        return False

    def __hash__(self):
        return hash(("relion._parser.CTFFind", self._basepath))

    def __repr__(self):
        return f"CTFFind({repr(str(self._basepath))})"

    def __str__(self):
        return f"<CTFFind parser at {self._basepath}>"

    @property
    def job_number(self):
        jobs = [x.name for x in self._basepath.iterdir()]
        return jobs

    def _load_job_directory(self, jobdir):
        file = self._read_star_file(jobdir, "micrographs_ctf.star")

        info_table = self._find_table_from_column_name("_rlnCtfAstigmatism", file)

        astigmatism = self.parse_star_file("_rlnCtfAstigmatism", file, info_table)
        defocus_u = self.parse_star_file("_rlnDefocusU", file, info_table)
        defocus_v = self.parse_star_file("_rlnDefocusV", file, info_table)
        defocus_angle = self.parse_star_file("_rlnDefocusAngle", file, info_table)
        max_resolution = self.parse_star_file("_rlnCtfMaxResolution", file, info_table)
        fig_of_merit = self.parse_star_file("_rlnCtfFigureOfMerit", file, info_table)

        micrograph_name = self.parse_star_file("_rlnMicrographName", file, info_table)

        micrograph_list = []
        for j in range(len(micrograph_name)):
            micrograph_list.append(
                CTFMicrograph(
                    micrograph_name[j],
                    astigmatism[j],
                    defocus_u[j],
                    defocus_v[j],
                    defocus_angle[j],
                    max_resolution[j],
                    fig_of_merit[j],
                )
            )
        return micrograph_list

    @staticmethod
    def for_cache(ctfmicrograph):
        return str(ctfmicrograph.micrograph_name)

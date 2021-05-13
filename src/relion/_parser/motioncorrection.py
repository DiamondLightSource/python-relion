from collections import namedtuple
from relion._parser.jobtype import JobType

MCMicrograph = namedtuple(
    "MCMicrograph",
    [
        "micrograph_name",
        "micrograph_number",
        "total_motion",
        "early_motion",
        "late_motion",
    ],
)

MCMicrograph.__doc__ = "Motion Correction stage."
MCMicrograph.micrograph_name.__doc__ = "Micrograph name. Useful for reference."
MCMicrograph.micrograph_number.__doc__ = "Micrograph number: sequential in time."
MCMicrograph.total_motion.__doc__ = (
    "Total motion. The amount the sample moved during exposure. Units angstrom (A)."
)
MCMicrograph.early_motion.__doc__ = "Early motion."
MCMicrograph.late_motion.__doc__ = "Late motion."


class MotionCorr(JobType):
    def __eq__(self, other):
        if isinstance(other, MotionCorr):  # check this
            return self._basepath == other._basepath
        return False

    def __hash__(self):
        return hash(("relion._parser.MotionCorr", self._basepath))

    def __repr__(self):
        return f"MotionCorr({repr(str(self._basepath))})"

    def __str__(self):
        return f"<MotionCorr parser at {self._basepath}>"

    def _load_job_directory(self, jobdir):
        try:
            file = self._read_star_file(jobdir, "corrected_micrographs.star")
        except RuntimeError:
            return []

        info_table = self._find_table_from_column_name("_rlnAccumMotionTotal", file)
        if info_table is None:
            return []

        accum_motion_total = self.parse_star_file(
            "_rlnAccumMotionTotal", file, info_table
        )
        accum_motion_late = self.parse_star_file(
            "_rlnAccumMotionLate", file, info_table
        )
        accum_motion_early = self.parse_star_file(
            "_rlnAccumMotionEarly", file, info_table
        )
        micrograph_name = self.parse_star_file("_rlnMicrographName", file, info_table)

        micrograph_list = []
        for j in range(len(micrograph_name)):
            micrograph_list.append(
                MCMicrograph(
                    micrograph_name[j],
                    j + 1,
                    accum_motion_total[j],
                    accum_motion_early[j],
                    accum_motion_late[j],
                )
            )
        return micrograph_list

    @staticmethod
    def for_cache(mcmicrograph):
        return str(mcmicrograph.micrograph_name)

    @staticmethod
    def for_validation(mcmicrograph):
        return {str(mcmicrograph.micrograph_name): mcmicrograph.micrograph_number}

    # this allows an MCMicrograph object to be copied but with some attributes changed
    @staticmethod
    def mutate_result(mcmicrograph, **kwargs):
        attr_names_list = MCMicrograph._fields
        attr_list = [
            kwargs.get(name, getattr(mcmicrograph, name)) for name in attr_names_list
        ]
        return MCMicrograph(*attr_list)

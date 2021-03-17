from collections import namedtuple
from relion._parser.jobtype import JobType

MCMicrograph = namedtuple(
    "MCMicrograph", ["micrograph_name", "total_motion", "early_motion", "late_motion"]
)

MCMicrograph.__doc__ = "Motion Correction stage."
MCMicrograph.micrograph_name.__doc__ = "Micrograph name. Useful for reference."
MCMicrograph.total_motion.__doc__ = (
    "Total motion. The amount the sample moved during exposure. Units angstrom (A)."
)
MCMicrograph.early_motion.__doc__ = "Early motion."
MCMicrograph.late_motion.__doc__ = "Late motion."


class MotionCorr(JobType):
    def __hash__(self):
        return hash(("relion._parser.MotionCorr", self._basepath))

    def __repr__(self):
        return f"MotionCorr({repr(str(self._basepath))})"

    def __str__(self):
        return f"<MotionCorr parser at {self._basepath}>"

    def _load_job_directory(self, jobdir):
        file = self._read_star_file(jobdir, "corrected_micrographs.star")
        accum_motion_total = self.parse_star_file("_rlnAccumMotionTotal", file, 1)
        accum_motion_late = self.parse_star_file("_rlnAccumMotionLate", file, 1)
        accum_motion_early = self.parse_star_file("_rlnAccumMotionEarly", file, 1)
        micrograph_name = self.parse_star_file("_rlnMicrographName", file, 1)

        micrograph_list = []
        for j in range(len(micrograph_name)):
            micrograph_list.append(
                MCMicrograph(
                    micrograph_name[j],
                    accum_motion_total[j],
                    accum_motion_early[j],
                    accum_motion_late[j],
                )
            )
        return micrograph_list

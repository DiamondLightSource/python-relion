from __future__ import annotations

import logging
from collections import namedtuple

from relion._parser.jobtype import JobType

logger = logging.getLogger("relion._parser.motioncorrection")

MCMicrograph = namedtuple(
    "MCMicrograph",
    [
        "micrograph_name",
        "micrograph_snapshot_full_path",
        "micrograph_number",
        "total_motion",
        "early_motion",
        "late_motion",
        "micrograph_timestamp",
        "drift_data",
    ],
)

MCMicrograph.__doc__ = "Motion Correction stage."
MCMicrograph.micrograph_name.__doc__ = "Micrograph name. Useful for reference."
MCMicrograph.micrograph_snapshot_full_path.__doc__ = (
    "Path to jpeg of the motion corrected micrograph."
)
MCMicrograph.micrograph_number.__doc__ = "Micrograph number: sequential in time."
MCMicrograph.total_motion.__doc__ = (
    "Total motion. The amount the sample moved during exposure. Units angstrom (A)."
)
MCMicrograph.early_motion.__doc__ = "Early motion."
MCMicrograph.late_motion.__doc__ = "Late motion."
MCMicrograph.micrograph_timestamp.__doc__ = (
    "Time stamp at which the micrograph was created."
)


MCMicrographDrift = namedtuple(
    "MCMicrographDrift",
    [
        "frame",
        "deltaX",
        "deltaY",
    ],
)

MCDriftCacheRecord = namedtuple(
    "MCDriftCacheRecord",
    [
        "data",
        "file_size",
        "movie_name",
    ],
)


class MotionCorr(JobType):
    def __init__(self, path, drift_cache=None):
        super().__init__(path)
        if drift_cache is None:
            self._drift_cache = {}
        else:
            self._drift_cache = drift_cache

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
        except (FileNotFoundError, OSError, RuntimeError, ValueError):
            return []

        info_table = self._find_table_from_column_name("_rlnAccumMotionTotal", file)
        if info_table is None:
            logger.debug(f"_rlnAccumMotionTotal not found in file {file}")
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
            drift_data, movie_name = self.collect_drift_data(micrograph_name[j], jobdir)
            if movie_name:
                try:
                    movie_creation_time = (
                        (self._basepath.parent / movie_name).resolve().stat().st_ctime
                    )
                except FileNotFoundError:
                    logger.debug(
                        f"failed to find movie {self._basepath.parent / movie_name} so using default timestamp"
                    )
                    movie_creation_time = None
            else:
                movie_creation_time = None
            micrograph_list.append(
                MCMicrograph(
                    micrograph_name[j],
                    str(self._basepath.parent / micrograph_name[j]).replace(
                        ".mrc", ".jpeg"
                    ),
                    j + 1,
                    accum_motion_total[j],
                    accum_motion_early[j],
                    accum_motion_late[j],
                    movie_creation_time,
                    drift_data,
                )
            )
        return micrograph_list

    def collect_drift_data(self, mic_name, jobdir):
        drift_data = []
        drift_star_file_path = mic_name.split(jobdir + "/")[-1].replace("mrc", "star")
        if self._drift_cache.get(jobdir):
            if self._drift_cache[jobdir].get(mic_name):
                try:
                    if (
                        self._drift_cache[jobdir][mic_name].file_size
                        == (self._basepath / jobdir / drift_star_file_path)
                        .stat()
                        .st_size
                    ):
                        return (
                            self._drift_cache[jobdir][mic_name].data,
                            self._drift_cache[jobdir][mic_name].movie_name,
                        )
                except FileNotFoundError:
                    logger.debug(
                        "Could not find expected file containing drift data",
                        exc_info=True,
                    )
                    return [], ""
        else:
            self._drift_cache[jobdir] = {}
        try:
            drift_star_file = self._read_star_file(jobdir, drift_star_file_path)
        except (FileNotFoundError, OSError, RuntimeError, ValueError):
            return drift_data, ""
        try:
            info_table = self._find_table_from_column_name(
                "_rlnMicrographFrameNumber", drift_star_file
            )
            movie_table = 0
        except (FileNotFoundError, OSError, RuntimeError, ValueError):
            return drift_data, ""
        if info_table is None:
            logger.debug(
                f"_rlnMicrographFrameNumber or _rlnMicrographMovieName not found in file {drift_star_file}"
            )
            return drift_data, ""
        frame_numbers = self.parse_star_file(
            "_rlnMicrographFrameNumber", drift_star_file, info_table
        )
        deltaxs = self.parse_star_file(
            "_rlnMicrographShiftX", drift_star_file, info_table
        )
        deltays = self.parse_star_file(
            "_rlnMicrographShiftY", drift_star_file, info_table
        )
        movie_name = self.parse_star_file_pair(
            "_rlnMicrographMovieName", drift_star_file, movie_table
        )
        for f, dx, dy in zip(frame_numbers, deltaxs, deltays):
            drift_data.append(MCMicrographDrift(int(f), float(dx), float(dy)))
        try:
            self._drift_cache[jobdir][mic_name] = MCDriftCacheRecord(
                drift_data,
                (self._basepath / jobdir / drift_star_file_path).stat().st_size,
                movie_name,
            )
        except FileNotFoundError:
            return [], ""
        return drift_data, movie_name

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

    @staticmethod
    def db_unpack(micrograph_list):
        res = [
            {
                "micrograph_full_path": micrograph.micrograph_name,
                "total_motion": micrograph.total_motion,
                "early_motion": micrograph.early_motion,
                "late_motion": micrograph.late_motion,
                "average_motion_per_frame": (
                    float(micrograph.total_motion) / len(micrograph.drift_data)
                ),
                "image_number": micrograph.micrograph_number,
                "micrograph_snapshot_full_path": micrograph.micrograph_snapshot_full_path,
                "drift_data": micrograph.drift_data,
                "created_time_stamp": micrograph.micrograph_timestamp,
            }
            for micrograph in micrograph_list
        ]
        return res

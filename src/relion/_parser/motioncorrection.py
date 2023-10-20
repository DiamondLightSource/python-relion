from __future__ import annotations

import logging
from collections import namedtuple
from pathlib import Path

import numpy as np
import plotly.express as px

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
        "average_motion_per_frame",
        "micrograph_timestamp",
        "drift_plot_full_path",
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
    def __init__(self, path):
        super().__init__(path)

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
            (
                number_of_frames,
                drift_plot_full_path,
                movie_name,
            ) = self.collect_drift_data(micrograph_name[j], jobdir)
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
                    float(accum_motion_total[j]) / number_of_frames,
                    movie_creation_time,
                    drift_plot_full_path,
                )
            )
        return micrograph_list

    def collect_drift_data(self, mic_name, jobdir):
        drift_star_file_path = mic_name.split(jobdir + "/")[-1].replace("mrc", "star")
        try:
            drift_star_file = self._read_star_file(jobdir, drift_star_file_path)
        except (FileNotFoundError, OSError, RuntimeError, ValueError):
            return 1, "", ""
        try:
            info_table = self._find_table_from_column_name(
                "_rlnMicrographFrameNumber", drift_star_file
            )
            movie_table = 0
        except (FileNotFoundError, OSError, RuntimeError, ValueError):
            return 1, "", ""
        if info_table is None:
            logger.debug(
                f"_rlnMicrographFrameNumber or _rlnMicrographMovieName not found in file {drift_star_file}"
            )
            return 1, "", ""
        deltaxs = self.parse_star_file(
            "_rlnMicrographShiftX", drift_star_file, info_table
        )
        deltays = self.parse_star_file(
            "_rlnMicrographShiftY", drift_star_file, info_table
        )
        movie_name = self.parse_star_file_pair(
            "_rlnMicrographMovieName", drift_star_file, movie_table
        )
        try:
            fig = px.scatter(
                x=np.array(deltaxs, dtype=float), y=np.array(deltays, dtype=float)
            )
            drift_plot_name = Path(mic_name).stem + "_drift_plot.json"
            drift_plot_full_path = Path(mic_name).parent / drift_plot_name
            fig.write_json(drift_plot_full_path)
        except FileNotFoundError:
            return 1, "", ""
        return len(deltaxs), drift_plot_full_path, movie_name

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
                "average_motion_per_frame": micrograph.average_motion_per_frame,
                "image_number": micrograph.micrograph_number,
                "micrograph_snapshot_full_path": micrograph.micrograph_snapshot_full_path,
                "drift_plot_full_path": micrograph.drift_plot_full_path,
                "created_time_stamp": micrograph.micrograph_timestamp,
            }
            for micrograph in micrograph_list
        ]
        return res

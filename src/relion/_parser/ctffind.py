from __future__ import annotations

import logging
from collections import namedtuple

from relion._parser.jobtype import JobType

logger = logging.getLogger("relion._parser.ctffind")

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
        "amp_contrast",
        "diagnostic_plot_path",
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
CTFMicrograph.amp_contrast.__doc__ = "Amplitude contrast."
CTFMicrograph.diagnostic_plot_path.__doc__ = (
    "Path to the CTF diagnostic (fit/data comparison) plot (jpeg)."
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
        try:
            file = self._read_star_file(jobdir, "micrographs_ctf.star")
        except (FileNotFoundError, OSError, RuntimeError, ValueError):
            return []

        info_table = self._find_table_from_column_name("_rlnCtfAstigmatism", file)
        if info_table is None:
            return []

        astigmatism = self.parse_star_file("_rlnCtfAstigmatism", file, info_table)
        defocus_u = self.parse_star_file("_rlnDefocusU", file, info_table)
        defocus_v = self.parse_star_file("_rlnDefocusV", file, info_table)
        defocus_angle = self.parse_star_file("_rlnDefocusAngle", file, info_table)
        max_resolution = self.parse_star_file("_rlnCtfMaxResolution", file, info_table)
        fig_of_merit = self.parse_star_file("_rlnCtfFigureOfMerit", file, info_table)

        micrograph_name = self.parse_star_file("_rlnMicrographName", file, info_table)
        ctf_img_path = self.parse_star_file("_rlnCtfImage", file, info_table)

        info_table = self._find_table_from_column_name("_rlnAmplitudeContrast", file)

        amp_contrast = self.parse_star_file("_rlnAmplitudeContrast", file, info_table)

        micrograph_list = []
        for j in range(len(micrograph_name)):
            plot_path = (
                str(self._basepath.parent / ctf_img_path[j])
                .split(":")[0]
                .replace(".ctf", ".jpeg")
            )
            micrograph_list.append(
                CTFMicrograph(
                    micrograph_name[j],
                    astigmatism[j],
                    defocus_u[j],
                    defocus_v[j],
                    defocus_angle[j],
                    max_resolution[j],
                    fig_of_merit[j],
                    amp_contrast[0],
                    plot_path,
                )
            )
        return micrograph_list

    @staticmethod
    def for_cache(ctfmicrograph):
        return str(ctfmicrograph.micrograph_name)

    @staticmethod
    def db_unpack(micrograph_list):
        res = [
            {
                "micrograph_full_path": micrograph.micrograph_name,
                "astigmatism": micrograph.astigmatism,
                "astigmatism_angle": micrograph.defocus_angle,
                "estimated_resolution": micrograph.max_resolution,
                "estimated_defocus": (
                    float(micrograph.defocus_u) + float(micrograph.defocus_v)
                )
                / 2,
                "cc_value": micrograph.fig_of_merit,
                "amplitude_contrast": micrograph.amp_contrast,
                "fft_theoretical_full_path": micrograph.diagnostic_plot_path,
            }
            for micrograph in micrograph_list
        ]
        return res

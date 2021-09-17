# import pathlib
import csv
import logging
from collections import namedtuple

from relion._parser.jobtype import JobType

logger = logging.getLogger("relion._parser.relativeicethickness")

RelativeIceThicknessMicrograph = namedtuple(
    "RelativeIceThicknessMicrograph", ["minimum", "q1", "median", "q3", "maximum"]
)

RelativeIceThicknessMicrograph.__doc__ = "Relative ice thickness data for a micrograph."
RelativeIceThicknessMicrograph.minimum.__doc__ = "Minimum ice thickness. Unitless"
RelativeIceThicknessMicrograph.q1.__doc__ = "Quartile 1. Unitless"
RelativeIceThicknessMicrograph.median.__doc__ = "Median ice thickness. Unitless"
RelativeIceThicknessMicrograph.q3.__doc__ = "Quartile 3. Unitless"
RelativeIceThicknessMicrograph.maximum.__doc__ = "Maximum ice thickness. Unitless"


class RelativeIceThickness(JobType):
    def __eq__(self, other):
        if isinstance(other, RelativeIceThickness):
            return self._basepath == other._basepath
        return False

    def __hash__(self):
        return hash(("relion._parser.RelativeIceThickness", self._basepath))

    def __repr__(self):
        return f"RelativeIceThickness({repr(str(self._basepath))})"

    def __str__(self):
        return f"<RelativeIceThickness parser at {self._basepath}>"

    @property
    def job_number(self):
        jobs = [x.name for x in self._basepath.iterdir()]
        return jobs

    def _load_job_directory(self, jobdir):
        try:
            ice_dict = self.csv_to_dict(self._basepath / jobdir / "five_figures.csv")
        except (FileNotFoundError, OSError, RuntimeError, ValueError):
            return []
        list_micrograph_path = ice_dict["path"]
        list_minimum = ice_dict["min"]
        list_q1 = ice_dict["q1"]
        list_median = ice_dict["median"]
        list_q3 = ice_dict["q3"]
        list_maximum = ice_dict["maximum"]

        micrograph_list = []
        for j in range(len(list_micrograph_path)):
            micrograph_list.append(
                RelativeIceThicknessMicrograph(
                    list_minimum[j],
                    list_q1[j],
                    list_median[j],
                    list_q3[j],
                    list_maximum[j],
                )
            )
        return micrograph_list

    def csv_to_dict(self, file_path):
        with open(file_path, newline="") as csvfile:
            list_row_dicts = list(csv.DictReader(csvfile))
            combi_dict = {}
            for row_dict in list_row_dicts:
                for k, v in row_dict.items():
                    combi_dict.setdefault(k, []).append(v)
            return combi_dict

    @staticmethod
    def db_unpack(micrograph_list):
        res = [
            {
                "minimum": micrograph.minimum,
                "q1": micrograph.q1,
                "median": micrograph.median,
                "q3": micrograph.q3,
                "maximum": micrograph.maximum,
            }
            for micrograph in micrograph_list
        ]
        return res

import collections.abc
from gemmi import cif
import os
import functools
from collections import namedtuple

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


class CTFFind(collections.abc.Mapping):
    def __eq__(self, other):
        if isinstance(other, CTFFind):
            return self._basepath == other._basepath
        return False

    def __hash__(self):
        return hash(("relion._parser.CTFFind", self._basepath))

    def __init__(self, path):
        self._basepath = path
        self._jobcache = {}

    def __iter__(self):
        return (x.name for x in self._basepath.iterdir())

    def __len__(self):
        return len(list(self._basepath.iterdir()))

    def __repr__(self):
        return f"CTFFind({repr(str(self._basepath))})"

    def __str__(self):
        return f"<CTFFind parser at {self._basepath}>"

    @property
    def jobs(self):
        return sorted(d.stem for d in self._basepath.iterdir() if d.is_dir())

    def __getitem__(self, key):
        if not isinstance(key, str):
            raise KeyError(f"Invalid argument {key!r}, expected string")
        if key not in self._jobcache:
            job_path = self._basepath / key
            if not job_path.is_dir():
                raise KeyError(
                    f"no job directory present for {key} in {self._basepath}"
                )
            self._jobcache[key] = self._load_job_directory(key)
        return self._jobcache[key]

    @property
    def job_number(self):
        jobs = [x.name for x in self._basepath.iterdir()]
        return jobs

    def _load_job_directory(self, jobdir):
        file = self._read_star_file(jobdir)

        astigmatism = self.parse_star_file("_rlnCtfAstigmatism", file, 1)
        defocus_u = self.parse_star_file("_rlnDefocusU", file, 1)
        defocus_v = self.parse_star_file("_rlnDefocusV", file, 1)
        defocus_angle = self.parse_star_file("_rlnDefocusAngle", file, 1)
        max_resolution = self.parse_star_file("_rlnCtfMaxResolution", file, 1)
        fig_of_merit = self.parse_star_file("_rlnCtfFigureOfMerit", file, 1)

        micrograph_name = self.parse_star_file("_rlnMicrographName", file, 1)

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

    def parse_star_file(self, loop_name, star_doc, block_number):
        data_block = star_doc[block_number]
        values = data_block.find_loop(loop_name)
        values_list = list(values)
        if not values_list:
            print("Warning - no values found for", loop_name)
        return values_list

    @functools.lru_cache(maxsize=None)
    def _read_star_file(self, job_num):
        full_path = self._basepath / job_num / "micrographs_ctf.star"
        gemmi_readable_path = os.fspath(full_path)
        star_doc = cif.read_file(gemmi_readable_path)
        return star_doc

from __future__ import annotations

import logging
import pathlib
from typing import List

from relion._parser.autopick import ParticleCacheRecord, ParticlePickerInfo
from relion._parser.jobtype import JobType

logger = logging.getLogger("relion._parser.cryolo")


class Cryolo(JobType):
    def __init__(self, path, particle_cache=None, seen_star_files=None):
        super().__init__(path)
        self._particle_cache = particle_cache or {}

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

    def _load_job_directory(
        self, jobdir, input_tag: str = "in_mic", star_location: str = "Movies"
    ):
        num_particles = 0
        particles_per_micrograph = {}
        first_mic = ""
        coords = {}
        for star_file in (self._basepath / jobdir / star_location).glob("**/*"):
            if (
                star_file.is_file()
                and "gain" not in str(star_file)
                and "copy" not in str(star_file)
            ):
                part_coords = self._get_particle_info(jobdir, star_file)
                if part_coords:
                    coords[star_file] = part_coords
                    if particles_per_micrograph == {}:
                        first_mic = star_file
                    particles_per_micrograph[star_file] = len(part_coords)
                    num_particles += len(part_coords)

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
        inmicindex = variables.index(input_tag)
        ctffilename = pathlib.Path(
            self.parse_star_file("_rlnJobOptionValue", jobfile, info_table)[
                inmicindex
            ].replace("'", "")
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
        indexed_micrograph_names = {
            pathlib.Path(mn).stem: mn for mn in micrograph_names
        }

        particle_picker_info = []
        for mic, num_particles in particles_per_micrograph.items():
            mc_mic_name = self._get_micrograph_name(mic, indexed_micrograph_names)
            if not mc_mic_name:
                continue
            mic_parts = pathlib.Path(mc_mic_name).parts
            highlighted_micrograph = self._get_highlighted_mircograph(
                jobdir, mc_mic_name, mic_parts
            )
            particle_picker_info.append(
                ParticlePickerInfo(
                    num_particles,
                    mc_mic_name,
                    str(self._basepath.parent / mc_mic_name).replace(".mrc", ".jpeg"),
                    str(first_mic.relative_to(self._basepath / jobdir))
                    .replace("_autopick", "")
                    .replace(".star", ".mrc"),
                    str(highlighted_micrograph),
                    coords[mic],
                    jobdir,
                )
            )

        return particle_picker_info

    def _get_highlighted_mircograph(
        self, jobdir: str, mc_mic_name: str, mic_parts: List[str]
    ) -> pathlib.Path:
        return (
            self._basepath
            / jobdir
            / pathlib.Path(mc_mic_name)
            .relative_to(pathlib.Path(mic_parts[0]) / mic_parts[1])
            .with_suffix(".jpeg")
        )

    def _get_particle_info(self, jobdir, star_file):
        if self._particle_cache.get(jobdir) is None:
            self._particle_cache[jobdir] = {}
        if self._particle_cache[jobdir].get(star_file):
            try:
                if (
                    self._particle_cache[jobdir][star_file].file_size
                    == star_file.stat().st_size
                ):
                    return self._particle_cache[jobdir][star_file].data
            except FileNotFoundError:
                logger.debug(
                    "Could not find expected file containing particle data",
                    exc_info=True,
                )
                return []

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

        all_particles = self.parse_star_file("_rlnCoordinateX", file, info_table)
        ys = self.parse_star_file("_rlnCoordinateY", file, info_table)
        coords = [(x, y) for x, y in zip(all_particles, ys)]
        try:
            self._particle_cache[jobdir][star_file] = ParticleCacheRecord(
                coords,
                star_file.stat().st_size,
            )
        except FileNotFoundError:
            return []
        return coords

    def _get_micrograph_name(self, micrograph, micrograph_names):
        return micrograph_names.get(micrograph.stem.replace("_autopick", ""))

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
                "mc_image_full_path": pi.mc_image_full_path,
                "summary_image_full_path": pi.highlighted_micrograph,
                "particle_coordinates": pi.coordinates,
            }
            for pi in partpickinfo
        ]
        return res


class CryoloAutoPick(Cryolo):
    def __eq__(self, other):
        if isinstance(other, CryoloAutoPick):  # check this
            return self._basepath == other._basepath
        return False

    def __hash__(self):
        return hash(("relion._parser.CryoloAutoPick", self._basepath))

    def __repr__(self):
        return f"CryoloAutoPick({repr(str(self._basepath))})"

    def __str__(self):
        return f"<CryoloAutoPick parser at {self._basepath}>"

    def _load_job_directory(self, jobdir):
        return super()._load_job_directory(
            jobdir, input_tag="'input_file'", star_location="STAR"
        )

    def _get_highlighted_mircograph(
        self, jobdir: str, mc_mic_name: str, mic_parts: List[str]
    ) -> pathlib.Path:
        return (
            self._basepath / jobdir / "STAR" / pathlib.Path(mc_mic_name).name
        ).with_suffix(".jpeg")

    @property
    def jobs(self):
        return super(Cryolo, self).jobs

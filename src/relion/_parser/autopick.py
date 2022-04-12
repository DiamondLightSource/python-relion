from __future__ import annotations

import logging
import pathlib
from collections import namedtuple

from relion._parser.jobtype import JobType

logger = logging.getLogger("relion._parser.autopick")

ParticlePickerInfo = namedtuple(
    "ParticlePickerInfo",
    [
        "number_of_particles",
        "micrograph_full_path",
        "mc_image_full_path",
        "first_micrograph_name",
        "highlighted_micrograph",
        "coordinates",
        "job",
    ],
)

ParticleCacheRecord = namedtuple(
    "ParticleCacheRecord",
    [
        "data",
        "file_size",
    ],
)


class AutoPick(JobType):
    def __init__(self, path, particle_cache=None):
        super().__init__(path)
        self._particle_cache = particle_cache or {}

    def __eq__(self, other):
        if isinstance(other, AutoPick):  # check this
            return self._basepath == other._basepath
        return False

    def __hash__(self):
        return hash(("relion._parser.AutoPick", self._basepath))

    def __repr__(self):
        return f"AutoPick({repr(str(self._basepath))})"

    def __str__(self):
        return f"<AutoPick parser at {self._basepath}>"

    def _load_job_directory(self, jobdir):
        try:
            file = self._read_star_file(jobdir, "summary.star")
        except (RuntimeError, FileNotFoundError, OSError, ValueError):
            return []

        info_table = self._find_table_from_column_name("_rlnGroupNrParticles", file)
        if info_table is None:
            logger.debug(f"_rlnGroupNrParticles not found in file {file}")
            return []

        all_particles = self.parse_star_file("_rlnGroupNrParticles", file, info_table)
        # num_particles = sum([int(n) for n in all_particles])

        mc_micrographs = self.parse_star_file("_rlnMicrographName", file, info_table)

        first_mc_micrograph = mc_micrographs[0]

        particle_picker_info = []
        for mic, np in zip(mc_micrographs, all_particles):
            coords = self._get_particle_info(jobdir, pathlib.Path(mic))
            mic_parts = pathlib.Path(mic).parts
            highlighted_micrograph = (
                self._basepath
                / jobdir
                / pathlib.Path(mic)
                .relative_to(pathlib.Path(mic_parts[0]) / mic_parts[1])
                .with_suffix(".jpeg")
            )
            particle_picker_info.append(
                ParticlePickerInfo(
                    int(np),
                    mic,
                    str(self._basepath.parent / mic).replace(".mrc", ".jpeg"),
                    first_mc_micrograph,
                    str(highlighted_micrograph),
                    coords,
                    jobdir,
                )
            )

        return particle_picker_info

    def _get_particle_info(self, jobdir, micrograph):
        particle_data = []
        mic_parts = micrograph.parts
        mc_job_path = pathlib.Path(mic_parts[0]) / mic_parts[1]
        particle_star_file = pathlib.Path(
            str(micrograph.relative_to(mc_job_path).with_suffix(".star")).replace(
                micrograph.stem, micrograph.stem + "_autopick"
            )
        )
        if self._particle_cache.get(jobdir):
            if self._particle_cache[jobdir].get(micrograph):
                try:
                    if (
                        self._particle_cache[jobdir][micrograph].file_size
                        == (self._basepath / jobdir / particle_star_file).stat().st_size
                    ):
                        return self._particle_cache[jobdir][micrograph].data
                except FileNotFoundError:
                    logger.debug(
                        "Could not find expected file containing particle data",
                        exc_info=True,
                    )
                    return []
        else:
            self._particle_cache[jobdir] = {}
        try:
            particle_star = self._read_star_file(jobdir, particle_star_file)
        except (FileNotFoundError, RuntimeError, ValueError):
            return particle_data
        try:
            info_table = self._find_table_from_column_name(
                "_rlnCoordinateX", particle_star
            )
        except (FileNotFoundError, RuntimeError, ValueError):
            return particle_data
        if info_table is None:
            logger.debug(
                f"_rlnMicrographFrameNumber not found in file {particle_star_file}"
            )
            return particle_data
        xs = self.parse_star_file("_rlnCoordinateX", particle_star, info_table)
        ys = self.parse_star_file("_rlnCoordinateY", particle_star, info_table)
        for x, y in zip(xs, ys):
            particle_data.append((x, y))
        try:
            self._particle_cache[jobdir][micrograph] = ParticleCacheRecord(
                particle_data,
                (self._basepath / jobdir / particle_star_file).stat().st_size,
            )
        except FileNotFoundError:
            return []
        return particle_data

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

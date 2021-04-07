import collections.abc
import os
from gemmi import cif


class JobType(collections.abc.Mapping):
    def __eq__(self, other):
        if isinstance(other, JobType):  # check this
            return self._basepath == other._basepath
        return False

    def __hash__(self):
        return hash(("relion._parser.JobType", self._basepath))

    def __init__(self, path):
        self._basepath = path
        self._jobcache = {}

    def __iter__(self):
        return iter(self.jobs)

    def __len__(self):
        return len(self.jobs)

    def __repr__(self):
        return f"JobType({repr(str(self._basepath))})"

    def __str__(self):
        return f"<JobType parser at {self._basepath}>"

    @property
    def jobs(self):
        return sorted(
            d.name
            for d in self._basepath.iterdir()
            if d.is_dir() and not d.is_symlink()
        )

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

    def _load_job_directory(self, jobdir):
        raise NotImplementedError("Load job directory not implemented")

    def _read_star_file(self, job_num, file_name):
        full_path = self._basepath / job_num / file_name
        gemmi_readable_path = os.fspath(full_path)
        star_doc = cif.read_file(gemmi_readable_path)
        return star_doc

    def parse_star_file(self, loop_name, star_doc, block_number):
        data_block = star_doc[block_number]
        values = data_block.find_loop(loop_name)
        values_list = list(values)
        if not values_list:
            print("Warning - no values found for", loop_name)
        return values_list

    def _find_table_from_column_name(self, cname, star_doc):
        for block_index, block in enumerate(star_doc):
            if list(block.find_loop(cname)):
                return block_index
        return None

    @staticmethod
    def for_cache(element):
        return None

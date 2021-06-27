import functools
import re

import ispyb

# if we replace uuid with count do not include 0 in the count because it will break some bool checks for None


class ProcessID:
    def __init__(self, start):
        self.id = start

    def __call__(self):
        old_id = self.id
        self.id += 1
        return old_id

    def reset(self, start):
        self.id = start


PID = ProcessID(1)


class Table:
    def __init__(
        self,
        columns,
        primary_key,
        unique=None,
        counters=None,
        append=None,
        required=None,
    ):
        self.columns = columns
        self._tab = {}
        for c in self.columns:
            self._tab[c] = []
        self._primary_key = primary_key
        self._last_update = {self: 0}
        if unique is not None:
            if isinstance(unique, str):
                self._unique = [unique]
            else:
                self._unique = unique
        else:
            self._unique = unique
        if counters is not None:
            if isinstance(counters, str):
                self._counters = [counters]
            else:
                self._counters = counters
        else:
            self._counters = []
        if append is None:
            self._append = []
        elif isinstance(append, list):
            self._append = append
        else:
            self._append = [append]
        if required is None:
            self._required = []
        elif isinstance(required, list):
            self._required = required
        else:
            self._required = [required]

    def __getitem__(self, key):
        return self._tab[key]

    def add_row(self, row):

        modified = False

        unique_check = self._unique_check(row)

        prim_key_arg = unique_check or row.get(self._primary_key)
        if unique_check is None or prim_key_arg not in self._tab[self._primary_key]:
            try:
                for counter in self._counters:
                    row[counter] = len(self._tab[counter]) + 1
            except TypeError:
                pass
        else:
            for counter in self._counters:
                index = self._tab[self._primary_key].index(prim_key_arg)
                row[counter] = self._tab[counter][index]

        if prim_key_arg is None or prim_key_arg not in self._tab[self._primary_key]:
            modified = True
            for c in self.columns:
                if c == self._primary_key:
                    self._tab[c].append(row.get(c) or PID())
                else:
                    self._tab[c].append(row.get(c))
        else:
            index = self._tab[self._primary_key].index(prim_key_arg)
            for c in self.columns:
                if c != self._primary_key:
                    if self._tab[c][index] != row.get(c):
                        if c in self._append:
                            if isinstance(self._tab[c][index], list) and isinstance(
                                row.get(c), list
                            ):
                                for n in row.get(c):
                                    if n not in self._tab[c][index]:
                                        modified = True
                                        self._tab[c][index].append(n)
                            elif isinstance(self._tab[c][index], list):
                                if row.get(c) not in self._tab[c][index]:
                                    modified = True
                                    self._tab[c][index].append(row.get(c))
                            elif isinstance(row.get(c), list):
                                self._tab[c][index] = [self._tab[c][index]]
                                for n in row.get(c):
                                    if n not in self._tab[c][index]:
                                        modified = True
                                        self._tab[c][index].append(n)
                                if len(self._tab[c][index]) == 1:
                                    self._tab[c][index] = self._tab[c][index][0]
                            else:
                                modified = True
                                self._tab[c][index] = [
                                    self._tab[c][index],
                                    row.get(c),
                                ]
                        else:
                            modified = True
                            self._tab[c][index] = row.get(c)

        if modified:
            for req in self._required:
                if row.get(req) is None:
                    return
            if prim_key_arg is None:
                return row.get(self._primary_key) or self._tab[self._primary_key][-1]
            else:
                return prim_key_arg
        else:
            return

    def _unique_check(self, in_values):
        try:
            unique_indices = []
            for u in self._unique:
                if in_values.get(u) in self._tab[u]:
                    indices = [
                        i
                        for i, element in enumerate(self._tab[u])
                        if element == in_values.get(u)
                    ]
                    if isinstance(indices, list):
                        unique_indices.append(indices)
                    else:
                        unique_indices.append([indices])
                else:
                    break
            else:
                for i1, ui1 in enumerate(unique_indices):
                    for i2, ui2 in enumerate(unique_indices):
                        if i1 != i2:
                            if set(ui1).isdisjoint(ui2):
                                return
                else:
                    overlap_list = unique_indices[0]
                    for i in range(len(unique_indices) - 1):
                        curr_overlap = set(unique_indices[i]).intersection(
                            unique_indices[i + 1]
                        )
                        overlap_list = list(
                            set(overlap_list).intersection(curr_overlap)
                        )
                    if not overlap_list:
                        return
                    return self._tab[self._primary_key][overlap_list[0]]
            return
        except TypeError:
            return

    def get_row_index(self, key, value):
        if value is None:
            return
        indices = [i for i, element in enumerate(self._tab[key]) if element == value]
        if indices:
            if len(indices) == 1:
                return indices[0]
            else:
                return indices
        return

    def get_row_by_primary_key(self, value):
        row_index = self.get_row_index(self._primary_key, value)
        row = {}
        for c in self.columns:
            row[c] = self._tab[c][row_index]
        return row


def to_snake_case(camel_case):
    return re.sub(r"(?<!^)(?=[A-Z])", "_", camel_case).lower()


def get_prim_key(tab):
    for key in tab.__table__.columns.keys():
        if tab.__table__.columns[key].primary_key:
            return to_snake_case(key)


class MotionCorrectionTable(Table):
    def __init__(self):
        columns = [
            to_snake_case(c)
            for c in ispyb.sqlalchemy.MotionCorrection.__table__.columns.keys()
        ]
        columns.append("job_string")
        prim_key = get_prim_key(ispyb.sqlalchemy.MotionCorrection)
        super().__init__(
            columns,
            prim_key,
            unique="micrograph_full_path",
            counters="image_number",
        )


class CTFTable(Table):
    def __init__(self):
        columns = [
            to_snake_case(c) for c in ispyb.sqlalchemy.CTF.__table__.columns.keys()
        ]
        prim_key = get_prim_key(ispyb.sqlalchemy.CTF)
        super().__init__(
            columns,
            prim_key,
            unique="motion_correction_id",
            required="motion_correction_id",
        )


class ParticlePickerTable(Table):
    def __init__(self):
        columns = [
            to_snake_case(c)
            for c in ispyb.sqlalchemy.ParticlePicker.__table__.columns.keys()
        ]
        columns.append("micrograph_full_path")
        columns.append("first_motion_correction_micrograph")
        columns.append("job_string")
        prim_key = get_prim_key(ispyb.sqlalchemy.ParticlePicker)
        super().__init__(
            columns, prim_key, unique=["micrograph_full_path", "job_string"]
        )


class ParticleClassificationGroupTable(Table):
    def __init__(self):
        columns = [
            to_snake_case(c)
            for c in ispyb.sqlalchemy.ParticleClassificationGroup.__table__.columns.keys()
        ]
        columns.append("job_string")
        prim_key = get_prim_key(ispyb.sqlalchemy.ParticleClassificationGroup)
        super().__init__(columns, prim_key, unique="job_string")


class ParticleClassificationTable(Table):
    def __init__(self):
        columns = [
            to_snake_case(c)
            for c in ispyb.sqlalchemy.ParticleClassification.__table__.columns.keys()
        ]
        columns.append("job_string")
        prim_key = get_prim_key(ispyb.sqlalchemy.ParticleClassification)
        super().__init__(columns, prim_key, unique=["job_string", "class_number"])


class CryoemInitialModelTable(Table):
    def __init__(self):
        columns = [
            to_snake_case(c)
            for c in ispyb.sqlalchemy.CryoemInitialModel.__table__.columns.keys()
        ]
        columns.append("ini_model_job_string")
        prim_key = get_prim_key(ispyb.sqlalchemy.CryoemInitialModel)
        super().__init__(
            columns,
            prim_key,
            unique="ini_model_job_string",
            append="particle_classification_id",
        )


@functools.singledispatch
def insert(primary_table, end_time, source, relion_options, **kwargs):
    raise ValueError(f"{primary_table!r} is not a known Table")


@insert.register(MotionCorrectionTable)
def _(
    primary_table: MotionCorrectionTable,
    end_time,
    source,
    relion_options,
    row,
):

    row.update(
        {
            "dose_per_frame": relion_options.motioncor_doseperframe,
            "patches_used_x": relion_options.motioncor_patches_x,
            "patches_used_y": relion_options.motioncor_patches_y,
        }
    )
    pid = primary_table.add_row(row)
    return pid


@insert.register(CTFTable)
def _(
    primary_table: CTFTable,
    end_time,
    source,
    relion_options,
    row,
):

    row.update(
        {
            "box_size_x": relion_options.ctffind_boxsize,
            "box_size_y": relion_options.ctffind_boxsize,
            "min_resolution": relion_options.ctffind_minres,
            "max_resolution": relion_options.ctffind_maxres,
            "min_defocus": relion_options.ctffind_defocus_min,
            "max_defocus": relion_options.ctffind_defocus_max,
            "defocus_step_size": relion_options.ctffind_defocus_step,
        }
    )

    pid = primary_table.add_row(row)
    return pid


@insert.register(ParticlePickerTable)
def _(
    primary_table: ParticlePickerTable,
    end_time,
    source,
    relion_options,
    row,
):

    row.update(
        {
            "particle_picking_template": relion_options.cryolo_gmodel,
            "particle_diameter": int(
                relion_options.extract_boxsize
                * relion_options.angpix
                / relion_options.motioncor_binning
            )
            / 10,
        }
    )
    pid = primary_table.add_row(row)
    return pid


@insert.register(ParticleClassificationGroupTable)
def _(
    primary_table: ParticleClassificationGroupTable,
    end_time,
    source,
    relion_options,
    row,
):

    row.update(
        {
            "number_of_particles_per_batch": relion_options.batch_size,
            "number_of_classes_per_batch": relion_options.class2d_nr_classes,
            "symmetry": relion_options.symmetry,
        }
    )
    pid = primary_table.add_row(row)
    return pid


@insert.register(ParticleClassificationTable)
def _(
    primary_table: ParticleClassificationTable,
    end_time,
    source,
    relion_options,
    row,
):

    pid = primary_table.add_row(row)
    return pid


@insert.register(CryoemInitialModelTable)
def _(
    primary_table: CryoemInitialModelTable,
    end_time,
    source,
    relion_options,
    row,
):

    row["number_of_particles"] = row["init_model_number_of_particles"][
        row["init_model_class_num"]
    ]
    row.update({"resolution": relion_options.inimodel_resol_final})
    pid = primary_table.add_row(row)
    return pid


@functools.singledispatch
def construct_message(table, primary_key):
    raise ValueError(f"{table!r} is not a known Table")


@construct_message.register(MotionCorrectionTable)
def _(table: MotionCorrectionTable, primary_key: int):
    results = {
        "ispyb_command": "insert_motion_correction",
    }
    for k, v in table.get_row_by_primary_key(primary_key).items():
        results[k] = v
    return results


@construct_message.register(CTFTable)
def _(table: CTFTable, primary_key: int):
    results = {
        "ispyb_command": "insert_ctf",
    }
    for k, v in table.get_row_by_primary_key(primary_key).items():
        results[k] = v
    return results


@construct_message.register(ParticlePickerTable)
def _(table: ParticlePickerTable, primary_key: int):
    results = {
        "ispyb_command": "insert_particle_picker",
    }
    for k, v in table.get_row_by_primary_key(primary_key).items():
        results[k] = v
    return results


@construct_message.register(ParticleClassificationGroupTable)
def _(table: ParticleClassificationGroupTable, primary_key: int):
    results = {
        "ispyb_command": "insert_particle_classification_group",
    }
    for k, v in table.get_row_by_primary_key(primary_key).items():
        results[k] = v
    return results


@construct_message.register(ParticleClassificationTable)
def _(table: ParticleClassificationTable, primary_key: int):
    results = {
        "ispyb_command": "insert_particle_classification",
    }
    for k, v in table.get_row_by_primary_key(primary_key).items():
        results[k] = v
    return results


@construct_message.register(CryoemInitialModelTable)
def _(table: CryoemInitialModelTable, primary_key: int):
    results = {
        "ispyb_command": "insert_cryoem_initial_model",
    }
    for k, v in table.get_row_by_primary_key(primary_key).items():
        results[k] = v
    return results

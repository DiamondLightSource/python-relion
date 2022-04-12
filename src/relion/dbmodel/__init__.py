from __future__ import annotations

import collections.abc

from relion.dbmodel.dbgraph import DBGraph
from relion.dbmodel.dbnode import DBNode
from relion.dbmodel.modeltables import (
    CryoemInitialModelTable,
    CTFTable,
    MotionCorrectionTable,
    ParticleClassificationGroupTable,
    ParticleClassificationTable,
    ParticlePickerTable,
    RelativeIceThicknessTable,
)


class DBModel(collections.abc.Mapping):
    def __init__(self, db_name):
        self._db = self._make_db(db_name)

    def __eq__(self, other):
        if isinstance(other, DBModel):
            if super().__eq__(other) and self._db == other._db:
                return True
        return False

    def __getitem__(self, key):
        return self._db[key]

    def __len__(self):
        return len(self._db.keys())

    def __iter__(self):
        return self._db.items()

    def values(self):
        return self._db.values()

    def keys(self):
        return self._db.keys()

    @property
    def db_nodes(self):
        nodes = []
        for node in self.values():
            if node not in nodes:
                nodes.append(node)
        return nodes

    def _make_db(self, db_name):
        if db_name == "ISPyB":
            return self._make_ispyb_model()
        else:
            raise ValueError(f"{db_name} not implmented in relion.dbmodel.DBModel")

    def _make_ispyb_model(self):
        self.mc_db_node = DBNode("MCTable", [MotionCorrectionTable()])

        self.ctf_db_node = DBNode("CTFTable", [CTFTable()])
        self.mc_db_node.link_to(
            self.ctf_db_node,
            traffic={
                "check_for": "micrograph_full_path",
                "foreign_key": "motion_correction_id",
                "table_key": "motion_correction_id",
                "foreign_table": self.mc_db_node.tables[0],
            },
        )

        self.rel_ice_bd_node = DBNode(
            "RelativeIceThicknessTable", [RelativeIceThicknessTable()]
        )
        self.mc_db_node.link_to(
            self.rel_ice_bd_node,
            traffic={
                "check_for": "micrograph_full_path",
                "foreign_key": "motion_correction_id",
                "table_key": "motion_correction_id",
                "foreign_table": self.mc_db_node.tables[0],
            },
        )

        self.parpick_db_node = DBNode("ParticlePickerTable", [ParticlePickerTable()])
        self.mc_db_node.link_to(
            self.parpick_db_node,
            traffic={
                "check_for": "micrograph_full_path",
                "foreign_key": "motion_correction_id",
                "table_key": "first_motion_correction_id",
                "foreign_table": self.mc_db_node.tables[0],
            },
        )

        self.class_group_db_node = DBNode(
            "ClassificationGroupTable",
            [ParticleClassificationGroupTable()],
        )
        self.class_db_node = DBNode(
            "ClassificationTable",
            [ParticleClassificationTable()],
        )
        self.parpick_db_node.link_to(
            self.class_group_db_node,
            traffic={
                "check_for": "parpick_job_string",
                "check_for_foreign_name": "job_string",
                "foreign_key": "particle_picker_id",
                "table_key": "particle_picker_id",
                "first": True,
                "foreign_table": self.parpick_db_node.tables[0],
            },
        )
        self.class_group_db_node.link_to(
            self.class_db_node,
            traffic={
                "check_for": "job_string",
                "foreign_key": "particle_classification_group_id",
                "table_key": "particle_classification_group_id",
                "foreign_table": self.class_group_db_node.tables[0],
            },
        )
        self.class2d_db_node = DBGraph(
            "2DClassificationTables",
            [self.class_group_db_node, self.class_db_node],
            auto_connect=True,
        )

        self.ini_model_db_node = DBNode(
            "InitialModelTable",
            [CryoemInitialModelTable()],
        )
        self.class_db_node.link_to(
            self.ini_model_db_node,
            traffic={
                "check_for": "job_string",
                "foreign_key": "particle_classification_id",
                "table_key": "particle_classification_id",
                "foreign_table": self.class_db_node.tables[0],
                "first": False,
            },
        )
        self.class3d_db_node = DBGraph(
            "3DClassificationTables",
            [
                self.class_group_db_node,
                self.class_db_node,
                self.ini_model_db_node,
            ],
            auto_connect=True,
        )

        db_dict = {
            "MotionCorr": self.mc_db_node,
            "CtfFind": self.ctf_db_node,
            "AutoPick": self.parpick_db_node,
            "External/crYOLO_AutoPick/": self.parpick_db_node,
            "Class2D": self.class2d_db_node,
            "InitialModel": self.class3d_db_node,
            "Class3D": self.class3d_db_node,
            "External/Icebreaker_5fig/": self.rel_ice_bd_node,
        }

        return db_dict

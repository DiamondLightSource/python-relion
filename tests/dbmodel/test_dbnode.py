import pytest

import relion
from relion.dbmodel import modeltables
from relion.dbmodel.dbnode import DBNode


@pytest.fixture
def empty_options():
    return []


@pytest.fixture
def proj(dials_data):
    return relion.Project(
        dials_data("relion_tutorial_data"),
        run_options=empty_options,
    )


@pytest.fixture
def mc_table(proj):
    table = modeltables.MotionCorrectionTable()
    mc_res = proj.motioncorrection["job002"]
    mc_db_entries = proj.motioncorrection.db_unpack(mc_res)
    for entry in mc_db_entries:
        table.add_row(entry)
    return table


@pytest.fixture
def ctf_table(proj):
    table = modeltables.CTFTable()
    ctf_res = proj.ctffind["job003"]
    ctf_db_entries = proj.ctffind.db_unpack(ctf_res)
    for i, entry in enumerate(ctf_db_entries):
        table.add_row({**entry, "motion_correction_id": i + 1})
    return table


@pytest.fixture
def mc_db_node(mc_table):
    node = DBNode("MCTable", [modeltables.MotionCorrectionTable()])
    return node


@pytest.fixture
def ctf_db_node(ctf_table):
    node = DBNode("CTFTable", [modeltables.CTFTable()])
    return node


def test_correct_motion_correction_inserts_on_mc_table(mc_table):
    assert len(mc_table["motion_correction_id"]) == 24
    base_id = sorted(mc_table["motion_correction_id"])[0]
    first_row = mc_table.get_row_by_primary_key(base_id)
    assert (
        first_row["micrograph_full_path"]
        == "MotionCorr/job002/Movies/20170629_00021_frameImage.mrc"
    )
    second_row = mc_table.get_row_by_primary_key(base_id + 1)
    assert second_row["total_motion"] == "19.551677"


def test_correct_inserts_on_ctf_table(ctf_table):
    base_id = sorted(ctf_table["ctf_id"])[0]
    first_row = ctf_table.get_row_by_primary_key(base_id)
    assert len(ctf_table["ctf_id"]) == 24
    assert first_row["astigmatism"] == "288.135742"


def test_boolean_db_node(mc_db_node):
    assert mc_db_node

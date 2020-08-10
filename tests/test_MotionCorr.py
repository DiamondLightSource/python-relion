import pytest
from pathlib import Path
import relion.MotionCorrection as MC


@pytest.fixture
def input_test_folder(dials_data):
    return Path(dials_data("relion_tutorial_data"))


def test_total_value(input_test_folder):
    mc_object = MC.MotionCorrection(input_test_folder)
    mc_object.set_total_accum_motion()
    total_motion = mc_object.get_accum_motion_total()
    assert total_motion[0][1] == "16.420495"


def test_late_motion(input_test_folder):
    mc_object = MC.MotionCorrection(input_test_folder)
    mc_object.set_late_accum_motion()
    late_motion = mc_object.get_accum_motion_late()
    assert late_motion[0][1] == "13.914187"


def test_early_motion(input_test_folder):
    mc_object = MC.MotionCorrection(input_test_folder)
    mc_object.set_early_accum_motion()
    early_motion = mc_object.get_accum_motion_early()
    assert early_motion[0][1] == "2.506308"

import pytest
import relion


@pytest.fixture
def input(dials_data):
    return relion.Project(dials_data("relion_tutorial_data")).motioncorrection


def test_total_value(input):
    mc_object = input
    # mc_object.set_total_accum_motion()
    total_motion = mc_object.accum_motion_total
    assert total_motion[0] == "16.420495"


def test_late_motion(input):
    mc_object = input
    # mc_object.set_late_accum_motion()
    late_motion = mc_object.accum_motion_late
    assert late_motion[0] == "13.914187"


def test_early_motion(input):
    mc_object = input
    # mc_object.set_early_accum_motion()
    early_motion = mc_object.accum_motion_early
    assert early_motion[0] == "2.506308"


def test_invalid_loop_name(input):
    mc_object = input
    early_motion = mc_object.accum_motion_early
    assert early_motion is None


def test_dict(input):
    mc_object = input
    # mc_object.set_early_accum_motion()
    # mc_object.set_late_accum_motion()
    # mc_object.set_total_accum_motion()
    # mc_object.set_micrograph_name()
    early_motion = mc_object.accum_motion_early
    late_motion = mc_object.accum_motion_late
    total_motion = mc_object.accum_motion_total
    names = mc_object.micrograph_name
    mc_object.construct_dict(names, total_motion, early_motion, late_motion)

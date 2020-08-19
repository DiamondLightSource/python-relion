import pytest
import relion
from pprint import pprint


@pytest.fixture
def input(dials_data):
    return relion.Project(dials_data("relion_tutorial_data")).motioncorrection


@pytest.fixture
def invalid_input(dials_data):
    return relion.Project(dials_data("relion_tutorial_data"))


def test_total_value(input):
    mc_object = input
    assert mc_object.accum_motion_total[0][0] == "16.420495"


def test_late_motion(input):
    mc_object = input
    assert mc_object.accum_motion_late[0][0] == "13.914187"


def test_early_motion(input):
    mc_object = input
    assert mc_object.accum_motion_early[0][0] == "2.506308"


def test_invalid_input(invalid_input):
    mc_object = invalid_input
    try:
        early_motion = mc_object.accum_motion_early
    except AttributeError:
        early_motion = False
    assert early_motion is False


def test_all_keys_are_different(input):
    mc_object = input
    early_motion = mc_object.accum_motion_early
    late_motion = mc_object.accum_motion_late
    total_motion = mc_object.accum_motion_total
    names = mc_object.micrograph_name
    jobs = mc_object.job_number
    mc_dict = mc_object.construct_dict(
        jobs, names, total_motion, early_motion, late_motion
    )
    pprint(mc_dict)
    key_list = list(mc_dict.keys())
    for i in range(1, len(key_list) - 1):
        assert key_list[i] != key_list[i - 1]

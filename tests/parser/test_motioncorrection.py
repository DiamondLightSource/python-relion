import pytest
import relion
from pprint import pprint


@pytest.fixture
def input(dials_data):
    return relion.Project(dials_data("relion_tutorial_data")).motioncorrection


@pytest.fixture
def invalid_input(dials_data):
    return relion.Project(dials_data("relion_tutorial_data"))


def test_job_num(input):
    mc_object = input
    pprint(dict(mc_object))
    assert list(dict(mc_object).keys())[0] == "job002"


def test_all_keys_are_different(input):
    mc_object = input
    dictionary = dict(mc_object)
    key_list = list(dictionary.keys())
    for i in range(1, len(key_list) - 1):
        assert key_list[i] != key_list[i - 1]


def test_total_value(input):
    mc_object = input
    assert mc_object["job002"][0].total_motion == "16.420495"


def test_late_motion(input):
    mc_object = input
    assert mc_object["job002"][0].late_motion == "13.914187"


def test_early_motion(input):
    mc_object = input
    assert mc_object["job002"][0].early_motion == "2.506308"


def test_invalid_input(invalid_input):
    mc_object = invalid_input
    try:
        early_motion = mc_object["job002"][0].early_motio
    except TypeError:
        early_motion = False
    assert early_motion is False

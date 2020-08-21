import pytest
import relion
from pprint import pprint


@pytest.fixture
def input(dials_data):
    return relion.Project(dials_data("relion_tutorial_data")).class3D


def test_job_num(input):
    class3d_object = input
    pprint(dict(class3d_object))
    assert list(dict(class3d_object).keys())[0] == "job016"


def test_class_number(input):
    class3d_object = input
    assert class3d_object["job016"][3].particle_sum[1] == 4501


def test_class_distribution(input):
    class3d_object = input
    assert class3d_object["job016"][0].class_distribution == 0.055685


def test_output_is_serialisable(input):
    object = input
    assert object.class_number == eval(repr(object.class_number))


def test_all_keys_are_different(input):
    class3d_object = input
    dictionary = dict(class3d_object)
    key_list = list(dictionary.keys())
    for i in range(1, len(key_list) - 1):
        assert key_list[i] != key_list[i - 1]


def test_sum_all(input):
    class3d_object = input
    total = []
    for item in dict(class3d_object):
        for i in range(len(class3d_object[item])):
            total.append(class3d_object[item][i].particle_sum[1])
    assert sum(total) == 5367

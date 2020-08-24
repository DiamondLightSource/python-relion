import pytest
import relion
from pathlib import Path
from pprint import pprint
from operator import attrgetter


@pytest.fixture
def class2d(dials_data):
    return relion.Project(dials_data("relion_tutorial_data")).class2D


input = class2d


def test_list_all_jobs_in_class2d_directory(class2d):
    """
    When used in an iterator context the Class2D instance returns
    all known job and alias names.
    """
    assert sorted(class2d) == ["job008", "job013"]


def test_class2d_behaves_like_a_dictionary(class2d):
    """
    Class2D implements the Mapping abstract baseclass,
    in other words it behaves like a fancy dictionary.
    """
    # pprint(class2d)
    dc = dict(class2d)
    assert list(dc) == list(class2d.keys()) == list(class2d)
    assert list(dc.values()) == list(class2d.values())
    # pprint(list(dc))


def test_job_num(input):
    class2d_object = input
    # pprint(dict(class2d_object))
    assert list(dict(class2d_object).keys())[0] == "job008"


def test_class_distribution(input):
    class2d_object = input
    assert class2d_object["job008"][0].class_distribution == 0.016487


def test_output_is_serialisable(input):
    object = input
    assert object["job008"][0].class_distribution == eval(
        repr(object["job008"][0].class_distribution)
    )


def test_all_keys_are_different(input):
    class2d_object = input
    dictionary = dict(class2d_object)
    key_list = list(dictionary.keys())
    for i in range(1, len(key_list) - 1):
        assert key_list[i] != key_list[i - 1]


def test_top_twenty_list(input):
    class2d_object = input
    twenty_list = []
    for item in dict(class2d_object):
        temp_list = sorted(class2d_object[item], key=attrgetter("class_distribution"))[
            -20:
        ]
        temp_list.reverse()
        twenty_list.append(temp_list)
    pprint(twenty_list)
    assert len(twenty_list[0]) == 20
    assert len(twenty_list[1]) == 20
    assert (
        twenty_list[1][0].reference_image
        == "000016@Class2D/job013/run_it025_classes.mrcs"
    )


def test_sum_all(input):
    class2d_object = input

    sum_list = []
    for item in dict(class2d_object):
        for i in range(len(class2d_object[item])):
            sum_list.append(class2d_object[item][i].particle_sum[1])
    assert sum(sum_list) == 10640


@pytest.fixture
def bigger_data():
    return Path(
        "/dls/ebic/data/staff-scratch/colin/EMPIAR-10264/Refine3D"
    )  # /job005/run_it018_data.star')

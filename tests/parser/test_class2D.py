import pytest
import relion
from pathlib import Path
from pprint import pprint
from operator import attrgetter


@pytest.fixture
def class2d(dials_data):
    return relion.Project(dials_data("relion_tutorial_data")).class2D


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
    dc = dict(class2d)
    assert list(dc) == list(class2d.keys()) == list(class2d)
    assert list(dc.values()) == list(class2d.values())


def test_jobs_are_in_correct_order_and_unique(class2d):
    assert list(class2d) == ["job008", "job013"]
    assert len(set(class2d)) == len(class2d)


def test_class_distribution(class2d):
    assert class2d["job008"][0].class_distribution == 0.016487


def test_output_is_serialisable(class2d):
    assert class2d["job008"][0].class_distribution == eval(
        repr(class2d["job008"][0].class_distribution)
    )


def test_top_twenty_list(class2d):
    twenty_list = []
    for item in dict(class2d):
        temp_list = sorted(class2d[item], key=attrgetter("class_distribution"))[-20:]
        temp_list.reverse()
        twenty_list.append(temp_list)
    pprint(twenty_list)
    assert len(twenty_list[0]) == 20
    assert len(twenty_list[1]) == 20
    assert (
        twenty_list[1][0].reference_image
        == "000016@Class2D/job013/run_it025_classes.mrcs"
    )


def test_sum_all(class2d):
    sum_list = []
    for item in dict(class2d):
        for i in range(len(class2d[item])):
            sum_list.append(class2d[item][i].particle_sum[1])
    assert sum(sum_list) == 10640


@pytest.fixture
def bigger_data():
    return Path(
        "/dls/ebic/data/staff-scratch/colin/EMPIAR-10264/Refine3D"
    )  # /job005/run_it018_data.star')

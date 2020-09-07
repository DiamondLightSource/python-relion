import pytest
import relion
from pprint import pprint


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
    dictionary = dict(class2d)
    final_dictionary = class2d.top_twenty(dictionary)
    pprint(final_dictionary)
    assert len(final_dictionary["job008"]) == 20
    assert len(final_dictionary["job013"]) == 20
    assert (
        final_dictionary["job013"][0].reference_image
        == "000016@Class2D/job013/run_it025_classes.mrcs"
    )


def test_sum_top_twenty(class2d):
    dictionary = dict(class2d)
    final_dictionary = class2d.top_twenty(dictionary)
    job_sum_list = []
    for item in final_dictionary:
        sum_list = []
        for i in range(len(final_dictionary[item])):
            sum_list.append(final_dictionary[item][i].particle_sum[1])
        job_sum_list.append(sum(sum_list))
    assert job_sum_list[0] == 900
    assert job_sum_list[1] == 7699


def test_sum_all(class2d):
    job_sum_list = []
    for item in dict(class2d):
        sum_list = []
        for i in range(len(class2d[item])):
            sum_list.append(class2d[item][i].particle_sum[1])
        job_sum_list.append(sum(sum_list))
    assert sum(job_sum_list) == 10640
    assert job_sum_list[0] == 1158
    assert job_sum_list[1] == 9482

import sys
from pprint import pprint

import pytest

import relion


@pytest.fixture
def empty_options():
    return []


@pytest.fixture
def proj(dials_data):
    return relion.Project(
        dials_data("relion_tutorial_data", pathlib=True), run_options=empty_options
    )


@pytest.fixture
def class2d(proj):
    return proj.class2D


def test_result_of_casting_to_string(class2d, proj):
    class2d_path = proj.basepath / "Class2D"
    assert str(class2d) == f"<Class2D parser at {class2d_path}>"


def test_class2D_representation(class2d, proj):
    class2d_path = proj.basepath / "Class2D"
    assert repr(class2d) == f"Class2D({repr(str(class2d_path))})"


def test_list_all_jobs_in_class2d_directory(class2d):
    """
    When used in an iterator context the Class2D instance returns
    all known job (dropping alias names).
    """
    assert sorted(class2d) == ["job008", "job013"]


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_aliases_are_dropped_on_iterating_so_jobs_arent_double_counted(proj):
    """
    Test that aliases are dropped so that jobs aren't double
    counted when iterated over
    """
    symlink = proj.basepath / "Class2D" / "LoG"
    symlink.symlink_to(proj.basepath / "Class2D" / "job008")
    sym_class2d = proj.class2D
    assert sorted(sym_class2d) == ["job008", "job013"]
    symlink.unlink()


def test_class2d_behaves_like_a_dictionary(class2d):
    """
    Class2D implements the Mapping abstract baseclass,
    in other words it behaves like a fancy dictionary.
    """
    dc = dict(class2d)
    assert list(dc) == list(class2d.keys()) == list(class2d)
    assert list(dc.values()) == list(class2d.values())


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_len_drops_symlinks_from_the_job_count_to_avoid_double_counting(proj):
    """
    Test that __len__ has the correct behaviour when symlinks
    are present
    """
    symlink = proj.basepath / "Class2D" / "LoG"
    symlink.symlink_to(proj.basepath / "Class2D" / "job008")
    sym_class2d = proj.class2D
    assert len(sym_class2d) == 2
    symlink.unlink()


def test_len_returns_correct_number_of_jobs(class2d):
    """
    Test that __len__ has the correct behaviour
    """
    assert len(class2d) == 2


def test_jobs_are_in_correct_order_and_unique(class2d):
    assert list(class2d) == ["job008", "job013"]
    assert len(set(class2d)) == len(class2d)


def test_class_distribution(class2d):
    assert class2d["job008"][0].class_distribution == 0.016487


def test_output_is_serialisable(class2d):
    assert class2d["job008"][0].class_distribution == eval(
        repr(class2d["job008"][0].class_distribution)
    )


def test_top_twenty_list(class2d, dials_data):
    dictionary = dict(class2d)
    final_dictionary = class2d.top_twenty(dictionary)
    pprint(final_dictionary)
    assert len(final_dictionary["job008"]) == 20
    assert len(final_dictionary["job013"]) == 20
    assert final_dictionary["job013"][0].reference_image == str(
        dials_data("relion_tutorial_data", pathlib=True)
        / "Class2D/job013/run_it025_classes.mrcs"
    )
    assert final_dictionary["job013"][0].particle_sum[0] == 16


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

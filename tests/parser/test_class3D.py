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
def class3d_object(proj):
    return proj.class3D


def test_result_of_casting_to_string(class3d_object, proj):
    class3d_path = proj.basepath / "Class3D"
    assert str(class3d_object) == f"<Class3D parser at {class3d_path}>"


def test_class3D_representation(class3d_object, proj):
    class3d_path = proj.basepath / "Class3D"
    assert repr(class3d_object) == f"Class3D({repr(str(class3d_path))})"


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_aliases_are_dropped_on_iterating_so_jobs_arent_double_counted(proj):
    """
    Test that aliases are dropped so that jobs aren't double
    counted when iterated over
    """
    symlink = proj.basepath / "Class3D" / "first_exhaustive"
    symlink.symlink_to(proj.basepath / "Class3D" / "job016")
    sym_class3d = proj.class3D
    assert sorted(sym_class3d) == ["job016"]
    symlink.unlink()


def test_len_returns_correct_number_of_jobs(class3d_object):
    """
    Test that __len__ has the correct behaviour
    """
    assert len(class3d_object) == 1


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_len_drops_symlinks_from_the_job_count_to_avoid_double_counting(proj):
    """
    Test that __len__ has the correct behaviour when symlinks
    are present
    """
    symlink = proj.basepath / "Class3D" / "first_exhaustive"
    symlink.symlink_to(proj.basepath / "Class3D" / "job016")
    sym_class3d = proj.class3D
    assert len(sym_class3d) == 1
    symlink.unlink()


def test_job_num(class3d_object):
    pprint(dict(class3d_object))
    assert list(dict(class3d_object).keys())[0] == "job016"


def test_class_number(class3d_object):
    assert class3d_object["job016"][3].particle_sum[1] == 4501


def test_class_distribution(class3d_object):
    assert class3d_object["job016"][0].class_distribution == 0.055685


def test_output_is_serialisable(class3d_object):
    assert class3d_object["job016"][0].class_distribution == eval(
        repr(class3d_object["job016"][0].class_distribution)
    )
    # assert class3d_object.class_number == eval(repr(class3d_object.class_number))


def test_all_keys_are_different(class3d_object):
    dictionary = dict(class3d_object)
    key_list = list(dictionary.keys())
    for i in range(1, len(key_list) - 1):
        assert key_list[i] != key_list[i - 1]


def test_sum_all(class3d_object):
    assert (
        sum(
            class3d.particle_sum[1]
            for job_output in class3d_object.values()
            for class3d in job_output
        )
        == 5367
    )

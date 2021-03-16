import pytest
import relion
import pathlib
import sys
from pprint import pprint


@pytest.fixture
def proj(dials_data):
    return relion.Project(dials_data("relion_tutorial_data"))


@pytest.fixture
def class3d_object(proj):
    return proj.class3D


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_list_all_jobs_in_class3d_directory_symlink(proj):
    """
    Test that aliases are dropped so that jobs aren't double
    counted when iterated over
    """
    symlink = pathlib.Path(proj.basepath / "Class3D/first_exhaustive")
    symlink.symlink_to(proj.basepath / "Class3D/job016/")
    sym_class3d = proj.class3D
    assert sorted(sym_class3d) == ["job016"]
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

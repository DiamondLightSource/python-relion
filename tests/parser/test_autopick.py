import sys
from pprint import pprint

import pytest

import relion


@pytest.fixture
def empty_options():
    return []


@pytest.fixture
def proj(dials_data):
    return relion.Project(dials_data("relion_tutorial_data"), run_options=empty_options)


@pytest.fixture
def input(proj):
    return proj.autopick


@pytest.fixture
def invalid_input(dials_data):
    return relion.Project(dials_data("relion_tutorial_data"), run_options=empty_options)


def test_result_of_casting_to_string(input, proj):
    autopick_path = proj.basepath / "AutoPick"
    assert str(input) == f"<AutoPick parser at {autopick_path}>"


def test_autopick_representation(input, proj):
    autopick_path = proj.basepath / "AutoPick"
    assert repr(input) == f"AutoPick({repr(str(autopick_path))})"


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_aliases_are_dropped_on_iterating_so_jobs_arent_double_counted(proj):
    """
    Test that aliases are dropped so that jobs aren't double
    counted when iterated over
    """
    symlink = proj.basepath / "AutoPick" / "autopick2"
    symlink.symlink_to(proj.basepath / "AutoPick" / "job006")
    sym_autopick = proj.autopick
    assert sorted(sym_autopick) == ["job006", "job010", "job011"]
    symlink.unlink()


def test_len_returns_correct_number_of_jobs(input):
    """
    Test that __len__ has the correct behaviour
    """
    assert len(input) == 3


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_len_drops_symlinks_from_the_job_count_to_avoid_double_counting(proj):
    """
    Test that __len__ has the correct behaviour when symlinks
    are present
    """
    symlink = proj.basepath / "AutoPick" / "autopick2"
    symlink.symlink_to(proj.basepath / "AutoPick" / "job006")
    sym_autopick = proj.autopick
    assert len(sym_autopick) == 3
    symlink.unlink()


def test_job_num(input):
    ap_object = input
    pprint(dict(ap_object))
    assert list(dict(ap_object).keys())[0] == "job006"


def test_all_keys_are_different(input):
    ap_object = input
    dictionary = dict(ap_object)
    key_list = list(dictionary.keys())
    for i in range(1, len(key_list) - 1):
        assert key_list[i] != key_list[i - 1]


def test_number_of_particles_value(input):
    ap_object = input
    assert ap_object["job006"][0].number_of_particles == 242
    assert ap_object["job006"][-1].number_of_particles == 237
    assert ap_object["job010"][0].number_of_particles == 422
    assert ap_object["job011"][0].number_of_particles == 422


def test_micrograph_path_name(input):
    ap_object = input
    assert (
        ap_object["job006"][0].first_micrograph_name
        == "MotionCorr/job002/Movies/20170629_00021_frameImage.mrc"
    )


def test_invalid_input(invalid_input):
    ap_object = invalid_input
    try:
        num_particles = ap_object["job006"][0].num_particles
    except TypeError:
        num_particles = False
    assert num_particles is False

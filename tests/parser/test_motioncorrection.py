import pytest
import relion
import sys
from pprint import pprint


@pytest.fixture
def proj(dials_data):
    return relion.Project(dials_data("relion_tutorial_data"))


@pytest.fixture
def input(proj):
    return proj.motioncorrection


@pytest.fixture
def invalid_input(dials_data):
    return relion.Project(dials_data("relion_tutorial_data"))


def test_result_of_casting_to_string(input, proj):
    motioncorr_path = proj.basepath / "MotionCorr"
    assert str(input) == f"<MotionCorr parser at {motioncorr_path}>"


def test_motioncorr_representation(input, proj):
    motioncorr_path = proj.basepath / "MotionCorr"
    assert repr(input) == f"MotionCorr({repr(str(motioncorr_path))})"


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_aliases_are_dropped_on_iterating_so_jobs_arent_double_counted(proj):
    """
    Test that aliases are dropped so that jobs aren't double
    counted when iterated over
    """
    symlink = proj.basepath / "MotionCorr" / "relioncor2"
    symlink.symlink_to(proj.basepath / "MotionCorr" / "job002")
    sym_motioncorr = proj.motioncorrection
    assert sorted(sym_motioncorr) == ["job002"]
    symlink.unlink()


def test_len_returns_correct_number_of_jobs(input):
    """
    Test that __len__ has the correct behaviour
    """
    assert len(input) == 1


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_len_drops_symlinks_from_the_job_count_to_avoid_double_counting(proj):
    """
    Test that __len__ has the correct behaviour when symlinks
    are present
    """
    symlink = proj.basepath / "MotionCorr" / "relioncor2"
    symlink.symlink_to(proj.basepath / "MotionCorr" / "job002")
    sym_motioncorr = proj.motioncorrection
    assert len(sym_motioncorr) == 1
    symlink.unlink()


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

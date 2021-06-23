import pytest
import sys
import relion
import pathlib


@pytest.fixture
def proj(dials_data):
    return relion.Project(dials_data("relion_tutorial_data"))


@pytest.fixture
def ctffind(proj):
    return proj.ctffind


def test_result_of_casting_to_string(ctffind, proj):
    ctffind_path = proj.basepath / "CtfFind"
    assert str(ctffind) == f"<CTFFind parser at {ctffind_path}>"


def test_ctffind_representation(ctffind, proj):
    ctffind_path = proj.basepath / "CtfFind"
    assert repr(ctffind) == f"CTFFind({repr(str(ctffind_path))})"


def test_list_ctffind_jobs(ctffind):
    assert ctffind
    assert len(ctffind) == 1
    assert list(ctffind.jobs) == ["job003"]
    assert ctffind["job003"]


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_aliases_are_dropped_on_iterating_so_jobs_arent_double_counted(proj):
    """
    Test that aliases are dropped so that jobs aren't double
    counted when iterated over
    """
    symlink = proj.basepath / "CtfFind" / "ctffind4"
    symlink.symlink_to(proj.basepath / "Class2D" / "job003")
    sym_ctffind = proj.ctffind
    assert sorted(sym_ctffind) == ["job003"]
    symlink.unlink()


def test_len_returns_correct_number_of_jobs(ctffind):
    """
    Test that __len__ has the correct behaviour
    """
    assert len(ctffind) == 1


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_len_drops_symlinks_from_the_job_count_to_avoid_double_counting(proj):
    """
    Test that __len__ has the correct behaviour when symlinks
    are present
    """
    symlink = proj.basepath / "CtfFind" / "ctffind4"
    symlink.symlink_to(proj.basepath / "CtfFind" / "job003")
    sym_ctffind = proj.ctffind
    assert len(sym_ctffind) == 1
    symlink.unlink()


def test_all_keys_are_different(ctffind):
    dictionary = dict(ctffind)
    key_list = list(dictionary.keys())
    for i in range(1, len(key_list) - 1):
        assert key_list[i] != key_list[i - 1]


def test_invalid_job_references_raise_KeyErrors(ctffind):
    for reference in ("job004", None, 1):
        with pytest.raises(KeyError):
            print("Checking", reference)
            ctffind[reference]


def test_astigmatism(ctffind):
    assert ctffind["job003"][0].astigmatism == "288.135742"


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_diagnosis_plot_path(proj, ctffind):
    assert ctffind["job003"][0].diagnostic_plot_path == str(
        pathlib.PurePosixPath(proj.basepath)
        / "CtfFind"
        / "job003"
        / "Movies"
        / "20170629_00021_frameImage_PS.jpeg"
    )

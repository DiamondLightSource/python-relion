import pytest
import pathlib
import sys
import relion


@pytest.fixture
def proj(dials_data):
    return relion.Project(dials_data("relion_tutorial_data"))


@pytest.fixture
def ctffind(proj):
    return proj.ctffind


def test_list_ctffind_jobs(ctffind):
    assert ctffind
    assert len(ctffind) == 1
    assert list(ctffind.jobs) == ["job003"]
    assert ctffind["job003"]


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_list_all_jobs_in_ctffind_directory_symlink(proj):
    """
    Test that aliases are dropped so that jobs aren't double
    counted when iterated over
    """
    symlink = pathlib.Path(proj.basepath / "CtfFind/ctffind4")
    symlink.symlink_to(proj.basepath / "Class2D/job003/")
    sym_ctffind = proj.ctffind
    assert sorted(sym_ctffind) == ["job003"]
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

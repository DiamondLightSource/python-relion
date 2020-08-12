import pytest
import relion


@pytest.fixture
def ctfFind(dials_data):
    print("check1", relion.Project(dials_data("relion_tutorial_data")).ctffind)
    return relion.Project(dials_data("relion_tutorial_data")).ctffind


def test_list_ctffind_jobs(ctfFind):
    print("check")
    assert ctfFind
    print("check2")
    assert list(ctfFind.jobs) == ["job003"]
    assert ctfFind["job003"]


def test_invalid_job_references_raise_KeyErrors(ctfFind):
    for reference in ("job004", None, 1):
        with pytest.raises(KeyError):
            print("Checking", reference)
            ctfFind[reference]


def test_astigmatism(ctfFind):
    print("ctf", ctfFind)
    ctf_object = ctfFind
    astigmatism = ctf_object.astigmatism
    assert astigmatism[0] == "288.135742"

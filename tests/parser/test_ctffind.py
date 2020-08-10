import pytest
import relion


@pytest.fixture
def ctffind(dials_data):
    return relion.Project(dials_data("relion_tutorial_data")).ctffind


def test_list_ctffind_jobs(ctffind):
    assert ctffind
    assert list(ctffind.jobs) == ["job003"]
    assert ctffind["job003"]


def test_invalid_job_references_raise_KeyErrors(ctffind):
    for reference in ("job004", None, 1):
        with pytest.raises(KeyError):
            print("Checking", reference)
            ctffind[reference]

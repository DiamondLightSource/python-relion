import pytest
import relion


@pytest.fixture
def ctffind(dials_data):
    return relion.Project(dials_data("relion_tutorial_data")).ctffind


def test_list_ctffind_jobs(ctffind):
    assert ctffind
    assert list(ctffind.jobs) == ["job003"]
    assert ctffind["job003"]


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

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


def test_astigmatism(ctffind):
    print("ctf", ctffind)
    ctf_object = ctffind
    astigmatism = ctf_object.astigmatism
    assert astigmatism[0] == "288.135742"


def test_all_keys_are_different(ctffind):
    ctf_object = ctffind
    astigmatism = ctf_object.astigmatism
    defocus_u = ctf_object.defocus_u
    defocus_v = ctf_object.defocus_v
    defocus_angle = ctf_object.defocus_angle
    max_res = ctf_object.max_resolution
    fig_of_merit = ctf_object.fig_of_merit
    names = ctf_object.micrograph_name
    ctf_dict = ctf_object.construct_dict(
        names, astigmatism, defocus_u, defocus_v, defocus_angle, max_res, fig_of_merit
    )
    key_list = list(ctf_dict.keys())
    for i in range(1, len(key_list) - 1):
        assert key_list[i] != key_list[i - 1]

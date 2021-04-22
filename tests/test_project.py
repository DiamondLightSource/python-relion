import pytest
import relion
import pathlib


@pytest.fixture
def proj(dials_data):
    return relion.Project(dials_data("relion_tutorial_data"))


def test_basic_Project_object_behaviour(tmp_path):
    rp1 = relion.Project(tmp_path)
    assert rp1
    assert str(tmp_path) in str(rp1)
    assert tmp_path.name in repr(rp1)

    rp2 = relion.Project(str(tmp_path))
    assert rp2
    assert str(rp1) == str(rp2)
    assert repr(rp1) == repr(rp2)

    # check objects with equal paths are equal
    assert rp1 == rp2

    # ensure objects are hashable and equivalent
    assert len({rp1, rp2}) == 1


def test_create_Project_on_inaccessible_path_fails(tmp_path):
    with pytest.raises(ValueError):
        relion.Project(tmp_path / "does_not_exist")


def test_Project_schedule_files_property_contains_the_correct_files(dials_data, proj):
    assert (
        pathlib.Path(dials_data("relion_tutorial_data")) / "pipeline_PREPROCESS.log"
        in proj.schedule_files
    )
    assert (
        pathlib.Path(dials_data("relion_tutorial_data")) / "pipeline_CLASS2D.log"
        in proj.schedule_files
    )
    assert (
        pathlib.Path(dials_data("relion_tutorial_data")) / "pipeline_INIMODEL.log"
        in proj.schedule_files
    )
    assert (
        pathlib.Path(dials_data("relion_tutorial_data")) / "pipeline_CLASS3D.log"
        in proj.schedule_files
    )


def test_results_collection_does_not_crash_for_an_empty_project():
    proj = relion.Project("./")
    results = proj.results
    assert results._results == []

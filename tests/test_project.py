import pathlib

import pytest
from gemmi import cif

import relion


@pytest.fixture
def empty_options():
    return []


@pytest.fixture
def proj(dials_data):
    return relion.Project(dials_data("relion_tutorial_data"), run_options=empty_options)


def remove_corrected_star_slice(corrected_star_path, required_slice):
    star_doc = cif.read_file(corrected_star_path)
    new_star_doc = cif.Document()
    new_star_doc.add_new_block("optics")
    optics_loops = [
        "_rlnOpticsGroupName",
        "_rlnOpticsGroup",
        "_rlnMtfFileName",
        "_rlnMicrographOriginalPixelSize",
        "_rlnVoltage",
        "_rlnSphericalAberration",
        "_rlnAmplitudeContrast",
        "_rlnMicrographPixelSize",
    ]
    new_loop = new_star_doc[0].init_loop("", optics_loops)
    old_loop = []
    for loop in optics_loops:
        old_loop.extend(list(star_doc[0].find_loop(loop)))
    new_loop.add_row(old_loop)
    new_star_doc.add_new_block("micrographs")
    mic_loops = [
        "_rlnCtfPowerSpectrum",
        "_rlnMicrographName",
        "_rlnMicrographMetadata",
        "_rlnOpticsGroup",
        "_rlnAccumMotionTotal",
        "_rlnAccumMotionEarly",
        "_rlnAccumMotionLate",
    ]
    new_loop = new_star_doc[1].init_loop("", mic_loops)
    old_micrographs = [[] for _ in range(len(star_doc[1].find_loop(mic_loops[0])))]
    for loop in mic_loops:
        for index, elem in enumerate(star_doc[1].find_loop(loop)):
            old_micrographs[index].append(elem)
    for mic in old_micrographs[required_slice]:
        new_loop.add_row(mic)
    return new_star_doc


def test_basic_Project_object_behaviour(tmp_path):
    rp1 = relion.Project(tmp_path, run_options=empty_options)
    assert rp1
    assert str(tmp_path) in str(rp1)
    assert tmp_path.name in repr(rp1)

    rp2 = relion.Project(str(tmp_path), run_options=empty_options)
    assert rp2
    assert str(rp1) == str(rp2)
    assert repr(rp1) == repr(rp2)

    # check objects with equal paths are equal
    assert rp1 == rp2

    # ensure objects are hashable and equivalent
    assert len({rp1, rp2}) == 1


def test_create_Project_on_inaccessible_path_fails(tmp_path):
    with pytest.raises(ValueError):
        relion.Project(tmp_path / "does_not_exist", run_options=empty_options)


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


def test_get_imported_files_from_job_directory(proj):
    imported = proj.get_imported()
    assert len(imported) == 24
    assert imported[0] == "Movies/20170629_00021_frameImage.tiff"

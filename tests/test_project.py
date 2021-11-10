import os
import sys

import pytest
from gemmi import cif

import relion

try:
    from relion.cryolo_relion_it.cryolo_relion_it import RelionItOptions
    from relion.zocalo.wrapper import construct_message
except ImportError:
    pass


@pytest.fixture
def empty_options():
    class Options:
        motioncor_doseperframe: int = 1
        motioncor_patches_x: int = 5
        motioncor_patches_y: int = 5
        ctffind_boxsize: int = 512
        ctffind_minres: int = 5
        ctffind_maxres: int = 30
        ctffind_defocus_min: int = 5000
        ctffind_defocus_max: int = 50000
        ctffind_defocus_step: int = 500
        cryolo_gmodel: str = ""
        extract_boxsize: int = 256
        angpix: float = 0.885
        motioncor_binning: int = 1
        batch_size: int = 20000
        class2d_nr_classes: int = 50
        class3d_nr_classes: int = 4
        symmetry: str = "C1"
        inimodel_resol_final: int = 15

    return Options


@pytest.fixture
def proj(dials_data, empty_options):
    return relion.Project(
        dials_data("relion_tutorial_data", pathlib=True), run_options=empty_options
    )


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


def test_create_Project_with_cluster_information_collection_does_not_fail(dials_data):
    relion.Project(
        dials_data("relion_tutorial_data", pathlib=True),
        run_options=empty_options,
        cluster=True,
    )


def test_Project_schedule_files_property_contains_the_correct_files(dials_data, proj):
    assert (
        dials_data("relion_tutorial_data", pathlib=True) / "pipeline_PREPROCESS.log"
        in proj.schedule_files
    )
    assert (
        dials_data("relion_tutorial_data", pathlib=True) / "pipeline_CLASS2D.log"
        in proj.schedule_files
    )
    assert (
        dials_data("relion_tutorial_data", pathlib=True) / "pipeline_INIMODEL.log"
        in proj.schedule_files
    )
    assert (
        dials_data("relion_tutorial_data", pathlib=True) / "pipeline_CLASS3D.log"
        in proj.schedule_files
    )


def test_get_imported_files_from_job_directory(proj):
    imported = proj.get_imported()
    assert len(imported) == 24
    assert imported[0] == "Movies/20170629_00021_frameImage.tiff"


def test_mulitple_loads_do_not_grow_the_in_list_of_data_pipeline_nodes(proj):
    mctabnode = proj._data_pipeline._node_list[2]
    assert len(mctabnode._in) == 1
    assert len(mctabnode._out) == 3
    proj.load()
    assert len(mctabnode._in) == 1
    assert len(mctabnode._out) == 3


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_prepended_results_are_picked_up_correctly(dials_data, proj):
    corrected_star_path = os.fspath(
        dials_data("relion_tutorial_data", pathlib=True)
        / "MotionCorr"
        / "job002"
        / "corrected_micrographs.star"
    )
    star_doc = cif.read_file(corrected_star_path)
    new_star_doc = remove_corrected_star_slice(corrected_star_path, slice(2, None))
    new_star_doc.write_file(corrected_star_path)
    proj._data_pipeline()

    assert (
        len(proj._db_model["MotionCorr"].tables[0]._tab["motion_correction_id"]) == 22
    )
    assert sorted(proj._db_model["MotionCorr"].tables[0]._tab["image_number"]) == list(
        range(1, 23)
    )

    star_doc.write_file(corrected_star_path)
    (
        dials_data("relion_tutorial_data", pathlib=True)
        / "MotionCorr"
        / "job002"
        / "RELION_JOB_EXIT_SUCCESS"
    ).touch()
    proj.load()
    proj._data_pipeline()
    assert (
        len(proj._db_model["MotionCorr"].tables[0]._tab["motion_correction_id"]) == 24
    )
    assert sorted(proj._db_model["MotionCorr"].tables[0]._tab["image_number"]) == list(
        range(1, 25)
    )
    base_id = sorted(
        proj._db_model["MotionCorr"].tables[0]._tab["motion_correction_id"]
    )[0]
    last_mc_id = sorted(
        proj._db_model["MotionCorr"].tables[0]._tab["motion_correction_id"]
    )[-1]
    first_row = proj._db_model["MotionCorr"].tables[0].get_row_by_primary_key(base_id)
    assert (
        first_row["micrograph_full_path"]
        == "MotionCorr/job002/Movies/20170629_00023_frameImage.mrc"
    )
    last_row = proj._db_model["MotionCorr"].tables[0].get_row_by_primary_key(last_mc_id)
    assert (
        last_row["micrograph_full_path"]
        == "MotionCorr/job002/Movies/20170629_00022_frameImage.mrc"
    )


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_appended_results_are_picked_up_correctly(dials_data, proj):
    corrected_star_path = os.fspath(
        dials_data("relion_tutorial_data", pathlib=True)
        / "MotionCorr"
        / "job002"
        / "corrected_micrographs.star"
    )
    star_doc = cif.read_file(corrected_star_path)
    new_star_doc = remove_corrected_star_slice(corrected_star_path, slice(0, -2))
    new_star_doc.write_file(corrected_star_path)
    proj._data_pipeline()

    assert (
        len(proj._db_model["MotionCorr"].tables[0]._tab["motion_correction_id"]) == 22
    )
    assert sorted(proj._db_model["MotionCorr"].tables[0]._tab["image_number"]) == list(
        range(1, 23)
    )

    star_doc.write_file(corrected_star_path)
    (
        dials_data("relion_tutorial_data", pathlib=True)
        / "MotionCorr"
        / "job002"
        / "RELION_JOB_EXIT_SUCCESS"
    ).touch()
    proj.load()
    proj._data_pipeline()
    assert (
        len(proj._db_model["MotionCorr"].tables[0]._tab["motion_correction_id"]) == 24
    )
    assert sorted(proj._db_model["MotionCorr"].tables[0]._tab["image_number"]) == list(
        range(1, 25)
    )
    base_id = sorted(
        proj._db_model["MotionCorr"].tables[0]._tab["motion_correction_id"]
    )[0]
    last_mc_id = sorted(
        proj._db_model["MotionCorr"].tables[0]._tab["motion_correction_id"]
    )[-1]
    first_row = proj._db_model["MotionCorr"].tables[0].get_row_by_primary_key(base_id)
    assert (
        first_row["micrograph_full_path"]
        == "MotionCorr/job002/Movies/20170629_00021_frameImage.mrc"
    )
    last_row = proj._db_model["MotionCorr"].tables[0].get_row_by_primary_key(last_mc_id)
    assert (
        last_row["micrograph_full_path"]
        == "MotionCorr/job002/Movies/20170629_00049_frameImage.mrc"
    )


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_prepended_results_are_picked_up_correctly_in_project_messages(dials_data):
    project = relion.Project(
        dials_data("relion_tutorial_data", pathlib=True),
        run_options=RelionItOptions,
        message_constructors={"ispyb": construct_message},
    )
    corrected_star_path = os.fspath(
        dials_data("relion_tutorial_data", pathlib=True)
        / "MotionCorr"
        / "job002"
        / "corrected_micrographs.star"
    )
    star_doc = cif.read_file(corrected_star_path)
    new_star_doc = remove_corrected_star_slice(corrected_star_path, slice(2, None))
    new_star_doc.write_file(corrected_star_path)
    msgs = project.messages

    assert len(msgs) == 5
    assert len(msgs[0]["ispyb"]) == 22

    star_doc.write_file(corrected_star_path)
    (
        dials_data("relion_tutorial_data", pathlib=True)
        / "MotionCorr"
        / "job002"
        / "RELION_JOB_EXIT_SUCCESS"
    ).touch()
    project.load()
    msgs = project.messages

    assert len(msgs) == 5
    assert len(msgs[0]["ispyb"]) == 2
    assert (
        msgs[0]["ispyb"][0]["buffer_command"]["micrograph_full_path"]
        == "MotionCorr/job002/Movies/20170629_00021_frameImage.mrc"
    )
    mc_id = msgs[0]["ispyb"][0]["buffer_store"]
    assert msgs[1]["ispyb"][0]["buffer_lookup"]["motion_correction_id"] == mc_id


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_appended_results_are_picked_up_correctly_in_project_messages(dials_data):
    project = relion.Project(
        dials_data("relion_tutorial_data", pathlib=True),
        run_options=RelionItOptions,
        message_constructors={"ispyb": construct_message},
    )
    corrected_star_path = os.fspath(
        dials_data("relion_tutorial_data", pathlib=True)
        / "MotionCorr"
        / "job002"
        / "corrected_micrographs.star"
    )
    star_doc = cif.read_file(corrected_star_path)
    new_star_doc = remove_corrected_star_slice(corrected_star_path, slice(0, -2))
    new_star_doc.write_file(corrected_star_path)
    msgs = project.messages

    assert len(msgs) == 5
    assert len(msgs[0]["ispyb"]) == 22

    star_doc.write_file(corrected_star_path)
    (
        dials_data("relion_tutorial_data", pathlib=True)
        / "MotionCorr"
        / "job002"
        / "RELION_JOB_EXIT_SUCCESS"
    ).touch()
    project.load()
    msgs = project.messages

    assert len(msgs) == 5
    assert len(msgs[0]["ispyb"]) == 2
    assert (
        msgs[0]["ispyb"][0]["buffer_command"]["micrograph_full_path"]
        == "MotionCorr/job002/Movies/20170629_00048_frameImage.mrc"
    )
    mc_id = msgs[0]["ispyb"][0]["buffer_store"]
    assert msgs[1]["ispyb"][0]["buffer_lookup"]["motion_correction_id"] == mc_id

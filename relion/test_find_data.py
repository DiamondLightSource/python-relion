import pytest
import relion.FindData as FD
from pathlib import Path


@pytest.fixture
def input_test_dict():
    return {
        1: {
            "name": "MotionCorr/job002",
            1: [
                "total_motion",
                "corrected_micrographs.star",
                1,
                "_rlnAccumMotionTotal",
            ],
            2: [
                "early_motion",
                "corrected_micrographs.star",
                1,
                "_rlnAccumMotionEarly",
            ],
            3: ["late_motion", "corrected_micrographs.star", 1, "_rlnAccumMotionLate"],
        },
        2: {
            "name": "CtfFind/job003",
            1: ["astigmatism", "micrographs_ctf.star", 1, "_rlnCtfAstigmatism"],
            2: ["defocusU", "micrographs_ctf.star", 1, "_rlnDefocusU"],
            3: ["defocusV", "micrographs_ctf.star", 1, "_rlnDefocusV"],
            4: ["astigmatism_angle", "micrographs_ctf.star", 1, "_rlnDefocusAngle"],
            5: ["max_resolution", "micrographs_ctf.star", 1, "_rlnCtfMaxResolution"],
            6: ["cc/fig_of_merit", "micrographs_ctf.star", 1, "_rlnCtfFigureOfMerit"],
        },
    }


@pytest.fixture
def input_test_folder(dials_data):
    return Path(dials_data("relion_tutorial_data"))


@pytest.fixture
def input_file_type():
    return "star"


# Test that the result of FindData.get_data can be indexed - this is how the values will be accessed
# To keep the generalness we won't refer directly to any value when accessing the data


def test_total_motion_value(input_test_folder, input_file_type, input_test_dict):
    FDobject = FD.FindData(input_test_folder, input_file_type, input_test_dict)
    data = FDobject.get_data()
    # This refers to Motioncorr, total_motion, first value
    assert data[0][1][1] == "16.420495"


def test_early_motion_value(input_test_folder, input_file_type, input_test_dict):
    FDobject = FD.FindData(input_test_folder, input_file_type, input_test_dict)
    data = FDobject.get_data()
    assert data[0][2][1] == "2.506308"


def test_late_motion_value(input_test_folder, input_file_type, input_test_dict):
    FDobject = FD.FindData(input_test_folder, input_file_type, input_test_dict)
    data = FDobject.get_data()
    assert data[0][3][1] == "13.914187"


def test_astigmatism_value(input_test_folder, input_file_type, input_test_dict):
    FDobject = FD.FindData(input_test_folder, input_file_type, input_test_dict)
    data = FDobject.get_data()
    assert data[1][1][1] == "288.135742"


def test_defocusU_value(input_test_folder, input_file_type, input_test_dict):
    FDobject = FD.FindData(input_test_folder, input_file_type, input_test_dict)
    data = FDobject.get_data()
    assert data[1][2][1] == "10863.857422"


def test_defocusV_value(input_test_folder, input_file_type, input_test_dict):
    FDobject = FD.FindData(input_test_folder, input_file_type, input_test_dict)
    data = FDobject.get_data()
    assert data[1][3][1] == "10575.721680"


def test_astigmatism_angle_value(input_test_folder, input_file_type, input_test_dict):
    FDobject = FD.FindData(input_test_folder, input_file_type, input_test_dict)
    data = FDobject.get_data()
    assert data[1][4][1] == "77.967194"


def test_max_resolution_value(input_test_folder, input_file_type, input_test_dict):
    FDobject = FD.FindData(input_test_folder, input_file_type, input_test_dict)
    data = FDobject.get_data()
    assert data[1][5][1] == "4.809192"


def test_fig_of_merit_value(input_test_folder, input_file_type, input_test_dict):
    FDobject = FD.FindData(input_test_folder, input_file_type, input_test_dict)
    data = FDobject.get_data()
    assert data[1][6][1] == "0.131144"

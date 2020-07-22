import pytest
import relion.FindData as FD
from pathlib import Path


@pytest.fixture
def input_test_dict_star():
    return {
        1: {
            "name": "MotionCorr/job002",
            1: [
                "total_motion",
                "star",
                "corrected_micrographs.star",
                1,
                "_rlnAccumMotionTotal",
            ],
            2: [
                "early_motion",
                "star",
                "corrected_micrographs.star",
                1,
                "_rlnAccumMotionEarly",
            ],
            3: [
                "late_motion",
                "star",
                "corrected_micrographs.star",
                1,
                "_rlnAccumMotionLate",
            ],
        },
        2: {
            "name": "CtfFind/job003",
            1: ["astigmatism", "star", "micrographs_ctf.star", 1, "_rlnCtfAstigmatism"],
            2: ["defocusU", "star", "micrographs_ctf.star", 1, "_rlnDefocusU"],
            3: ["defocusV", "star", "micrographs_ctf.star", 1, "_rlnDefocusV"],
            4: [
                "astigmatism_angle",
                "star",
                "micrographs_ctf.star",
                1,
                "_rlnDefocusAngle",
            ],
            5: [
                "max_resolution",
                "star",
                "micrographs_ctf.star",
                1,
                "_rlnCtfMaxResolution",
            ],
            6: [
                "cc/fig_of_merit",
                "star",
                "micrographs_ctf.star",
                1,
                "_rlnCtfFigureOfMerit",
            ],
            7: [
                "cc/fig_doesnt_exist",
                "star",
                "micrographs_ctf.star",
                1,
                "_rlnCtfFigure",
            ],
        },
    }


@pytest.fixture
def input_test_dict_out_file():
    return {
        1: {
            "name": "MotionCorr/job002/Movies/GridSquare_24959318/Data",
            1: [
                "Align patch",
                "out",
                "FoilHole_24363955_Data_24996956_24996958_20200625_021313_fractions.out",
            ],
        }
    }


@pytest.fixture
def input_test_folder(dials_data):
    return Path(dials_data("relion_tutorial_data"))


@pytest.fixture
def input_test_folder_2():
    return Path("/dls/m02/data/2020/bi27053-1/processing/Relion_nd/")


@pytest.fixture
def input_file_type():
    return "star"


@pytest.fixture
def input_file_type_2():
    return "out"


def test_total_motion_value(input_test_folder, input_test_dict_star):
    FDobject = FD.FindData(input_test_folder, input_test_dict_star)
    data = FDobject.get_data()
    # This refers to Motioncorr, total_motion, first value
    assert data[0][1][1] == "16.420495"


def test_early_motion_value(input_test_folder, input_test_dict_star):
    FDobject = FD.FindData(input_test_folder, input_test_dict_star)
    data = FDobject.get_data()
    assert data[0][2][1] == "2.506308"


def test_late_motion_value(input_test_folder, input_test_dict_star):
    FDobject = FD.FindData(input_test_folder, input_test_dict_star)
    data = FDobject.get_data()
    assert data[0][3][1] == "13.914187"


def test_astigmatism_value(input_test_folder, input_test_dict_star):
    FDobject = FD.FindData(input_test_folder, input_test_dict_star)
    data = FDobject.get_data()
    assert data[1][1][1] == "288.135742"


def test_defocusU_value(input_test_folder, input_test_dict_star):
    FDobject = FD.FindData(input_test_folder, input_test_dict_star)
    data = FDobject.get_data()
    assert data[1][2][1] == "10863.857422"


def test_defocusV_value(input_test_folder, input_test_dict_star):
    FDobject = FD.FindData(input_test_folder, input_test_dict_star)
    data = FDobject.get_data()
    assert data[1][3][1] == "10575.721680"


def test_astigmatism_angle_value(input_test_folder, input_test_dict_star):
    FDobject = FD.FindData(input_test_folder, input_test_dict_star)
    data = FDobject.get_data()
    assert data[1][4][1] == "77.967194"


def test_max_resolution_value(input_test_folder, input_test_dict_star):
    FDobject = FD.FindData(input_test_folder, input_test_dict_star)
    data = FDobject.get_data()
    assert data[1][5][1] == "4.809192"


def test_fig_of_merit_value(input_test_folder, input_test_dict_star):
    FDobject = FD.FindData(input_test_folder, input_test_dict_star)
    data = FDobject.get_data()
    assert data[1][6][1] == "0.131144"


def test_out_file_finds_string(input_test_folder_2, input_test_dict_out_file):
    FDobject = FD.FindData(input_test_folder_2, input_test_dict_out_file)
    assert FDobject.get_data() is True


def test_output_is_serialisable(input_test_folder, input_test_dict_star):
    FDobject = FD.FindData(input_test_folder, input_test_dict_star)
    data = FDobject.get_data()
    assert data == eval(repr(data))

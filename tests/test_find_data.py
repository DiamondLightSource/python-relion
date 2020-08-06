import pytest
import relion.FindData as FD
from pathlib import Path
import relion.input_dict
from pprint import pprint


@pytest.fixture
def input_test_dict_star():
    return relion.input_dict.input_star


@pytest.fixture
def input_test_dict_class():
    return relion.input_dict.input_class_number


@pytest.fixture
def input_test_dict_2Dclass():
    return relion.input_dict.input_2D_class


@pytest.fixture
def input_test_dict_out_file():
    return relion.input_dict.input_out


@pytest.fixture
def input_test_folder(dials_data):
    return Path(dials_data("relion_tutorial_data"))


@pytest.fixture
def input_test_folder_2():
    return Path("/dls/m02/data/2020/bi27053-1/processing/Relion_nd/")


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


def test_2D_class_distribution_value(input_test_folder, input_test_dict_star):
    FDobject = FD.FindData(input_test_folder, input_test_dict_star)
    data = FDobject.get_data()
    assert data[2][1][1] == "0.016487"


def test_3D_class_distribution_value(input_test_folder, input_test_dict_star):
    FDobject = FD.FindData(input_test_folder, input_test_dict_star)
    data = FDobject.get_data()
    assert data[3][1][1] == "0.055685"


def test_out_file_finds_string(input_test_folder_2, input_test_dict_out_file):
    FDobject = FD.FindData(input_test_folder_2, input_test_dict_out_file)
    assert FDobject.get_data() is True


def test_output_is_serialisable(input_test_folder, input_test_dict_star):
    FDobject = FD.FindData(input_test_folder, input_test_dict_star)
    data = FDobject.get_data()
    assert data == eval(repr(data))


def test_class_number(input_test_folder, input_test_dict_class):
    FDobject = FD.FindData(input_test_folder, input_test_dict_class)
    data = FDobject.get_data()
    assert data[0][1][1] == "24"


def test_2D_class_output(input_test_folder, input_test_dict_2Dclass):
    FDobject = FD.FindData(input_test_folder, input_test_dict_2Dclass)
    data = FDobject.get_data()
    pprint(data)

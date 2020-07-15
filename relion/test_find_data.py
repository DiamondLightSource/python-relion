import pytest
import relion.FindData as FD


@pytest.fixture
def input_test_dict():
    return {
        1: {
            "name": "MotionCorr",
            1: [
                "total_motion",
                "corrected_micrographs.star",
                1,
                "_rlnAccumMotionTotal",
            ],
        },
        2: {
            "name": "CtfFind",
            1: ["astigmatism", "micrographs_ctf.star", 1, "_rlnCtfAstigmatism"],
            2: ["defocusU", "micrographs_ctf.star", 1, "_rlnDefocusU"],
            3: ["defocusV", "micrographs_ctf.star", 1, "_rlnDefocusV"],
        },
    }


@pytest.fixture
def input_test_folder(dials_data):
    return dials_data("relion_tutorial_data")


@pytest.fixture
def input_file_type():
    return "star"


# Test that the result of FindData.get_data can be indexed - this is how the values will be accessed
# To keep the generalness we don't refer directly to any value when accessing the data

file_type = "star"
test_dict = {
    1: {
        "name": "MotionCorr/relioncor2",
        1: ["total_motion", "corrected_micrographs.star", 1, "_rlnAccumMotionTotal"],
    },
    2: {
        "name": "CtfFind/job003",
        1: ["astigmatism", "micrographs_ctf.star", 1, "_rlnCtfAstigmatism"],
        2: ["defocusU", "micrographs_ctf.star", 1, "_rlnDefocusU"],
        3: ["defocusV", "micrographs_ctf.star", 1, "_rlnDefocusV"],
    },
}


def test_return_val_is_indexable(input_test_folder):
    FDobject = FD.FindData(input_test_folder, file_type, test_dict)
    data = FDobject.get_data()
    # This refers to Motioncorr, total_motion, first value
    # Whilst not the most intuitive to access, this way is less specific and possibly less fragile - not relying on certain names etc.
    # As long as the code guarantees all data from the same section stays together, the name can be searched for for identifying when needed
    assert data[0][1][1] == "16.420495"

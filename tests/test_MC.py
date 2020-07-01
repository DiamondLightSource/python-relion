import relion.MotionCorrection as MotionCorrection
import pytest


@pytest.fixture
def known_file_path():
    return "/dls/ebic/data/staff-scratch/ccpem/Relion31TutorialPrecalculatedResults/MotionCorr/relioncor2/corrected_micrographs.star"


@pytest.fixture
def known_MC_object():
    MC = MotionCorrection.MotionCorrection(known_file_path)
    return MC


@pytest.mark.motion_correction  # Possible route if we decide to have one test_relion file containing all sections
# def test_total_motion_is_numeric():
#    x = known_MC_object.total_motion()
#    try:
#        float(x)
#        return True
#    except ValueError:
#        return False

# def test_block_count():
#    assert known_MC_object.number_of_blocks == 2


def test_total_motion_for_known_file():
    MC = MotionCorrection.MotionCorrection(
        "/dls/ebic/data/staff-scratch/ccpem/Relion31TutorialPrecalculatedResults/MotionCorr/relioncor2/corrected_micrographs.star"
    )
    assert MC.total_motion() == "16.420495"
    # assert known_MC_object.total_motion() == '16.420495'

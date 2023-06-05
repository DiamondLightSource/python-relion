from __future__ import annotations

import sys
from unittest import mock

import pytest
import zocalo.configuration
from workflows.transport.offline_transport import OfflineTransport

from relion.zocalo import motioncorr


@pytest.fixture
def mock_zocalo_configuration(tmp_path):
    mock_zc = mock.MagicMock(zocalo.configuration.Configuration)
    mock_zc.storage = {
        "zocalo.recipe_directory": tmp_path,
    }
    return mock_zc


@pytest.fixture
def mock_environment(mock_zocalo_configuration):
    return {"config": mock_zocalo_configuration}


@pytest.fixture
def offline_transport(mocker):
    transport = OfflineTransport()
    mocker.spy(transport, "send")
    return transport


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
@mock.patch("relion.zocalo.motioncorr.procrunner.run")
def test_motioncorr_service(
    mock_procrunner, mock_environment, offline_transport, tmp_path
):
    """
    Send a test message to MotionCorr
    This should call the mock procrunner then send messages on to
    the murfey_feedback, ispyb_connector and images services.
    It also creates the next jobs (ctffind and two icebreaker jobs)
    and the node_creator is called for both import and motion correction.
    """
    mock_procrunner().returncode = 0

    header = {
        "message-id": mock.sentinel,
        "subscription": mock.sentinel,
    }
    motioncorr_test_message = {
        "parameters": {
            "collection_type": "spa",
            "pix_size": 0.1,
            "autopick": {"autopick": "autopick"},
            "ctf": {"ctf": "ctf"},
            "movie": "Movies/sample.tiff",
            "mrc_out": "MotionCorr/job002/Movies/sample.mrc",
            "patch_size": 5,
            "gpu": 0,
            "gain_ref": "gain.mrc",
            "mc_uuid": 0,
            "rot_gain": None,
            "flip_gain": None,
            "dark": None,
            "use_gpus": None,
            "sum_range": None,
            "iter": None,
            "tol": None,
            "throw": None,
            "trunc": None,
            "fm_ref": 1,
            "kv": None,
            "fm_dose": 1,
            "fm_int_file": None,
            "mag": None,
            "ft_bin": None,
            "serial": None,
            "in_suffix": None,
            "eer_sampling": None,
            "out_stack": None,
            "bft": None,
            "group": None,
            "detect_file": None,
            "arc_dir": None,
            "in_fm_motion": None,
            "split_sum": None,
            "movie_id": 1,
            "relion_it_options": {
                "do_icebreaker_job_group": True,
                "cryolo_threshold": 0.3,
                "ampl_contrast": 0.2,
            },
        },
        "content": "dummy",
    }

    # Set up the mock service
    service = motioncorr.MotionCorr(environment=mock_environment)
    service.transport = offline_transport
    service.start()

    # Work out the expected shifts
    service.x_shift_list = [-3.0, 3.0]
    service.y_shift_list = [4.0, -4.0]
    service.each_total_motion = [5.0, 5.0]
    total_motion = 10.0
    average_motion_per_frame = 5

    # Send a message to the service
    service.motion_correction(None, header=header, message=motioncorr_test_message)

    assert mock_procrunner.call_count == 2
    mock_procrunner.assert_called_with(
        command=[
            "MotionCor2",
            "-InTiff",
            motioncorr_test_message["parameters"]["movie"],
            "-PixSize",
            str(motioncorr_test_message["parameters"]["pix_size"]),
            "-OutMrc",
            motioncorr_test_message["parameters"]["mrc_out"],
            "-Gain",
            motioncorr_test_message["parameters"]["gain_ref"],
            "-FmRef",
            "1",
            "-FmDose",
            "1.0",
        ],
        callback_stdout=mock.ANY,
    )

    # Check that the correct messages were sent
    offline_transport.send.assert_any_call(
        destination="icebreaker",
        message={
            "parameters": {
                "icebreaker_type": "micrographs",
                "input_micrographs": motioncorr_test_message["parameters"]["mrc_out"],
                "output_path": "IceBreaker/job003/",
                "mc_uuid": motioncorr_test_message["parameters"]["mc_uuid"],
                "relion_it_options": motioncorr_test_message["parameters"][
                    "relion_it_options"
                ],
                "total_motion": total_motion,
            },
            "content": "dummy",
        },
    )
    offline_transport.send.assert_any_call(
        destination="icebreaker",
        message={
            "parameters": {
                "icebreaker_type": "enhancecontrast",
                "input_micrographs": motioncorr_test_message["parameters"]["mrc_out"],
                "output_path": "IceBreaker/job004/",
                "mc_uuid": motioncorr_test_message["parameters"]["mc_uuid"],
                "relion_it_options": motioncorr_test_message["parameters"][
                    "relion_it_options"
                ],
                "total_motion": total_motion,
            },
            "content": "dummy",
        },
    )
    offline_transport.send.assert_any_call(
        destination="ctffind",
        message={
            "parameters": {
                "ctf": "ctf",
                "input_image": motioncorr_test_message["parameters"]["mrc_out"],
                "mc_uuid": motioncorr_test_message["parameters"]["mc_uuid"],
                "relion_it_options": motioncorr_test_message["parameters"][
                    "relion_it_options"
                ],
                "amplitude_contrast": motioncorr_test_message["parameters"][
                    "relion_it_options"
                ]["ampl_contrast"],
                "collection_type": "spa",
                "output_image": "CtfFind/job006/Movies/sample.ctf",
                "pix_size": motioncorr_test_message["parameters"]["pix_size"],
            },
            "content": "dummy",
        },
    )
    offline_transport.send.assert_any_call(
        destination="ispyb_connector",
        message={
            "parameters": {
                "first_frame": 1,
                "last_frame": 2,
                "total_motion": total_motion,
                "average_motion_per_frame": average_motion_per_frame,
                "drift_plot_full_path": "MotionCorr/job002/Movies/sample_drift_plot.json",
                "micrograph_snapshot_full_path": "MotionCorr/job002/Movies/sample.jpeg",
                "micrograph_full_path": motioncorr_test_message["parameters"][
                    "mrc_out"
                ],
                "patches_used_x": motioncorr_test_message["parameters"]["patch_size"],
                "patches_used_y": motioncorr_test_message["parameters"]["patch_size"],
                "buffer_store": motioncorr_test_message["parameters"]["mc_uuid"],
                "dose_per_frame": motioncorr_test_message["parameters"]["fm_dose"],
                "ispyb_command": "buffer",
                "buffer_command": {"ispyb_command": "insert_motion_correction"},
            },
            "content": {"dummy": "dummy"},
        },
    )
    offline_transport.send.assert_any_call(
        destination="murfey_feedback",
        message={
            "register": "motion_corrected",
            "movie": motioncorr_test_message["parameters"]["movie"],
            "mrc_out": motioncorr_test_message["parameters"]["mrc_out"],
            "movie_id": motioncorr_test_message["parameters"]["movie_id"],
        },
    )
    offline_transport.send.assert_any_call(
        destination="images",
        message={
            "parameters": {"images_command": "mrc_to_jpeg"},
            "file": motioncorr_test_message["parameters"]["mrc_out"],
        },
    )
    offline_transport.send.assert_any_call(
        destination="spa.node_creator",
        message={
            "parameters": {
                "job_type": "relion.import.movies",
                "input_file": motioncorr_test_message["parameters"]["movie"],
                "output_file": "Import/job001/Movies/sample.tiff",
                "relion_it_options": motioncorr_test_message["parameters"][
                    "relion_it_options"
                ],
            },
            "content": "dummy",
        },
    )
    offline_transport.send.assert_any_call(
        destination="spa.node_creator",
        message={
            "parameters": {
                "job_type": "relion.motioncorr.motioncor2",
                "input_file": "Import/job001/Movies/sample.tiff",
                "output_file": motioncorr_test_message["parameters"]["mrc_out"],
                "relion_it_options": motioncorr_test_message["parameters"][
                    "relion_it_options"
                ],
                "results": {
                    "total_motion": str(total_motion),
                    "early_motion": str(total_motion),
                    "late_motion": str(0),
                },
            },
            "content": "dummy",
        },
    )


def test_parse_motioncorr_output(mock_environment, offline_transport):
    """
    Send test lines to the output parser
    to check the shift values are being read in
    """
    service = motioncorr.MotionCorr(environment=mock_environment)
    service.transport = offline_transport
    service.start()

    motioncorr.MotionCorr.parse_mc_output(
        service, "...... Frame (  1) shift:    -3.0      4.0"
    )
    motioncorr.MotionCorr.parse_mc_output(
        service, "...... Frame (  2) shift:    3.0      -4.0"
    )
    assert service.x_shift_list == [-3.0, 3.0]
    assert service.y_shift_list == [4.0, -4.0]
    assert service.each_total_motion == [5.0, 5.0]

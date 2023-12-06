from __future__ import annotations

import sys
from unittest import mock

import pytest
import zocalo.configuration
from workflows.transport.offline_transport import OfflineTransport

from relion.zocalo import motioncorr
from relion.zocalo.spa_relion_service_options import RelionServiceOptions


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
@mock.patch("relion.zocalo.motioncorr.subprocess.run")
def test_motioncorr_service_spa(
    mock_subprocess, mock_environment, offline_transport, tmp_path
):
    """
    Send a test message to MotionCorr
    This should call the mock subprocess then send messages on to
    the ispyb_connector and images services.
    It also creates the next jobs (ctffind and two icebreaker jobs)
    and the node_creator is called for both import and motion correction.
    """
    mock_subprocess().returncode = 0
    mock_subprocess().stdout = "stdout".encode("ascii")
    mock_subprocess().stderr = "stderr".encode("ascii")

    header = {
        "message-id": mock.sentinel,
        "subscription": mock.sentinel,
    }
    motioncorr_test_message = {
        "parameters": {
            "experiment_type": "spa",
            "pix_size": 0.1,
            "autopick": {"autopick": "autopick"},
            "ctf": {"ctf": "ctf"},
            "movie": f"{tmp_path}/Movies/sample.tiff",
            "mrc_out": f"{tmp_path}/MotionCorr/job002/Movies/sample.mrc",
            "patch_size": {"x": 5, "y": 5},
            "gpu": 0,
            "gain_ref": "gain.mrc",
            "mc_uuid": 0,
            "picker_uuid": 0,
            "rot_gain": 1,
            "flip_gain": 1,
            "dark": "dark",
            "use_gpus": 1,
            "sum_range": {"sum1": "sum1", "sum2": "sum2"},
            "iter": 1,
            "tol": 1.1,
            "throw": 1,
            "trunc": 1,
            "fm_ref": 1,
            "kv": 300,
            "fm_dose": 1,
            "fm_int_file": "fm_int_file",
            "mag": {"mag1": "mag1", "mag2": "mag2"},
            "ft_bin": 2,
            "serial": 1,
            "in_suffix": "mrc",
            "eer_sampling": 1,
            "out_stack": 1,
            "bft": {"global": 500, "local": 150},
            "group": 1,
            "defect_file": "file",
            "arc_dir": "arc_dir",
            "in_fm_motion": 1,
            "split_sum": 1,
            "movie_id": 1,
            "relion_options": {
                "angpix": 0.1,
                "do_icebreaker_jobs": True,
                "cryolo_threshold": 0.3,
                "ampl_contrast": 0.2,
            },
        },
        "content": "dummy",
    }
    output_relion_options = dict(RelionServiceOptions())
    output_relion_options.update(
        motioncorr_test_message["parameters"]["relion_options"]
    )
    output_relion_options["angpix"] = 0.2

    # Set up the mock service
    service = motioncorr.MotionCorr(environment=mock_environment)
    service.transport = offline_transport
    service.start()

    # Work out the expected shifts
    service.x_shift_list = [-3.0, 3.0]
    service.y_shift_list = [4.0, -4.0]
    service.each_total_motion = [5.0, 5.0]
    total_motion = 10.0
    early_motion = 10.0
    late_motion = 0.0
    average_motion_per_frame = 5

    # Send a message to the service
    service.motion_correction(None, header=header, message=motioncorr_test_message)

    mc_command = [
        "MotionCor2",
        "-InTiff",
        motioncorr_test_message["parameters"]["movie"],
        "-OutMrc",
        motioncorr_test_message["parameters"]["mrc_out"],
        "-PixSize",
        str(motioncorr_test_message["parameters"]["pix_size"]),
        "-FmDose",
        "1.0",
        "-Patch",
        "5 5",
        "-Gpu",
        "0",
        "-Gain",
        motioncorr_test_message["parameters"]["gain_ref"],
        "-RotGain",
        "1",
        "-FlipGain",
        "1",
        "-Dark",
        "dark",
        "-UseGpus",
        "1",
        "-SumRange",
        "sum1 sum2",
        "-Iter",
        "1",
        "-Tol",
        "1.1",
        "-Throw",
        "1",
        "-Trunc",
        "1",
        "-FmRef",
        "1",
        "-Kv",
        "300",
        "-Mag",
        "mag1 mag2",
        "-FtBin",
        "2.0",
        "-Serial",
        "1",
        "-InSuffix",
        "mrc",
        "-EerSampling",
        "1",
        "-OutStack",
        "1",
        "-Bft",
        "500 150",
        "-Group",
        "1",
        "-DefectFile",
        "file",
        "-ArcDir",
        "arc_dir",
        "-InFmMotion",
        "1",
        "-SplitSum",
        "1",
    ]

    assert mock_subprocess.call_count == 4
    mock_subprocess.assert_called_with(mc_command, capture_output=True)

    # Check that the correct messages were sent
    offline_transport.send.assert_any_call(
        destination="icebreaker",
        message={
            "parameters": {
                "icebreaker_type": "micrographs",
                "input_micrographs": motioncorr_test_message["parameters"]["mrc_out"],
                "output_path": f"{tmp_path}/IceBreaker/job003/",
                "mc_uuid": motioncorr_test_message["parameters"]["mc_uuid"],
                "relion_options": output_relion_options,
                "total_motion": total_motion,
                "early_motion": early_motion,
                "late_motion": late_motion,
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
                "output_path": f"{tmp_path}/IceBreaker/job004/",
                "mc_uuid": motioncorr_test_message["parameters"]["mc_uuid"],
                "relion_options": output_relion_options,
                "total_motion": total_motion,
                "early_motion": early_motion,
                "late_motion": late_motion,
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
                "picker_uuid": motioncorr_test_message["parameters"]["picker_uuid"],
                "relion_options": output_relion_options,
                "amplitude_contrast": output_relion_options["ampl_contrast"],
                "experiment_type": "spa",
                "output_image": f"{tmp_path}/CtfFind/job006/Movies/sample.ctf",
                "pix_size": motioncorr_test_message["parameters"]["pix_size"] * 2,
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
                "drift_plot_full_path": f"{tmp_path}/MotionCorr/job002/Movies/sample_drift_plot.json",
                "micrograph_snapshot_full_path": f"{tmp_path}/MotionCorr/job002/Movies/sample.jpeg",
                "micrograph_full_path": motioncorr_test_message["parameters"][
                    "mrc_out"
                ],
                "patches_used_x": motioncorr_test_message["parameters"]["patch_size"][
                    "x"
                ],
                "patches_used_y": motioncorr_test_message["parameters"]["patch_size"][
                    "y"
                ],
                "buffer_store": motioncorr_test_message["parameters"]["mc_uuid"],
                "dose_per_frame": motioncorr_test_message["parameters"]["fm_dose"],
                "ispyb_command": "buffer",
                "buffer_command": {"ispyb_command": "insert_motion_correction"},
            },
            "content": {"dummy": "dummy"},
        },
    )
    offline_transport.send.assert_any_call(
        destination="images",
        message={
            "image_command": "mrc_to_jpeg",
            "file": motioncorr_test_message["parameters"]["mrc_out"],
        },
    )
    offline_transport.send.assert_any_call(
        destination="node_creator",
        message={
            "parameters": {
                "job_type": "relion.import.movies",
                "input_file": motioncorr_test_message["parameters"]["movie"],
                "output_file": f"{tmp_path}/Import/job001/Movies/sample.tiff",
                "relion_options": output_relion_options,
                "command": "",
                "stdout": "",
                "stderr": "",
            },
            "content": "dummy",
        },
    )
    offline_transport.send.assert_any_call(
        destination="node_creator",
        message={
            "parameters": {
                "job_type": "relion.motioncorr.motioncor2",
                "input_file": f"{tmp_path}/Import/job001/Movies/sample.tiff",
                "output_file": motioncorr_test_message["parameters"]["mrc_out"],
                "relion_options": output_relion_options,
                "command": " ".join(mc_command),
                "stdout": "stdout",
                "stderr": "stderr",
                "results": {
                    "total_motion": total_motion,
                    "early_motion": early_motion,
                    "late_motion": late_motion,
                },
            },
            "content": "dummy",
        },
    )


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
@mock.patch("relion.zocalo.motioncorr.subprocess.run")
def test_motioncorr_service_tomo(
    mock_subprocess, mock_environment, offline_transport, tmp_path
):
    """
    Send a test message to MotionCorr
    This should call the mock subprocess then send messages on to
    the murfey_feedback, ispyb_connector and images services.
    It also creates the ctffind job.
    """
    mock_subprocess().returncode = 0
    mock_subprocess().stdout = "stdout".encode("ascii")
    mock_subprocess().stderr = "stderr".encode("ascii")

    header = {
        "message-id": mock.sentinel,
        "subscription": mock.sentinel,
    }
    motioncorr_test_message = {
        "parameters": {
            "experiment_type": "tomography",
            "pix_size": 0.1,
            "autopick": {"autopick": "autopick"},
            "ctf": {"ctf": "ctf"},
            "movie": f"{tmp_path}/Movies/sample.tiff",
            "mrc_out": f"{tmp_path}/MotionCorr/job002/Movies/sample.mrc",
            "patch_size": {"x": 5, "y": 5},
            "gpu": 0,
            "gain_ref": "gain.mrc",
            "mc_uuid": 0,
            "picker_uuid": 0,
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

    assert mock_subprocess.call_count == 4
    mock_subprocess.assert_called_with(
        [
            "MotionCor2",
            "-InTiff",
            motioncorr_test_message["parameters"]["movie"],
            "-OutMrc",
            motioncorr_test_message["parameters"]["mrc_out"],
            "-PixSize",
            str(motioncorr_test_message["parameters"]["pix_size"]),
            "-FmDose",
            "1.0",
            "-Patch",
            "5 5",
            "-Gpu",
            "0",
            "-Gain",
            motioncorr_test_message["parameters"]["gain_ref"],
            "-FmRef",
            "1",
        ],
        capture_output=True,
    )

    # Check that the correct messages were sent
    offline_transport.send.assert_any_call(
        destination="ctffind",
        message={
            "parameters": {
                "ctf": "ctf",
                "input_image": motioncorr_test_message["parameters"]["mrc_out"],
                "mc_uuid": motioncorr_test_message["parameters"]["mc_uuid"],
                "picker_uuid": motioncorr_test_message["parameters"]["picker_uuid"],
                "experiment_type": "tomography",
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
                "drift_plot_full_path": f"{tmp_path}/MotionCorr/job002/Movies/sample_drift_plot.json",
                "micrograph_snapshot_full_path": f"{tmp_path}/MotionCorr/job002/Movies/sample.jpeg",
                "micrograph_full_path": motioncorr_test_message["parameters"][
                    "mrc_out"
                ],
                "patches_used_x": motioncorr_test_message["parameters"]["patch_size"][
                    "x"
                ],
                "patches_used_y": motioncorr_test_message["parameters"]["patch_size"][
                    "y"
                ],
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
            "image_command": "mrc_to_jpeg",
            "file": motioncorr_test_message["parameters"]["mrc_out"],
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

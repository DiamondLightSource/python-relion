from __future__ import annotations

import sys
from unittest import mock

import pytest
import zocalo.configuration
from workflows.transport.offline_transport import OfflineTransport

from relion.zocalo import tomo_align


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
@mock.patch("relion.zocalo.tomo_align.subprocess.run")
@mock.patch("relion.zocalo.tomo_align.px.scatter")
def test_tomo_align_service(
    mock_plotly, mock_subprocess, mock_environment, offline_transport, tmp_path
):
    """
    Send a test message to TomoAlign
    This should call the mock subprocess then send messages on to
    the murfey_feedback, ispyb_connector and images services.
    """
    mock_subprocess().returncode = 0
    mock_subprocess().stdout = "stdout".encode("ascii")
    mock_subprocess().stderr = "stderr".encode("ascii")

    header = {
        "message-id": mock.sentinel,
        "subscription": mock.sentinel,
    }
    tomo_align_test_message = {
        "parameters": {
            "stack_file": f"{tmp_path}/test_stack.st",
            "path_pattern": None,
            "input_file_list": str([[f"{tmp_path}/input_file_1.mrc", "1.00"]]),
            "position": None,
            "aretomo_output_file": f"{tmp_path}/aretomo_output.mrc",
            "vol_z": 1200,
            "align": None,
            "out_bin": 4,
            "tilt_axis": None,
            "tilt_cor": 1,
            "flip_int": None,
            "flip_vol": 1,
            "wbp": None,
            "roi_file": [],
            "patch": None,
            "kv": None,
            "align_file": None,
            "angle_file": f"{tmp_path}/angles.file",
            "align_z": None,
            "pix_size": 1e-10,
            "init_val": None,
            "refine_flag": None,
            "out_imod": 1,
            "out_imod_xf": None,
            "dark_tol": None,
            "manual_tilt_offset": None,
        },
        "content": "dummy",
    }

    # Set up the mock service
    service = tomo_align.TomoAlign(environment=mock_environment)
    service.transport = offline_transport
    service.start()

    service.rot_centre_z_list = [1.1, 2.1]
    service.tilt_offset = 1.1
    service.mag = 1000
    service.rot = 0

    (tmp_path / "test_stack_aretomo_Imod").mkdir()
    (tmp_path / "test_stack.aln").touch()
    (tmp_path / "test_stack_aretomo_Imod/tilt.com").touch()

    # Send a message to the service
    service.tomo_align(None, header=header, message=tomo_align_test_message)

    assert mock_plotly.call_count == 1
    assert mock_subprocess.call_count == 5
    mock_subprocess.assert_any_call(
        [
            "newstack",
            "-fileinlist",
            f"{tmp_path}/test_stack_newstack.txt",
            "-output",
            f"{tmp_path}/test_stack.st",
            "-quiet",
        ]
    )
    mock_subprocess.assert_any_call(
        [
            "AreTomo",
            "-OutMrc",
            f"{tmp_path}/test_stack_aretomo.mrc",
            "-AngFile",
            f"{tmp_path}/angles.file",
            "-TiltCor",
            "1",
            "-InMrc",
            tomo_align_test_message["parameters"]["stack_file"],
            "-VolZ",
            str(tomo_align_test_message["parameters"]["vol_z"]),
            "-OutBin",
            str(tomo_align_test_message["parameters"]["out_bin"]),
            "-FlipVol",
            str(tomo_align_test_message["parameters"]["flip_vol"]),
            "-PixSize",
            "1.0",
            "-OutImod",
            str(tomo_align_test_message["parameters"]["out_imod"]),
        ],
        capture_output=True,
    )

    # Check that the correct messages were sent
    offline_transport.send.assert_any_call(
        destination="ispyb_connector",
        message={
            "parameters": {
                "ispyb_command": "multipart_message",
                "ispyb_command_list": [
                    {
                        "ispyb_command": "insert_tomogram",
                        "volume_file": "test_stack_aretomo.mrc",
                        "stack_file": f"{tmp_path}/test_stack.st",
                        "size_x": None,
                        "size_y": None,
                        "size_z": None,
                        "pixel_spacing": "4.0",
                        "tilt_angle_offset": "1.1",
                        "z_shift": 2.1,
                        "file_directory": f"{tmp_path}",
                        "central_slice_image": "test_stack_aretomo_thumbnail.jpeg",
                        "tomogram_movie": "test_stack_aretomo_movie.png",
                        "xy_shift_plot": "test_stack_xy_shift_plot.json",
                        "proj_xy": "test_stack_aretomo_projXY.jpeg",
                        "proj_xz": "test_stack_aretomo_projXZ.jpeg",
                        "store_result": "ispyb_tomogram_id",
                    },
                    {
                        "ispyb_command": "insert_tilt_image_alignment",
                        "psd_file": None,
                        "refined_magnification": "1000",
                        "refined_tilt_angle": None,
                        "refined_tilt_axis": "0",
                        "path": f"{tmp_path}/input_file_1.mrc",
                    },
                ],
            },
            "content": {"dummy": "dummy"},
        },
    )
    offline_transport.send.assert_any_call(
        destination="images",
        message={
            "image_command": "mrc_central_slice",
            "file": f"{tmp_path}/test_stack_aretomo.mrc",
        },
    )
    offline_transport.send.assert_any_call(
        destination="movie",
        message={
            "image_command": "mrc_to_apng",
            "file": f"{tmp_path}/test_stack_aretomo.mrc",
        },
    )
    offline_transport.send.assert_any_call(
        destination="projxy",
        message={
            "image_command": "mrc_to_jpeg",
            "file": f"{tmp_path}/test_stack_aretomo_projXY.mrc",
        },
    )
    offline_transport.send.assert_any_call(
        destination="projxz",
        message={
            "image_command": "mrc_to_jpeg",
            "file": f"{tmp_path}/test_stack_aretomo_projXZ.mrc",
        },
    )
    offline_transport.send.assert_any_call(
        destination="denoise",
        message={"volume": f"{tmp_path}/test_stack_aretomo.mrc"},
    )
    offline_transport.send.assert_any_call(destination="success", message="")


def test_parse_tomo_align_output(mock_environment, offline_transport):
    """
    Send test lines to the output parser
    to check the rotations and offsets are being read in
    """
    service = tomo_align.TomoAlign(environment=mock_environment)
    service.transport = offline_transport
    service.start()

    tomo_align.TomoAlign.parse_tomo_output(service, "Rot center Z 100.0 200.0 300.0")
    tomo_align.TomoAlign.parse_tomo_output(service, "Rot center Z 150.0 250.0 350.0")
    tomo_align.TomoAlign.parse_tomo_output(service, "Tilt offset 1.0, CC: 0.5")

    assert service.rot_centre_z_list == ["300.0", "350.0"]
    assert service.tilt_offset == 1.0

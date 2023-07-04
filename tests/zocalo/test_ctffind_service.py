from __future__ import annotations

import sys
from unittest import mock

import pytest
import zocalo.configuration
from workflows.transport.offline_transport import OfflineTransport

from relion.zocalo import ctffind
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
@mock.patch("relion.zocalo.ctffind.subprocess.run")
def test_ctffind_service(
    mock_subprocess, mock_environment, offline_transport, tmp_path
):
    """
    Send a test message to CTFFind
    This should call the mock subprocess then send messages on to the
    cryolo, node_creator, ispyb_connector and images services
    """
    mock_subprocess().returncode = 0
    mock_subprocess().stdout = "stdout".encode("utf8")
    mock_subprocess().stderr = "stderr".encode("utf8")

    header = {
        "message-id": mock.sentinel,
        "subscription": mock.sentinel,
    }
    ctffind_test_message = {
        "parameters": {
            "experiment_type": "spa",
            "pix_size": 0.1,
            "voltage": 300.0,
            "spher_aber": 2.7,
            "ampl_contrast": 0.8,
            "ampl_spectrum": 512,
            "min_res": 30.0,
            "max_res": 5.0,
            "min_defocus": 5000.0,
            "max_defocus": 50000.0,
            "defocus_step": 100.0,
            "astigmatism_known": "no",
            "slow_search": "no",
            "astigmatism_restrain": "no",
            "additional_phase_shift": "no",
            "expert_options": "no",
            "input_image": f"{tmp_path}/MotionCorr/job002/sample.mrc",
            "output_image": f"{tmp_path}/CtfFind/job006/sample.ctf",
            "mc_uuid": 0,
            "relion_options": {"cryolo_threshold": 0.3},
        },
        "content": "dummy",
    }
    output_relion_options = dict(RelionServiceOptions())
    output_relion_options.update(ctffind_test_message["parameters"]["relion_options"])

    # Set up the mock service
    service = ctffind.CTFFind(environment=mock_environment)
    service.transport = offline_transport
    service.start()

    # Set some parameters then send a message to the service
    service.defocus1 = 1
    service.defocus2 = 2
    service.astigmatism_angle = 3
    service.cc_value = 4
    service.estimated_resolution = 5
    service.ctf_find(None, header=header, message=ctffind_test_message)

    parameters_list = [
        ctffind_test_message["parameters"]["input_image"],
        ctffind_test_message["parameters"]["output_image"],
        ctffind_test_message["parameters"]["pix_size"],
        ctffind_test_message["parameters"]["voltage"],
        ctffind_test_message["parameters"]["spher_aber"],
        ctffind_test_message["parameters"]["ampl_contrast"],
        ctffind_test_message["parameters"]["ampl_spectrum"],
        ctffind_test_message["parameters"]["min_res"],
        ctffind_test_message["parameters"]["max_res"],
        ctffind_test_message["parameters"]["min_defocus"],
        ctffind_test_message["parameters"]["max_defocus"],
        ctffind_test_message["parameters"]["defocus_step"],
        ctffind_test_message["parameters"]["astigmatism_known"],
        ctffind_test_message["parameters"]["slow_search"],
        ctffind_test_message["parameters"]["astigmatism_restrain"],
        ctffind_test_message["parameters"]["additional_phase_shift"],
        ctffind_test_message["parameters"]["expert_options"],
    ]
    parameters_string = "\n".join(map(str, parameters_list))

    assert mock_subprocess.call_count == 4
    mock_subprocess.assert_called_with(
        ["ctffind"],
        input=parameters_string.encode("ascii"),
        capture_output=True,
    )

    # Check that the correct messages were sent
    offline_transport.send.assert_any_call(
        destination="cryolo",
        message={
            "content": "dummy",
            "parameters": {
                "input_path": ctffind_test_message["parameters"]["input_image"],
                "output_path": f"{tmp_path}/AutoPick/job007/STAR/sample.star",
                "ctf_values": {
                    "CtfImage": ctffind_test_message["parameters"]["output_image"],
                    "CtfMaxResolution": service.estimated_resolution,
                    "CtfFigureOfMerit": service.cc_value,
                    "DefocusU": service.defocus1,
                    "DefocusV": service.defocus2,
                    "DefocusAngle": service.astigmatism_angle,
                },
                "relion_options": output_relion_options,
                "mc_uuid": ctffind_test_message["parameters"]["mc_uuid"],
                "pix_size": ctffind_test_message["parameters"]["pix_size"],
                "threshold": output_relion_options["cryolo_threshold"],
            },
        },
    )
    offline_transport.send.assert_any_call(
        destination="ispyb_connector",
        message={
            "parameters": {
                "box_size_x": str(ctffind_test_message["parameters"]["ampl_spectrum"]),
                "box_size_y": str(ctffind_test_message["parameters"]["ampl_spectrum"]),
                "min_resolution": str(ctffind_test_message["parameters"]["min_res"]),
                "max_resolution": str(ctffind_test_message["parameters"]["max_res"]),
                "min_defocus": str(ctffind_test_message["parameters"]["min_defocus"]),
                "max_defocus": str(ctffind_test_message["parameters"]["max_defocus"]),
                "astigmatism": str(service.defocus2 - service.defocus1),
                "defocus_step_size": str(
                    ctffind_test_message["parameters"]["defocus_step"]
                ),
                "astigmatism_angle": str(service.astigmatism_angle),
                "estimated_resolution": str(service.estimated_resolution),
                "estimated_defocus": str((service.defocus1 + service.defocus2) / 2),
                "amplitude_contrast": str(
                    ctffind_test_message["parameters"]["ampl_contrast"]
                ),
                "cc_value": str(service.cc_value),
                "fft_theoretical_full_path": f"{tmp_path}/CtfFind/job006/sample.jpeg",
                "ispyb_command": "buffer",
                "buffer_lookup": {"motion_correction_id": 0},
                "buffer_command": {"ispyb_command": "insert_ctf"},
            },
            "content": {"dummy": "dummy"},
        },
    )
    offline_transport.send.assert_any_call(
        destination="images",
        message={
            "parameters": {"images_command": "mrc_to_jpeg"},
            "file": f"{tmp_path}/CtfFind/job006/sample.ctf",
        },
    )
    offline_transport.send.assert_any_call(
        destination="spa.node_creator",
        message={
            "parameters": {
                "job_type": "relion.ctffind.ctffind4",
                "input_file": f"{tmp_path}/MotionCorr/job002/sample.mrc",
                "output_file": f"{tmp_path}/CtfFind/job006/sample.ctf",
                "relion_options": output_relion_options,
                "command": f"ctffind\n{' '.join(map(str, parameters_list))}",
                "stdout": "stdout",
                "stderr": "stderr",
            },
            "content": "dummy",
        },
    )


def test_parse_ctffind_output(mock_environment, offline_transport):
    """
    Send test lines to the output parser
    to check the ctf values are being read in
    """
    service = ctffind.CTFFind(environment=mock_environment)
    service.transport = offline_transport
    service.start()

    ctffind.CTFFind.parse_ctf_output(service, "Estimated defocus values        : 1 , 2")
    ctffind.CTFFind.parse_ctf_output(service, "Estimated azimuth of astigmatism: 3")
    ctffind.CTFFind.parse_ctf_output(service, "Score                           : 4")
    ctffind.CTFFind.parse_ctf_output(service, "Thon rings with good fit up to  : 5")
    assert service.defocus1 == 1
    assert service.defocus2 == 2
    assert service.astigmatism_angle == 3
    assert service.cc_value == 4
    assert service.estimated_resolution == 5

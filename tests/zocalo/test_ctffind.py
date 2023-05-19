from __future__ import annotations

from unittest import mock

import pytest
import zocalo.configuration
from workflows.transport.offline_transport import OfflineTransport

from relion.zocalo import ctffind


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


@mock.patch("relion.zocalo.ctffind.procrunner.run")
def test_ctffind_service(
    mock_procrunner, mock_environment, offline_transport, tmp_path
):
    """
    Send a test message to CTFFind
    This should call the mock procrunner
    then send messages on to the ispyb_connector and images services
    """
    mock_procrunner().returncode = 0

    header = {
        "message-id": mock.sentinel,
        "subscription": mock.sentinel,
    }
    ctffind_test_message = {
        "parameters": {
            "collection_type": "spa",
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
            "output_image": f"{tmp_path}/CtfFind/job003/sample.ctf",
            "mc_uuid": 0,
            "relion_it_options": {"options": "options"},
        },
        "content": "dummy",
    }

    # set up the mock service
    service = ctffind.CTFFind(environment=mock_environment)
    service.transport = offline_transport
    service.start()

    # set some parameters then send a message to the service
    service.defocus1 = 1
    service.defocus2 = 2
    service.astigmatism_angle = 3
    service.cc_value = 4
    service.estimated_resolution = 5
    service.ctf_find(None, header=header, message=ctffind_test_message)

    parameters_list = [
        f"{tmp_path}/MotionCorr/job002/sample.mrc",
        f"{tmp_path}/CtfFind/job003/sample.ctf",
        "0.1",
        "300.0",
        "2.7",
        "0.8",
        "512",
        "30.0",
        "5.0",
        "5000.0",
        "50000.0",
        "100.0",
        "no",
        "no",
        "no",
        "no",
        "no",
    ]
    parameters_string = "\n".join(map(str, parameters_list))

    assert mock_procrunner.call_count == 2
    mock_procrunner.assert_called_with(
        command=["ctffind"],
        stdin=parameters_string.encode("ascii"),
        callback_stdout=mock.ANY,
    )

    offline_transport.send.assert_any_call(
        destination="ispyb_connector",
        message={
            "parameters": {
                "box_size_x": str(512),
                "box_size_y": str(512),
                "min_resolution": str(30.0),
                "max_resolution": str(5.0),
                "min_defocus": str(5000.0),
                "max_defocus": str(50000.0),
                "astigmatism": str(service.defocus2 - service.defocus1),
                "defocus_step_size": str(100.0),
                "astigmatism_angle": str(service.astigmatism_angle),
                "estimated_resolution": str(service.estimated_resolution),
                "estimated_defocus": str((service.defocus1 + service.defocus2) / 2),
                "amplitude_contrast": str(0.8),
                "cc_value": str(service.cc_value),
                "fft_theoretical_full_path": f"{tmp_path}/CtfFind/job003/sample.jpeg",
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
            "file": f"{tmp_path}/CtfFind/job003/sample.ctf",
        },
    )
    offline_transport.send.assert_any_call(
        destination="spa.node_creator",
        message={
            "parameters": {
                "job_type": "relion.ctffind.ctffind4",
                "input_file": f"{tmp_path}/MotionCorr/job002/sample.mrc",
                "output_file": f"{tmp_path}/CtfFind/job003/sample.ctf",
                "relion_it_options": {"options": "options"},
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

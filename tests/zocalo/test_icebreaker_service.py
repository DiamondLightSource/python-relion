from __future__ import annotations

import sys
from unittest import mock

import pytest
import zocalo.configuration
from workflows.transport.offline_transport import OfflineTransport

from relion.zocalo import icebreaker


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
@mock.patch("relion.zocalo.icebreaker.procrunner.run")
def test_icebreaker_micrographs_service(
    mock_procrunner, mock_environment, offline_transport, tmp_path
):
    """
    Send a test message to IceBreaker for running the micrographs job
    This should call the mock procrunner
    then send messages on to the node_creator and ispyb_connector services.
    It also creates the icebreaker summary jobs.
    """
    mock_procrunner().returncode = 0

    header = {
        "message-id": mock.sentinel,
        "subscription": mock.sentinel,
    }
    icebreaker_test_message = {
        "parameters": {
            "icebreaker_type": "micrographs",
            "input_micrographs": f"{tmp_path}/MotionCorr/job002/sample.mrc",
            "input_particles": None,
            "output_path": f"{tmp_path}/IceBreaker/job003/",
            "mc_uuid": 0,
            "cpus": 1,
            "relion_it_options": {"options": "options"},
            "total_motion": 0.5,
        },
        "content": "dummy",
    }

    # set up the mock service and send a message to the service
    service = icebreaker.IceBreaker(environment=mock_environment)
    service.transport = offline_transport
    service.start()
    service.icebreaker(None, header=header, message=icebreaker_test_message)

    assert mock_procrunner.call_count == 2
    mock_procrunner.assert_called_with(
        command=[
            "ib_job",
            "--j",
            str(icebreaker_test_message["parameters"]["cpus"]),
            "--mode",
            "group",
            "--single_mic",
            "MotionCorr/job002/sample.mrc",
            "--o",
            icebreaker_test_message["parameters"]["output_path"],
        ],
        callback_stdout=mock.ANY,
    )

    offline_transport.send.assert_any_call(
        destination="icebreaker",
        message={
            "parameters": {
                "icebreaker_type": "summary",
                "input_micrographs": f"{tmp_path}/IceBreaker/job003/sample_grouped.mrc",
                "mc_uuid": icebreaker_test_message["parameters"]["mc_uuid"],
                "relion_it_options": icebreaker_test_message["parameters"][
                    "relion_it_options"
                ],
                "output_path": f"{tmp_path}/IceBreaker/job005/",
            },
            "content": "dummy",
        },
    )
    offline_transport.send.assert_any_call(
        destination="spa.node_creator",
        message={
            "parameters": {
                "job_type": "icebreaker.micrograph_analysis.micrographs",
                "input_file": icebreaker_test_message["parameters"][
                    "input_micrographs"
                ],
                "output_file": icebreaker_test_message["parameters"]["output_path"],
                "relion_it_options": icebreaker_test_message["parameters"][
                    "relion_it_options"
                ],
                "results": {
                    "icebreaker_type": "micrographs",
                    "total_motion": str(
                        icebreaker_test_message["parameters"]["total_motion"]
                    ),
                },
            },
            "content": "dummy",
        },
    )


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
@mock.patch("relion.zocalo.icebreaker.procrunner.run")
def test_icebreaker_enhancecontrast_service(
    mock_procrunner, mock_environment, offline_transport, tmp_path
):
    """
    Send a test message to IceBreaker for running the enhance contrast job
    This should call the mock procrunner
    then send messages on to the node_creator and ispyb_connector services
    """
    mock_procrunner().returncode = 0

    header = {
        "message-id": mock.sentinel,
        "subscription": mock.sentinel,
    }
    icebreaker_test_message = {
        "parameters": {
            "icebreaker_type": "enhancecontrast",
            "input_micrographs": f"{tmp_path}/MotionCorr/job002/sample.mrc",
            "input_particles": None,
            "output_path": f"{tmp_path}/IceBreaker/job004/",
            "mc_uuid": 0,
            "cpus": 1,
            "relion_it_options": {"options": "options"},
            "total_motion": 0.5,
        },
        "content": "dummy",
    }

    # set up the mock service and send a message to the service
    service = icebreaker.IceBreaker(environment=mock_environment)
    service.transport = offline_transport
    service.start()
    service.icebreaker(None, header=header, message=icebreaker_test_message)

    assert mock_procrunner.call_count == 2
    mock_procrunner.assert_called_with(
        command=[
            "ib_job",
            "--j",
            str(icebreaker_test_message["parameters"]["cpus"]),
            "--mode",
            "flatten",
            "--single_mic",
            "MotionCorr/job002/sample.mrc",
            "--o",
            icebreaker_test_message["parameters"]["output_path"],
        ],
        callback_stdout=mock.ANY,
    )

    offline_transport.send.assert_any_call(
        destination="spa.node_creator",
        message={
            "parameters": {
                "job_type": "icebreaker.micrograph_analysis.enhancecontrast",
                "input_file": icebreaker_test_message["parameters"][
                    "input_micrographs"
                ],
                "output_file": icebreaker_test_message["parameters"]["output_path"],
                "relion_it_options": icebreaker_test_message["parameters"][
                    "relion_it_options"
                ],
                "results": {
                    "icebreaker_type": "enhancecontrast",
                    "total_motion": str(
                        icebreaker_test_message["parameters"]["total_motion"]
                    ),
                },
            },
            "content": "dummy",
        },
    )


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
@mock.patch("relion.zocalo.icebreaker.procrunner.run")
def test_icebreaker_summary_service(
    mock_procrunner, mock_environment, offline_transport, tmp_path
):
    """
    Send a test message to IceBreaker for running the summary job
    This should call the mock procrunner
    then send messages on to the node_creator and ispyb_connector services
    """
    mock_procrunner().returncode = 0

    header = {
        "message-id": mock.sentinel,
        "subscription": mock.sentinel,
    }
    icebreaker_test_message = {
        "parameters": {
            "icebreaker_type": "summary",
            "input_micrographs": f"{tmp_path}/IceBreaker/job003/sample_grouped.star",
            "input_particles": None,
            "output_path": f"{tmp_path}/IceBreaker/job005/",
            "mc_uuid": 0,
            "cpus": 1,
            "relion_it_options": {"options": "options"},
            "total_motion": 0.5,
        },
        "content": "dummy",
    }

    # set up the mock service and send a message to the service
    service = icebreaker.IceBreaker(environment=mock_environment)
    service.transport = offline_transport
    service.start()
    service.icebreaker(None, header=header, message=icebreaker_test_message)

    assert mock_procrunner.call_count == 2
    mock_procrunner.assert_called_with(
        command=[
            "ib_5fig",
            "--single_mic",
            "IceBreaker/job003/sample_grouped.star",
            "--o",
            icebreaker_test_message["parameters"]["output_path"],
        ],
        callback_stdout=mock.ANY,
    )

    offline_transport.send.assert_any_call(
        destination="spa.node_creator",
        message={
            "parameters": {
                "job_type": "icebreaker.micrograph_analysis.summary",
                "input_file": icebreaker_test_message["parameters"][
                    "input_micrographs"
                ],
                "output_file": icebreaker_test_message["parameters"]["output_path"],
                "relion_it_options": icebreaker_test_message["parameters"][
                    "relion_it_options"
                ],
                "results": {
                    "icebreaker_type": "summary",
                    "total_motion": str(
                        icebreaker_test_message["parameters"]["total_motion"]
                    ),
                },
            },
            "content": "dummy",
        },
    )

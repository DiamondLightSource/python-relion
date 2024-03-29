from __future__ import annotations

import sys
from unittest import mock

import pytest
import zocalo.configuration
from workflows.transport.offline_transport import OfflineTransport

from relion.zocalo import icebreaker
from relion.zocalo.spa_relion_service_options import RelionServiceOptions

output_relion_options = dict(RelionServiceOptions())


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
@mock.patch("relion.zocalo.icebreaker.subprocess.run")
def test_icebreaker_micrographs_service(
    mock_subprocess, mock_environment, offline_transport, tmp_path
):
    """
    Send a test message to IceBreaker for running the micrographs job
    This should call the mock subprocess
    then send a message on to the node_creator service.
    It also creates the icebreaker summary jobs.
    """
    mock_subprocess().returncode = 0
    mock_subprocess().stdout = "stdout".encode("ascii")
    mock_subprocess().stderr = "stderr".encode("ascii")

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
            "cpus": 1,
            "mc_uuid": 0,
            "relion_options": {"do_icebreaker_jobs": "True"},
            "total_motion": 0.5,
            "early_motion": 0.2,
            "late_motion": 0.3,
        },
        "content": "dummy",
    }

    # Set up the mock service and send a message to the service
    service = icebreaker.IceBreaker(environment=mock_environment)
    service.transport = offline_transport
    service.start()
    service.icebreaker(None, header=header, message=icebreaker_test_message)

    assert mock_subprocess.call_count == 4
    mock_subprocess.assert_called_with(
        [
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
        capture_output=True,
    )

    # Check that the correct messages were sent
    offline_transport.send.assert_any_call(
        destination="icebreaker",
        message={
            "parameters": {
                "icebreaker_type": "summary",
                "input_micrographs": f"{tmp_path}/IceBreaker/job003/sample_grouped.mrc",
                "mc_uuid": 0,
                "relion_options": output_relion_options,
                "output_path": f"{tmp_path}/IceBreaker/job005/",
            },
            "content": "dummy",
        },
    )
    offline_transport.send.assert_any_call(
        destination="node_creator",
        message={
            "parameters": {
                "job_type": "icebreaker.micrograph_analysis.micrographs",
                "input_file": icebreaker_test_message["parameters"][
                    "input_micrographs"
                ],
                "output_file": icebreaker_test_message["parameters"]["output_path"],
                "relion_options": output_relion_options,
                "command": (
                    "ib_job --j 1 --mode group --single_mic "
                    f"MotionCorr/job002/sample.mrc --o {tmp_path}/IceBreaker/job003/"
                ),
                "stdout": "stdout",
                "stderr": "stderr",
                "results": {
                    "icebreaker_type": "micrographs",
                    "total_motion": 0.5,
                    "early_motion": 0.2,
                    "late_motion": 0.3,
                },
                "success": True,
            },
            "content": "dummy",
        },
    )


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
@mock.patch("relion.zocalo.icebreaker.subprocess.run")
def test_icebreaker_enhancecontrast_service(
    mock_subprocess, mock_environment, offline_transport, tmp_path
):
    """
    Send a test message to IceBreaker for running the enhance contrast job
    This should call the mock subprocess
    then send a message on to the node_creator service
    """
    mock_subprocess().returncode = 0
    mock_subprocess().stdout = "stdout".encode("ascii")
    mock_subprocess().stderr = "stderr".encode("ascii")

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
            "cpus": 1,
            "mc_uuid": 0,
            "relion_options": {"options": "options"},
            "total_motion": 0.5,
            "early_motion": 0.2,
            "late_motion": 0.3,
        },
        "content": "dummy",
    }

    # Set up the mock service and send a message to the service
    service = icebreaker.IceBreaker(environment=mock_environment)
    service.transport = offline_transport
    service.start()
    service.icebreaker(None, header=header, message=icebreaker_test_message)

    assert mock_subprocess.call_count == 4
    mock_subprocess.assert_called_with(
        [
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
        capture_output=True,
    )

    # Check that the correct messages were sent
    offline_transport.send.assert_any_call(
        destination="node_creator",
        message={
            "parameters": {
                "job_type": "icebreaker.micrograph_analysis.enhancecontrast",
                "input_file": icebreaker_test_message["parameters"][
                    "input_micrographs"
                ],
                "output_file": icebreaker_test_message["parameters"]["output_path"],
                "relion_options": output_relion_options,
                "command": (
                    "ib_job --j 1 --mode flatten --single_mic "
                    f"MotionCorr/job002/sample.mrc --o {tmp_path}/IceBreaker/job004/"
                ),
                "stdout": "stdout",
                "stderr": "stderr",
                "results": {
                    "icebreaker_type": "enhancecontrast",
                    "total_motion": 0.5,
                    "early_motion": 0.2,
                    "late_motion": 0.3,
                },
                "success": True,
            },
            "content": "dummy",
        },
    )


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
@mock.patch("relion.zocalo.icebreaker.subprocess.run")
def test_icebreaker_summary_service(
    mock_subprocess, mock_environment, offline_transport, tmp_path
):
    """
    Send a test message to IceBreaker for running the summary job
    This should call the mock subprocess
    then send a message on to the node_creator service
    """
    mock_subprocess().returncode = 0
    mock_subprocess().stdout = "stdout".encode("ascii")
    mock_subprocess().stderr = "stderr".encode("ascii")

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
            "cpus": 1,
            "mc_uuid": 0,
            "relion_options": {"options": "options"},
            "total_motion": 0.5,
            "early_motion": 0.2,
            "late_motion": 0.3,
        },
        "content": "dummy",
    }

    # Set up the mock service and send a message to the service
    service = icebreaker.IceBreaker(environment=mock_environment)
    service.ice_minimum = 0
    service.ice_q1 = 1
    service.ice_median = 2
    service.ice_q3 = 3
    service.ice_maximum = 4
    service.transport = offline_transport
    service.start()
    service.icebreaker(None, header=header, message=icebreaker_test_message)

    assert mock_subprocess.call_count == 4
    mock_subprocess.assert_called_with(
        [
            "ib_5fig",
            "--single_mic",
            "IceBreaker/job003/sample_grouped.star",
            "--o",
            icebreaker_test_message["parameters"]["output_path"],
        ],
        capture_output=True,
    )

    # Check that the correct messages were sent
    offline_transport.send.assert_any_call(
        destination="node_creator",
        message={
            "parameters": {
                "job_type": "icebreaker.micrograph_analysis.summary",
                "input_file": icebreaker_test_message["parameters"][
                    "input_micrographs"
                ],
                "output_file": icebreaker_test_message["parameters"]["output_path"],
                "relion_options": output_relion_options,
                "command": (
                    "ib_5fig --single_mic IceBreaker/job003/sample_grouped.star "
                    f"--o {tmp_path}/IceBreaker/job005/"
                ),
                "stdout": "stdout",
                "stderr": "stderr",
                "results": {
                    "icebreaker_type": "summary",
                    "total_motion": 0.5,
                    "early_motion": 0.2,
                    "late_motion": 0.3,
                    "summary": ["0", "1", "2", "3", "4"],
                },
                "success": True,
            },
            "content": "dummy",
        },
    )
    offline_transport.send.assert_any_call(
        destination="ispyb_connector",
        message={
            "parameters": {
                "minimum": 0,
                "q1": 1,
                "median": 2,
                "q3": 3,
                "maximum": 4,
                "ispyb_command": "buffer",
                "buffer_lookup": {"motion_correction_id": 0},
                "buffer_command": {"ispyb_command": "insert_relative_ice_thickness"},
            },
            "content": "dummy",
        },
    )


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
@mock.patch("relion.zocalo.icebreaker.subprocess.run")
def test_icebreaker_particles_service(
    mock_subprocess, mock_environment, offline_transport, tmp_path
):
    """
    Send a test message to IceBreaker for running the particle analysis job
    This should call the mock subprocess
    then send a message on to the node_creator service
    """
    mock_subprocess().returncode = 0
    mock_subprocess().stdout = "stdout".encode("ascii")
    mock_subprocess().stderr = "stderr".encode("ascii")

    header = {
        "message-id": mock.sentinel,
        "subscription": mock.sentinel,
    }
    icebreaker_test_message = {
        "parameters": {
            "icebreaker_type": "particles",
            "input_micrographs": f"{tmp_path}/IceBreaker/job003/sample_grouped.star",
            "input_particles": f"{tmp_path}/Select/job009/particles_split1.star",
            "output_path": f"{tmp_path}/IceBreaker/job010/",
            "cpus": 1,
            "mc_uuid": 0,
            "relion_options": {"options": "options"},
            "total_motion": 0.5,
            "early_motion": 0.2,
            "late_motion": 0.3,
        },
        "content": "dummy",
    }

    # Set up the mock service and send a message to the service
    service = icebreaker.IceBreaker(environment=mock_environment)
    service.transport = offline_transport
    service.start()
    service.icebreaker(None, header=header, message=icebreaker_test_message)

    assert mock_subprocess.call_count == 4
    mock_subprocess.assert_called_with(
        [
            "ib_group",
            "--in_mics",
            "IceBreaker/job003/sample_grouped.star",
            "--in_parts",
            "Select/job009/particles_split1.star",
            "--o",
            icebreaker_test_message["parameters"]["output_path"],
        ],
        capture_output=True,
    )

    # Check that the correct messages were sent
    offline_transport.send.assert_any_call(
        destination="node_creator",
        message={
            "parameters": {
                "job_type": "icebreaker.micrograph_analysis.particles",
                "input_file": icebreaker_test_message["parameters"]["input_micrographs"]
                + ":"
                + icebreaker_test_message["parameters"]["input_particles"],
                "output_file": icebreaker_test_message["parameters"]["output_path"],
                "relion_options": output_relion_options,
                "command": (
                    "ib_group --in_mics IceBreaker/job003/sample_grouped.star "
                    "--in_parts Select/job009/particles_split1.star "
                    f"--o {tmp_path}/IceBreaker/job010/"
                ),
                "stdout": "stdout",
                "stderr": "stderr",
                "results": {
                    "icebreaker_type": "particles",
                    "total_motion": 0.5,
                    "early_motion": 0.2,
                    "late_motion": 0.3,
                },
                "success": True,
            },
            "content": "dummy",
        },
    )


def test_parse_icebreaker_output(mock_environment, offline_transport):
    """
    Send test lines to the output parser
    to check the thicknesses are being read in
    """
    service = icebreaker.IceBreaker(environment=mock_environment)
    service.transport = offline_transport
    service.start()

    icebreaker.IceBreaker.parse_icebreaker_output(service, "Results: path 0 1 2 3 4")
    assert service.ice_minimum == 0
    assert service.ice_q1 == 1
    assert service.ice_median == 2
    assert service.ice_q3 == 3
    assert service.ice_maximum == 4

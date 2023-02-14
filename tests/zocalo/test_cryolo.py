from __future__ import annotations

import os
from unittest import mock

import pytest
import zocalo.configuration
from workflows.transport.offline_transport import OfflineTransport

from relion.zocalo import cryolo


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


@mock.patch("relion.zocalo.cryolo.procrunner.run")
def test_cryolo_service(mock_procrunner, mock_environment, offline_transport, tmp_path):
    """
    Send a test message to CrYOLO
    This should call the mock procrunner
    then send messages on to the ispyb_connector and images services
    """
    mock_procrunner().returncode = 0

    header = {
        "message-id": mock.sentinel,
        "subscription": mock.sentinel,
    }

    cryolo_test_message = {
        "parameters": {
            "boxsize": 256,
            "pix_size": 0.1,
            "input_path": "sample.mrc",
            "output_path": str(tmp_path),
            "weights": "sample_weights",
            "mc_uuid": 0,
            "cryolo_command": "cryolo_predict.py",
        },
        "content": "dummy",
    }

    os.mkdir(tmp_path / "STAR")
    with open(tmp_path / "STAR/sample.star", "w") as f:
        f.write("data_\n\nloop_\n\n_rlnCoordinateX\n_rlnCoordinateY\n 0.1, 0.2")

    service = cryolo.CrYOLO(environment=mock_environment)
    service.transport = offline_transport
    service.start()
    service.cryolo(None, header=header, message=cryolo_test_message)

    assert mock_procrunner.call_count == 2
    mock_procrunner.assert_called_with(
        command=[
            "cryolo_predict.py",
            "--conf",
            "config.json",
            "-i",
            "sample.mrc",
            "-o",
            str(tmp_path),
            "--weights",
            "sample_weights",
            "--threshold",
            "0.3",
        ],
        callback_stdout=mock.ANY,
    )

    offline_transport.send.assert_any_call(
        destination="ispyb_connector",
        message={
            "parameters": {
                "particle_picking_template": "sample_weights",
                "particle_diameter": 2.56,
                "number_of_particles": 0,
                "summary_image_full_path": str(tmp_path) + "/picked_particles.mrc",
                "ispyb_command": "buffer",
                "buffer_lookup": {"motion_correction_id": 0},
                "buffer_command": {"ispyb_command": "insert_particle_picker"},
            },
            "content": {"dummy": "dummy"},
        },
    )
    offline_transport.send.assert_any_call(
        destination="images",
        message={
            "parameters": {"images_command": "picked_particles"},
            "file": "sample.mrc",
            "coordinates": [["0.1,", "0.2"]],
            "angpix": 0.1,
            "diameter": 25.6,
            "outfile": str(tmp_path) + "/picked_particles.jpeg",
        },
    )


def test_parse_cryolo_output(mock_environment, offline_transport):
    """
    Send test lines to the output parser
    to check the number of particles is being read in
    """
    service = cryolo.CrYOLO(environment=mock_environment)
    service.transport = offline_transport
    service.start()

    service.number_of_particles = 0

    cryolo.CrYOLO.parse_cryolo_output(service, "30 particles in total has been found")
    cryolo.CrYOLO.parse_cryolo_output(service, "Deleted 10 particles")

    assert service.number_of_particles == 20

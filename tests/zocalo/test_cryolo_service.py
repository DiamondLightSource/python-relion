from __future__ import annotations

import sys
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


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
@mock.patch("relion.zocalo.cryolo.procrunner.run")
def test_cryolo_service(mock_procrunner, mock_environment, offline_transport, tmp_path):
    """
    Send a test message to CrYOLO
    This should call the mock procrunner then send messages on to the
    node_creator, murfey_feedback, ispyb_connector and images services
    """
    mock_procrunner().returncode = 0

    header = {
        "message-id": mock.sentinel,
        "subscription": mock.sentinel,
    }

    output_path = tmp_path / "AutoPick/job001/STAR/sample.star"
    cryolo_test_message = {
        "parameters": {
            "boxsize": 256,
            "pix_size": 0.1,
            "input_path": "sample.mrc",
            "output_path": str(output_path),
            "config_file": str(tmp_path) + "/config.json",
            "weights": "sample_weights",
            "threshold": 0.3,
            "mc_uuid": 0,
            "cryolo_command": "cryolo_predict.py",
            "relion_it_options": {"options": "options"},
        },
        "content": "dummy",
    }

    # write a dummy config file expected by cryolo
    with open(tmp_path / "config.json", "w") as f:
        f.write('{\n"model": {\n"anchors": [160, 160]\n}\n}')

    # write star co-ordinate file in the format cryolo will output
    output_path.parent.mkdir(parents=True)
    with open(output_path, "w") as f:
        f.write("data_\n\nloop_\n\n_rlnCoordinateX\n_rlnCoordinateY\n 0.1, 0.2")

    # make the cryolo temporary dirs
    (tmp_path / "logs").mkdir()
    (tmp_path / "filtered").mkdir()

    # set up the mock service and send the message to it
    service = cryolo.CrYOLO(environment=mock_environment)
    service.transport = offline_transport
    service.start()
    service.cryolo(None, header=header, message=cryolo_test_message)

    assert mock_procrunner.call_count == 2
    mock_procrunner.assert_called_with(
        command=[
            "cryolo_predict.py",
            "--conf",
            str(tmp_path / "config.json"),
            "-o",
            str(output_path.parent.parent),
            "-i",
            "sample.mrc",
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
                "number_of_particles": 0,
                "summary_image_full_path": str(output_path) + "/picked_particles.mrc",
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
            "diameter": 16.0,
            "outfile": str(output_path) + "/picked_particles.jpeg",
        },
    )
    offline_transport.send.assert_any_call(
        destination="murfey_feedback",
        message={
            "register": "picked_particles",
            "motion_correction_id": 0,
            "number_of_particles": 0,
            "output_image": str(output_path),
        },
    )
    offline_transport.send.assert_any_call(
        destination="spa.node_creator",
        message={
            "parameters": {
                "job_type": "cryolo.autopick",
                "input_file": "sample.mrc",
                "output_file": str(output_path),
                "relion_it_options": {"options": "options"},
            },
            "content": "dummy",
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

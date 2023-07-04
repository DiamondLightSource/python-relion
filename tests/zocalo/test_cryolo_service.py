from __future__ import annotations

import sys
from unittest import mock

import pytest
import zocalo.configuration
from workflows.transport.offline_transport import OfflineTransport

from relion.zocalo import cryolo
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
@mock.patch("relion.zocalo.cryolo.subprocess.run")
def test_cryolo_service(mock_subprocess, mock_environment, offline_transport, tmp_path):
    """
    Send a test message to CrYOLO
    This should call the mock subprocess then send messages on to the
    node_creator, murfey_feedback, ispyb_connector and images services
    """
    mock_subprocess().returncode = 0
    mock_subprocess().stdout = "stdout".encode("ascii")
    mock_subprocess().stderr = "stderr".encode("ascii")

    header = {
        "message-id": mock.sentinel,
        "subscription": mock.sentinel,
    }

    output_path = tmp_path / "AutoPick/job007/STAR/sample.star"
    cbox_path = tmp_path / "AutoPick/job007/CBOX/sample.cbox"
    cryolo_test_message = {
        "parameters": {
            "boxsize": 256,
            "pix_size": 0.1,
            "input_path": "MotionCorr/job002/sample.mrc",
            "output_path": str(output_path),
            "config_file": str(tmp_path) + "/config.json",
            "weights": "sample_weights",
            "threshold": 0.3,
            "mc_uuid": 0,
            "ctf_values": {"dummy": "dummy"},
            "cryolo_command": "cryolo_predict.py",
            "relion_options": {"batch_size": 20000, "downscale": True},
        },
        "content": "dummy",
    }
    output_relion_options = dict(RelionServiceOptions())
    output_relion_options.update(cryolo_test_message["parameters"]["relion_options"])

    # Write a dummy config file expected by cryolo
    with open(tmp_path / "config.json", "w") as f:
        f.write('{\n"model": {\n"anchors": [160, 160]\n}\n}')

    # Write star co-ordinate file in the format cryolo will output
    output_path.parent.mkdir(parents=True)
    with open(output_path, "w") as f:
        f.write("data_\n\nloop_\n\n_rlnCoordinateX\n_rlnCoordinateY\n 0.1 0.2")
    cbox_path.parent.mkdir(parents=True)
    with open(cbox_path, "w") as f:
        f.write(
            "data_cryolo\n\nloop_\n\n_EstWidth\n_EstHeight\n_Confidence\n"
            "100 200 0.5\n100 200 0.5"
        )

    # Make the cryolo temporary dirs
    (tmp_path / "logs").mkdir()
    (tmp_path / "filtered").mkdir()

    # Set up the mock service and send the message to it
    service = cryolo.CrYOLO(environment=mock_environment)
    service.transport = offline_transport
    service.start()
    service.cryolo(None, header=header, message=cryolo_test_message)

    assert mock_subprocess.call_count == 4
    mock_subprocess.assert_called_with(
        [
            "cryolo_predict.py",
            "--conf",
            str(tmp_path / "config.json"),
            "-o",
            str(output_path.parent.parent),
            "-i",
            "MotionCorr/job002/sample.mrc",
            "--weights",
            "sample_weights",
            "--threshold",
            "0.3",
        ],
        cwd=tmp_path,
        capture_output=True,
    )

    # Check that the correct messages were sent
    extraction_params = {
        "ctf_values": cryolo_test_message["parameters"]["ctf_values"],
        "micrographs_file": cryolo_test_message["parameters"]["input_path"],
        "coord_list_file": cryolo_test_message["parameters"]["output_path"],
        "extract_file": "Extract/job008/Movies/sample_extract.star",
    }
    offline_transport.send.assert_any_call(
        destination="ispyb_connector",
        message={
            "parameters": {
                "particle_picking_template": "sample_weights",
                "number_of_particles": 0,
                "summary_image_full_path": str(output_path) + "/picked_particles.jpeg",
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
            "file": cryolo_test_message["parameters"]["input_path"],
            "coordinates": [["0.1", "0.2"]],
            "angpix": 0.1,
            "diameter": 16.0,
            "outfile": str(output_path) + "/picked_particles.jpeg",
        },
    )
    offline_transport.send.assert_any_call(
        destination="murfey_feedback",
        message={
            "register": "picked_particles",
            "motion_correction_id": cryolo_test_message["parameters"]["mc_uuid"],
            "micrograph": cryolo_test_message["parameters"]["input_path"],
            "particle_diameters": [100.0, 100.0, 200.0, 200.0],
            "extraction_parameters": extraction_params,
        },
    )
    offline_transport.send.assert_any_call(
        destination="spa.node_creator",
        message={
            "parameters": {
                "job_type": "cryolo.autopick",
                "input_file": cryolo_test_message["parameters"]["input_path"],
                "output_file": str(output_path),
                "relion_options": output_relion_options,
                "command": (
                    f"cryolo_predict.py --conf {tmp_path}/config.json "
                    f"-o {tmp_path}/AutoPick/job007 "
                    f"-i MotionCorr/job002/sample.mrc "
                    f"--weights sample_weights --threshold 0.3"
                ),
                "stdout": "stdout",
                "stderr": "stderr",
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

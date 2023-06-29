from __future__ import annotations

import sys
from unittest import mock

import pytest
import zocalo.configuration
from gemmi import cif
from workflows.transport.offline_transport import OfflineTransport

from relion.zocalo import select_particles
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
def test_select_particles_service(mock_environment, offline_transport, tmp_path):
    """
    Send a test message to the select particles service
    This should call the mock file reader then send a message on to
    the node_creator service and murfey
    """
    header = {
        "message-id": mock.sentinel,
        "subscription": mock.sentinel,
    }

    extract_file = tmp_path / "Extract/job008/Movies/sample.star"
    extract_file.parent.mkdir(parents=True)
    with open(extract_file, "w") as f:
        f.write(
            "data_particles\n\nloop_\n_rlnCoordinateX\n_rlnCoordinateY\n_rlnImageName"
            "\n_rlnMicrographName\n_rlnOpticsGroup\n_rlnCtfMaxResolution"
            "\n_rlnCtfFigureOfMerit\n_rlnDefocusU\n_rlnDefocusV\n_rlnDefocusAngle"
            "\n_rlnCtfBfactor\n_rlnCtfScalefactor\n_rlnPhaseShift"
        )
        for i in range(5):
            f.write(
                "\n1.0 2.0 0@Extract.mrcs sample.mrc 1 10 20 1.0 2.0 0.0 0.0 1.0 0.0"
            )
    output_dir = tmp_path / "Select/job009/"

    input_relion_options = {
        "pixel_size_on_image": 1.0,
        "voltage": 200,
        "spher_aber": 2.7,
        "ampl_contrast": 0.1,
        "do_icebreaker_jobs": True,
    }
    output_relion_options = dict(RelionServiceOptions())
    output_relion_options.update(input_relion_options)

    select_test_message = {
        "parameters": {
            "input_file": str(extract_file),
            "batch_size": 2,
            "image_size": 64,
            "relion_options": input_relion_options,
        },
        "content": "dummy",
    }

    # Set up the mock service and send the message to it
    service = select_particles.SelectParticles(environment=mock_environment)
    service.transport = offline_transport
    service.start()
    service.select_particles(None, header=header, message=select_test_message)

    # Check that the correct messages were sent
    offline_transport.send.assert_any_call(
        destination="spa.node_creator",
        message={
            "parameters": {
                "job_type": "relion.select.split",
                "input_file": str(extract_file),
                "output_file": f"{output_dir}/particles_split3.star",
                "relion_options": output_relion_options,
                "command": "",
                "stdout": "",
                "stderr": "",
            },
            "content": "dummy",
        },
    )
    offline_transport.send.assert_any_call(
        destination="murfey_feedback",
        message={
            "register": "complete_particles_file",
            "class2d_message": {
                "class2d_dir": f"{tmp_path}/Class2D/job",
                "particle_diameter": 64,
                "batch_size": 2,
                "relion_options": output_relion_options,
                "particles_file": f"{tmp_path}/Select/job009/particles_split2.star",
                "batch_is_complete": "True",
            },
        },
    )

    # Check the output files and their structure
    assert (output_dir / "particles_split1.star").exists()
    assert (output_dir / "particles_split2.star").exists()
    assert (output_dir / "particles_split3.star").exists()

    particles_file = cif.read_file(f"{output_dir}/particles_split3.star")
    particles_data = particles_file.find_block("particles")
    assert list(particles_data.find_loop("_rlnCoordinateX")) == ["1.0"]
    assert list(particles_data.find_loop("_rlnCoordinateY")) == ["2.0"]

    micrographs_optics = particles_file.find_block("optics")
    assert list(micrographs_optics.find_loop("_rlnOpticsGroupName")) == ["opticsGroup1"]
    assert list(micrographs_optics.find_loop("_rlnOpticsGroup")) == ["1"]
    assert list(micrographs_optics.find_loop("_rlnMicrographOriginalPixelSize")) == [
        str(input_relion_options["pixel_size_on_image"])
    ]
    assert list(micrographs_optics.find_loop("_rlnVoltage")) == [
        str(input_relion_options["voltage"])
    ]
    assert list(micrographs_optics.find_loop("_rlnSphericalAberration")) == [
        str(input_relion_options["spher_aber"])
    ]
    assert list(micrographs_optics.find_loop("_rlnAmplitudeContrast")) == [
        str(input_relion_options["ampl_contrast"])
    ]
    assert list(micrographs_optics.find_loop("_rlnImagePixelSize")) == [
        str(input_relion_options["pixel_size_on_image"])
    ]
    assert list(micrographs_optics.find_loop("_rlnImageSize")) == ["64"]
    assert list(micrographs_optics.find_loop("_rlnImageDimensionality")) == ["2"]
    assert list(micrographs_optics.find_loop("_rlnCtfDataAreCtfPremultiplied")) == ["0"]

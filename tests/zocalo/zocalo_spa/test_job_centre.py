from __future__ import annotations

from unittest import mock

import pytest
import zocalo.configuration
from workflows.transport.offline_transport import OfflineTransport

from relion.cryolo_relion_it.cryolo_relion_it import RelionItOptions
from relion.zocalo.zocalo_spa import job_centre

relion_it_options = dict(RelionItOptions())


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


def test_job_centre_service_import(mock_environment, offline_transport, tmp_path):
    """
    Send a test import message to the job runner
    This should send messages on to the node creator and motioncorr services
    """
    header = {
        "message-id": mock.sentinel,
        "subscription": mock.sentinel,
    }

    import_test_message = {
        "parameters": {
            "processing_dir": str(tmp_path),
            "file": "test_movie.mrc",
            "job_type": "relion.import.movies",
            "options": relion_it_options,
            "job_number": 1,
        },
        "content": "dummy",
    }

    # set up the mock service and send the message to it
    service = job_centre.JobRunner(environment=mock_environment)
    service.transport = offline_transport
    service.start()
    service.spa_job_centre(None, header=header, message=import_test_message)

    offline_transport.send.assert_any_call(
        destination="spa.node_creator",
        message={
            "parameters": {
                "processing_dir": str(tmp_path),
                "file": "test_movie.mrc",
                "job_type": "relion.import.movies",
                "options": mock.ANY,
                "job_paths": {"relion.import.movies": "Import/job001"},
                "job_number": 1,
                "job_status": "Success",
                "job_dir": "Import/job001",
                "job_params": mock.ANY,
            },
            "content": "dummy",
        },
    )
    offline_transport.send.assert_any_call(
        destination="spa.job_centre",
        message={
            "parameters": {
                "processing_dir": str(tmp_path),
                "file": "test_movie.mrc",
                "job_type": "relion.motioncorr.motioncor2",
                "options": mock.ANY,
                "job_paths": {"relion.import.movies": "Import/job001"},
                "job_number": 2,
                "job_status": "Running",
                "job_dir": "Import/job001",
                "job_params": mock.ANY,
            },
            "content": "dummy",
        },
    )

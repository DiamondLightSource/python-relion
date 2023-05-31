from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional
from unittest import mock

import pytest
import zocalo.configuration
from gemmi import cif
from workflows.transport.offline_transport import OfflineTransport

from relion.cryolo_relion_it.cryolo_relion_it import RelionItOptions
from relion.zocalo import node_creator

relion_it_options = RelionItOptions()


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


def setup_and_run_node_creation(
    environment: dict,
    transport: OfflineTransport,
    project_dir: Path,
    job_dir: str,
    job_type: str,
    input_file: Path,
    output_file: Path,
    results: Optional[dict] = None,
):
    """
    Run the node creation for any job and check the pipeline files are produced
    """
    header = {
        "message-id": mock.sentinel,
        "subscription": mock.sentinel,
    }

    # Write a dummy pipeline file expected by cryolo
    with open(project_dir / "default_pipeline.star", "w") as f:
        f.write("data_pipeline_general\n\n_rlnPipeLineJobCounter  1")

    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.touch()
    test_message = {
        "parameters": {
            "job_type": job_type,
            "input_file": str(input_file),
            "output_file": str(output_file),
            "relion_it_options": relion_it_options,
            "results": results,
        },
        "content": "dummy",
    }

    # set up the mock service and send the message to it
    service = node_creator.NodeCreator(environment=environment)
    service.transport = transport
    service.start()
    service.spa_node_creator(None, header=header, message=test_message)

    # Check that the correct general pipeline files have been made
    assert (project_dir / f"{job_type.replace('.', '_')}_job.star").exists()
    assert (project_dir / f".gui_{job_type.replace('.', '_')}job.star").exists()
    assert (project_dir / ".Nodes").is_dir()

    assert (project_dir / job_dir / "job.star").exists()
    assert (project_dir / job_dir / "run.out").exists()
    assert (project_dir / job_dir / "run.err").exists()
    assert (project_dir / job_dir / "run.job").exists()
    assert (project_dir / job_dir / "continue_job.star").exists()
    assert (project_dir / job_dir / "PIPELINER_JOB_EXIT_SUCCESS").exists()
    assert (project_dir / job_dir / "job_metadata.json").exists()
    assert (project_dir / job_dir / "default_pipeline.star").exists()
    assert (project_dir / job_dir / ".CCPEM_pipeliner_jobinfo").exists()


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_node_creator_import(mock_environment, offline_transport, tmp_path):
    """
    Send a test message to the node creator for
    relion.import.movies
    """
    job_dir = "Import/job001"
    input_file = tmp_path / "Movies/sample.mrc"
    output_file = tmp_path / job_dir / "Movies/sample.mrc"

    setup_and_run_node_creation(
        mock_environment,
        offline_transport,
        tmp_path,
        job_dir,
        "relion.import.movies",
        input_file,
        output_file,
    )

    # Check the output file structure
    assert (tmp_path / job_dir / "movies.star").exists()
    micrographs_file = cif.read_file(str(tmp_path / job_dir / "movies.star"))

    micrographs_optics = micrographs_file.find_block("optics")
    assert list(micrographs_optics.find_loop("_rlnOpticsGroupName")) == ["opticsGroup1"]
    assert list(micrographs_optics.find_loop("_rlnOpticsGroup")) == ["1"]
    assert list(micrographs_optics.find_loop("_rlnMicrographOriginalPixelSize")) == [
        str(relion_it_options.angpix)
    ]
    assert list(micrographs_optics.find_loop("_rlnVoltage")) == [
        str(relion_it_options.voltage)
    ]
    assert list(micrographs_optics.find_loop("_rlnSphericalAberration")) == [
        str(relion_it_options.Cs)
    ]
    assert list(micrographs_optics.find_loop("_rlnAmplitudeContrast")) == [
        str(relion_it_options.ampl_contrast)
    ]
    assert list(micrographs_optics.find_loop("_rlnMicrographPixelSize")) == [
        str(relion_it_options.angpix)
    ]

    micrographs_data = micrographs_file.find_block("movies")
    assert list(micrographs_data.find_loop("_rlnMicrographMovieName")) == [
        "Import/job001/Movies/sample.mrc"
    ]
    assert list(micrographs_data.find_loop("_rlnOpticsGroup")) == ["1"]


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_node_creator_motioncorr(mock_environment, offline_transport, tmp_path):
    """
    Send a test message to the node creator for
    relion.motioncorr.motioncor2
    """
    job_dir = "MotionCorr/job002"
    input_file = tmp_path / "Import/job001/Movies/sample.mrc"
    output_file = tmp_path / job_dir / "Movies/sample.mrc"

    setup_and_run_node_creation(
        mock_environment,
        offline_transport,
        tmp_path,
        job_dir,
        "relion.motioncorr.motioncor2",
        input_file,
        output_file,
        results={"total_motion": "10"},
    )

    # Check the output file structure
    assert (tmp_path / job_dir / "corrected_micrographs.star").exists()
    micrographs_file = cif.read_file(
        str(tmp_path / job_dir / "corrected_micrographs.star")
    )

    micrographs_optics = micrographs_file.find_block("optics")
    assert list(micrographs_optics.find_loop("_rlnOpticsGroupName")) == ["opticsGroup1"]
    assert list(micrographs_optics.find_loop("_rlnOpticsGroup")) == ["1"]
    assert list(micrographs_optics.find_loop("_rlnMicrographOriginalPixelSize")) == [
        str(relion_it_options.angpix)
    ]
    assert list(micrographs_optics.find_loop("_rlnVoltage")) == [
        str(relion_it_options.voltage)
    ]
    assert list(micrographs_optics.find_loop("_rlnSphericalAberration")) == [
        str(relion_it_options.Cs)
    ]
    assert list(micrographs_optics.find_loop("_rlnAmplitudeContrast")) == [
        str(relion_it_options.ampl_contrast)
    ]
    assert list(micrographs_optics.find_loop("_rlnMicrographPixelSize")) == [
        str(relion_it_options.angpix)
    ]

    micrographs_data = micrographs_file.find_block("micrographs")
    assert list(micrographs_data.find_loop("_rlnMicrographName")) == [
        "MotionCorr/job002/Movies/sample.mrc"
    ]
    assert list(micrographs_data.find_loop("_rlnMicrographMetadata")) == [
        "MotionCorr/job002/Movies/sample.star"
    ]
    assert list(micrographs_data.find_loop("_rlnOpticsGroup")) == ["1"]
    assert list(micrographs_data.find_loop("_rlnAccumMotionTotal")) == ["10"]
    assert list(micrographs_data.find_loop("_rlnAccumMotionEarly")) == ["0.0"]
    assert list(micrographs_data.find_loop("_rlnAccumMotionLate")) == ["10"]


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_node_creator_icebreaker_micrographs(
    mock_environment, offline_transport, tmp_path
):
    """
    Send a test message to the node creator for
    icebreaker.micrograph_analysis.micrographs
    """
    job_dir = "IceBreaker/job003"
    input_file = tmp_path / "MotionCorr/job002/Movies/sample.mrc"
    output_file = tmp_path / job_dir
    output_file.mkdir(parents=True)

    setup_and_run_node_creation(
        mock_environment,
        offline_transport,
        tmp_path,
        job_dir,
        "icebreaker.micrograph_analysis.micrographs",
        input_file,
        output_file,
        results={"icebreaker_type": "micrographs", "total_motion": "10"},
    )

    # Check the output file structure
    assert (tmp_path / job_dir / "grouped_micrographs.star").exists()
    micrographs_file = cif.read_file(
        str(tmp_path / job_dir / "grouped_micrographs.star")
    )

    micrographs_data = micrographs_file.find_block("micrographs")
    assert list(micrographs_data.find_loop("_rlnMicrographName")) == [
        "IceBreaker/job003/Movies/sample_grouped.mrc"
    ]
    assert list(micrographs_data.find_loop("_rlnMicrographMetadata")) == [
        "MotionCorr/job002/Movies/sample.star"
    ]
    assert list(micrographs_data.find_loop("_rlnOpticsGroup")) == ["1"]
    assert list(micrographs_data.find_loop("_rlnAccumMotionTotal")) == ["10"]
    assert list(micrographs_data.find_loop("_rlnAccumMotionEarly")) == ["0.0"]
    assert list(micrographs_data.find_loop("_rlnAccumMotionLate")) == ["10"]


def test_node_creator_icebreaker_enhancecontrast(
    mock_environment, offline_transport, tmp_path
):
    """
    Send a test message to the node creator for
    icebreaker.micrograph_analysis.enhancecontrast
    """
    job_dir = "IceBreaker/job004"
    input_file = tmp_path / "MotionCorr/job002/Movies/sample.mrc"
    output_file = tmp_path / job_dir
    output_file.mkdir(parents=True)

    setup_and_run_node_creation(
        mock_environment,
        offline_transport,
        tmp_path,
        job_dir,
        "icebreaker.micrograph_analysis.enhancecontrast",
        input_file,
        output_file,
        results={"icebreaker_type": "enhancecontrast", "total_motion": "10"},
    )

    # Check the output file structure
    assert (tmp_path / job_dir / "flattened_micrographs.star").exists()
    micrographs_file = cif.read_file(
        str(tmp_path / job_dir / "flattened_micrographs.star")
    )

    micrographs_data = micrographs_file.find_block("micrographs")
    assert list(micrographs_data.find_loop("_rlnMicrographName")) == [
        "IceBreaker/job004/Movies/sample_flattened.mrc"
    ]
    assert list(micrographs_data.find_loop("_rlnMicrographMetadata")) == [
        "MotionCorr/job002/Movies/sample.star"
    ]
    assert list(micrographs_data.find_loop("_rlnOpticsGroup")) == ["1"]
    assert list(micrographs_data.find_loop("_rlnAccumMotionTotal")) == ["10"]
    assert list(micrographs_data.find_loop("_rlnAccumMotionEarly")) == ["0.0"]
    assert list(micrographs_data.find_loop("_rlnAccumMotionLate")) == ["10"]


def test_node_creator_icebreaker_summary(mock_environment, offline_transport, tmp_path):
    """
    Send a test message to the node creator for
    icebreaker.micrograph_analysis.summary
    """
    job_dir = "IceBreaker/job005"
    input_file = tmp_path / "IceBreaker/job003/Movies/sample.mrc"
    output_file = tmp_path / job_dir
    output_file.mkdir(parents=True)

    (output_file / "five_figs_test.csv").touch()

    setup_and_run_node_creation(
        mock_environment,
        offline_transport,
        tmp_path,
        job_dir,
        "icebreaker.micrograph_analysis.summary",
        input_file,
        output_file,
        results={"icebreaker_type": "summary", "total_motion": "10"},
    )

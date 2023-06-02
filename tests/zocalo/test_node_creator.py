from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional
from unittest import mock

import pytest
import zocalo.configuration
from gemmi import cif
from workflows.transport.offline_transport import OfflineTransport

from relion.cryolo_relion_it import dls_options
from relion.cryolo_relion_it.cryolo_relion_it import RelionItOptions

relion_it_options = RelionItOptions()
relion_it_options.update_from(vars(dls_options))

node_creator = pytest.importorskip(
    "relion.zocalo.node_creator",
    reason="these tests require a modified version of the ccpem pipeliner",
)


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


def test_node_creator_ctffind(mock_environment, offline_transport, tmp_path):
    """
    Send a test message to the node creator for
    relion.ctffind.ctffind4
    """
    job_dir = "CtfFind/job006"
    input_file = tmp_path / "MotionCorr/job002/Movies/sample.mrc"
    output_file = tmp_path / job_dir / "Movies/sample.ctf"

    output_file.parent.mkdir(parents=True)
    with open(output_file.with_suffix(".txt"), "w") as f:
        f.write("0.0 1.0 2.0 3.0 4.0 5.0 6.0")

    setup_and_run_node_creation(
        mock_environment,
        offline_transport,
        tmp_path,
        job_dir,
        "relion.ctffind.ctffind4",
        input_file,
        output_file,
    )

    # Check the output file structure
    assert (tmp_path / job_dir / "micrographs_ctf.star").exists()
    micrographs_file = cif.read_file(str(tmp_path / job_dir / "micrographs_ctf.star"))

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
    assert list(micrographs_data.find_loop("_rlnOpticsGroup")) == ["1"]
    assert list(micrographs_data.find_loop("_rlnCtfImage")) == [
        "CtfFind/job006/Movies/sample.star:mrc"
    ]
    assert list(micrographs_data.find_loop("_rlnDefocusU")) == ["1.0"]
    assert list(micrographs_data.find_loop("_rlnDefocusV")) == ["2.0"]
    assert list(micrographs_data.find_loop("_rlnCtfAstigmatism")) == ["1.0"]
    assert list(micrographs_data.find_loop("_rlnDefocusAngle")) == ["3.0"]
    assert list(micrographs_data.find_loop("_rlnCtfFigureOfMerit")) == ["5.0"]
    assert list(micrographs_data.find_loop("_rlnCtfMaxResolution")) == ["6.0"]


def test_node_creator_cryolo(mock_environment, offline_transport, tmp_path):
    """
    Send a test message to the node creator for
    cryolo.autopick
    """
    job_dir = "AutoPick/job007"
    input_file = tmp_path / "MotionCorr/job002/Movies/sample.mrc"
    output_file = tmp_path / job_dir / "STAR/sample.star"

    (tmp_path / "MotionCorr/job002/").mkdir(parents=True)
    (tmp_path / "MotionCorr/job002/corrected_micrographs.star").touch()

    (tmp_path / job_dir / "DISTR").mkdir(parents=True)
    with open(
        tmp_path / job_dir / "DISTR/confidence_distribution_summary_1.txt", "w"
    ) as f:
        f.write("Metric, Value\nMEAN, 1.0\nSD, 1.0\nQ25, 0.5\nQ50, 1.0\nQ75, 1.5")

    setup_and_run_node_creation(
        mock_environment,
        offline_transport,
        tmp_path,
        job_dir,
        "cryolo.autopick",
        input_file,
        output_file,
    )

    # Check the output file structure
    assert (tmp_path / job_dir / "autopick.star").exists()
    micrographs_file = cif.read_file(str(tmp_path / job_dir / "autopick.star"))

    micrographs_data = micrographs_file.find_block("coordinate_files")
    assert list(micrographs_data.find_loop("_rlnMicrographName")) == [
        "MotionCorr/job002/Movies/sample.mrc"
    ]
    assert list(micrographs_data.find_loop("_rlnMicrographCoordinates")) == [
        "AutoPick/job007/STAR/sample.star"
    ]


def test_node_creator_extract(mock_environment, offline_transport, tmp_path):
    """
    Send a test message to the node creator for
    relion.extract
    """
    job_dir = "Extract/job008"
    input_file = Path(
        f"{tmp_path}/AutoPick/job007/STAR/sample.star"
        f":{tmp_path}/CtfFind/job006/Movies/sample.ctf"
    )
    output_file = tmp_path / job_dir / "Movies/sample.star"

    output_file.parent.mkdir(parents=True)
    with open(output_file, "w") as f:
        f.write("data_particles\n\nloop_\n_rlnCoordinateX\n_rlnCoordinateY\n1.0 2.0")

    setup_and_run_node_creation(
        mock_environment,
        offline_transport,
        tmp_path,
        job_dir,
        "relion.extract",
        input_file,
        output_file,
        results={"box_size": 64},
    )

    # Check the output file structure
    assert (tmp_path / job_dir / "particles.star").exists()
    micrographs_file = cif.read_file(str(tmp_path / job_dir / "particles.star"))

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
    assert list(micrographs_optics.find_loop("_rlnImagePixelSize")) == [
        str(relion_it_options.angpix)
    ]
    assert list(micrographs_optics.find_loop("_rlnImageSize")) == ["64"]
    assert list(micrographs_optics.find_loop("_rlnImageDimensionality")) == ["2"]
    assert list(micrographs_optics.find_loop("_rlnCtfDataAreCtfPremultiplied")) == ["0"]

    micrographs_data = micrographs_file.find_block("particles")
    assert list(micrographs_data.find_loop("_rlnCoordinateX")) == ["1.0"]
    assert list(micrographs_data.find_loop("_rlnCoordinateY")) == ["2.0"]


def test_node_creator_select(mock_environment, offline_transport, tmp_path):
    """
    Send a test message to the node creator for
    relion.select.split
    """
    job_dir = "Select/job009"
    input_file = tmp_path / "Extract/job007/Movies/sample.star"
    output_file = tmp_path / job_dir / "particles_split2.star"

    setup_and_run_node_creation(
        mock_environment,
        offline_transport,
        tmp_path,
        job_dir,
        "relion.select.split",
        input_file,
        output_file,
    )

    # Check the output file structure
    assert (
        tmp_path / ".Nodes/ParticlesData/Select/job009/particles_split2.star"
    ).exists()

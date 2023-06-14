from __future__ import annotations

import sys
from unittest import mock

import pytest
import zocalo.configuration
from workflows.transport.offline_transport import OfflineTransport

from relion.zocalo import select_classes


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
@mock.patch("relion.zocalo.ctffind.procrunner.run")
def test_select_classes_service(
    mock_procrunner, mock_environment, offline_transport, tmp_path
):
    """
    Send a test message to the select classes service
    This should
    """
    mock_procrunner().returncode = 0

    header = {
        "message-id": mock.sentinel,
        "subscription": mock.sentinel,
    }

    autoselect_file = tmp_path / "Select/job012/rank_model.star"
    autoselect_file.parent.mkdir(parents=True)
    with open(autoselect_file, "w") as f:
        f.write(
            "data_model_classes\n\nloop_\n_rlnReferenceImage\n"
            "_rlnClassScore\n_rlnClassDistribution\n"
            "_rlnAccuracyRotations\n_rlnAccuracyTranslationsAngst\n"
            "_rlnEstimatedResolution\n_rlnOverallFourierCompleteness\n"
            "_rlnClassPriorOffsetX\n_rlnClassPriorOffsetY"
        )
        f.write(
            "\n000001@Class2D/job010/run_it020_classes.mrcs "
            "0.004 0.035 3.100 1.416 16.183 1.000 -0.133 -0.001"
            "\n000002@Class2D/job010/run_it020_classes.mrcs "
            "0.008 0.035 3.100 1.416 16.183 1.000 -0.133 -0.001"
        )

    relion_it_options = {
        "do_icebreaker_group": True,
        "class2d_fraction_of_classes_to_remove": 0.5,
    }

    select_test_message = {
        "parameters": {
            "input_file": f"{tmp_path}/Class2D/job010/run_it020_optimiser.star",
            "combine_star_job_number": 13,
            "particle_diameter": 64,
            "particles_file": "particles.star",
            "classes_file": "class_averages.star",
            "python_exe": "/dls_sw/apps/EM/relion/4.0/conda/bin/python",
            "min_score": 0,
            "min_particles": 500,
            "class3d_batch_size": 50000,
            "class3d_max_size": 200000,
            "relion_it_options": relion_it_options,
        },
        "content": "dummy",
    }

    # Set up the mock service and send the message to it
    service = select_classes.SelectClasses(environment=mock_environment)
    service.transport = offline_transport
    service.start()
    service.total_count = 60000
    service.select_classes(None, header=header, message=select_test_message)

    assert mock_procrunner.call_count == 4
    mock_procrunner.assert_any_call(
        command=[
            "relion_class_ranker",
            "--opt",
            select_test_message["parameters"]["input_file"],
            "--o",
            "Select/job012/",
            "--auto_select",
            "--fn_root",
            "rank",
            "--do_granularity_features",
            "--fn_sel_parts",
            "particles.star",
            "--fn_sel_classavgs",
            "class_averages.star",
            "--python",
            select_test_message["parameters"]["python_exe"],
            "--select_min_nr_particles",
            "500",
            "--pipeline_control",
            "Select/job012/",
            "--min_score",
            "0.006",
        ],
        callback_stdout=mock.ANY,
        working_directory=str(tmp_path),
    )
    mock_procrunner.assert_any_call(
        command=[
            "combine_star_files.py",
            f"{tmp_path}/Select/job012/particles.star",
            "--output_dir",
            f"{tmp_path}/Select/job013",
            "--split",
            "--split_size",
            "50000",
        ],
        callback_stdout=mock.ANY,
        working_directory=str(tmp_path),
    )

    # Check that the correct messages were sent
    offline_transport.send.assert_any_call(
        destination="spa.node_creator",
        message={
            "parameters": {
                "job_type": "relion.select.class2dauto",
                "input_file": select_test_message["parameters"]["input_file"],
                "output_file": f"{tmp_path}/Select/job012/particles.star",
                "relion_it_options": select_test_message["parameters"][
                    "relion_it_options"
                ],
            },
            "content": "dummy",
        },
    )
    offline_transport.send.assert_any_call(
        destination="spa.node_creator",
        message={
            "parameters": {
                "job_type": "combine_star_files_job",
                "input_file": f"{tmp_path}/Select/job012/particles.star",
                "output_file": f"{tmp_path}/Select/job013/particles_all.star",
                "relion_it_options": select_test_message["parameters"][
                    "relion_it_options"
                ],
            },
            "content": "dummy",
        },
    )
    offline_transport.send.assert_any_call(
        destination="murfey_feedback",
        message={
            "register": "run_class3d",
            "class3d": {
                "particles_file": f"{tmp_path}/Select/job013/particles_split1.star",
                "class3d_dir": f"{tmp_path}/Class3D/job",
                "particle_diameter": 64,
                "relion_it_options": select_test_message["parameters"][
                    "relion_it_options"
                ],
            },
        },
    )


def test_parse_combiner_output(mock_environment, offline_transport):
    """
    Send test lines to the output parser
    to check the number of particles are being read in
    """
    service = select_classes.SelectClasses(environment=mock_environment)
    service.transport = offline_transport
    service.start()

    select_classes.SelectClasses.parse_combiner_output(
        service, "Adding Select/job/particles_all.star with 10 particles"
    )
    select_classes.SelectClasses.parse_combiner_output(service, "Split 20 particles")
    assert service.previous_total_count == 10
    assert service.total_count == 20

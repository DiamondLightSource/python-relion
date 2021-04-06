import pytest
from relion._parser.processnode import ProcessNode
from relion._parser.processgraph import ProcessGraph
from relion._parser.relion_pipeline import RelionPipeline
import pathlib


@pytest.fixture
def next_node_01():
    return ProcessNode("MotionCorr/job002")


@pytest.fixture
def next_node_02():
    return ProcessNode("CtfFind/job003")


@pytest.fixture
def node_with_links(next_node_01, next_node_02):
    node = ProcessNode("Import/job001")
    node.link_to(next_node_01)
    node.link_to(next_node_02)
    return node


@pytest.fixture
def graph(node_with_links, next_node_01, next_node_02):
    node_links = [node_with_links, next_node_01, next_node_02]
    return ProcessGraph(node_links)


@pytest.fixture
def pipeline(graph):
    rpipeline = RelionPipeline("Import/job001", graph)
    rpipeline._job_nodes = rpipeline._nodes
    return rpipeline


def test_relion_pipeline_iterator_with_preprepared_pipeline(
    pipeline, node_with_links, next_node_01, next_node_02
):
    assert list(pipeline) == ["Import", "MotionCorr", "CtfFind"]


def test_relion_pipeline_load_files_from_star(dials_data):
    pipeline = RelionPipeline("Import/job001")
    pipeline.load_nodes_from_star(
        dials_data("relion_tutorial_data") / "default_pipeline.star"
    )
    assert "Extract/job018" in pipeline._nodes
    assert "Class2D/job008/run_it025_model.star" in pipeline._nodes
    assert "Extract/job018" in pipeline._job_nodes
    assert "Class2D/job008/run_it025_model.star" not in pipeline._job_nodes


def test_relion_pipeline_check_job_node_statuses(dials_data):
    pipeline = RelionPipeline("Import/job001")
    pipeline.load_nodes_from_star(
        dials_data("relion_tutorial_data") / "default_pipeline.star"
    )
    pipeline.check_job_node_statuses(pathlib.Path(dials_data("relion_tutorial_data")))
    assert pipeline._job_nodes[pipeline._job_nodes.index("Extract/job018")].attributes[
        "status"
    ]
    assert pipeline._job_nodes[pipeline._job_nodes.index("Class2D/job008")].attributes[
        "status"
    ]


def test_relion_pipeline_current_job_property_without_any_start_time_information(
    dials_data,
):
    pipeline = RelionPipeline("Import/job001")
    pipeline.load_nodes_from_star(
        dials_data("relion_tutorial_data") / "default_pipeline.star"
    )
    pipeline.check_job_node_statuses(pathlib.Path(dials_data("relion_tutorial_data")))
    assert pipeline.current_jobs is None


def test_relion_pipeline_collect_job_times_from_dials_data_logs(dials_data):
    pipeline = RelionPipeline("Import/job001")
    pipeline.load_nodes_from_star(
        dials_data("relion_tutorial_data") / "default_pipeline.star"
    )
    logs = list(pathlib.Path(dials_data("relion_tutorial_data")).glob("pipeline*.log"))
    assert dials_data("relion_tutorial_data") / "pipeline_PREPROCESS.log" in logs
    pipeline.collect_job_times(logs)
    for job in pipeline._job_nodes:
        assert job.attributes.get("start_time_stamp") is not None
    assert (
        pipeline._job_nodes[pipeline._job_nodes.index("MotionCorr/job002")].attributes[
            "job_count"
        ]
        == 2
    )
    assert (
        pipeline._job_nodes[pipeline._job_nodes.index("Class2D/job008")].attributes[
            "job_count"
        ]
        == 1
    )


def test_relion_pipeline_current_job_property_with_timing_info(dials_data):
    pipeline = RelionPipeline("Import/job001")
    pipeline.load_nodes_from_star(
        dials_data("relion_tutorial_data") / "default_pipeline.star"
    )
    logs = list(pathlib.Path(dials_data("relion_tutorial_data")).glob("pipeline*.log"))
    pipeline.collect_job_times(logs)
    pipeline.check_job_node_statuses(pathlib.Path(dials_data("relion_tutorial_data")))
    pipeline._job_nodes[pipeline._job_nodes.index("LocalRes/job031")].attributes[
        "status"
    ] = None
    assert str(pipeline.current_jobs[0]._path) == "LocalRes/job031"

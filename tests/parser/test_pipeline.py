import pytest
from relion._parser.processnode import ProcessNode
from relion._parser.processgraph import ProcessGraph
from relion._parser.pipeline import Pipeline
from unittest import mock


@pytest.fixture
def node_with_links():
    node = ProcessNode("Project/Import/job001")
    next_node_01 = ProcessNode("Project/MotionCorr/job002")
    next_node_02 = ProcessNode("Project/CtfFind/job003")
    node.link_to(next_node_01)
    node.link_to(next_node_02)
    return node


@pytest.fixture
def graph(node_with_links):
    next_node_01 = ProcessNode("Project/MotionCorr/job002")
    next_node_02 = ProcessNode("Project/CtfFind/job003")
    node_links = [node_with_links, next_node_01, next_node_02]
    return ProcessGraph(node_links)


@mock.patch("relion._parser.pipeline.Digraph")
def test_Pipeline_show_all_nodes(mock_Digraph, graph):
    pipeline = Pipeline("Project/Import/job001", graph)
    pipeline.show_all_nodes()
    mock_Digraph.assert_called_once()
    mock_Digraph.return_value.attr.assert_called_once()
    nodecalls = [
        mock.call(name="Project/Import/job001"),
        mock.call(name="Project/MotionCorr/job002"),
        mock.call(name="Project/CtfFind/job003"),
    ]
    mock_Digraph.return_value.node.assert_has_calls(nodecalls)
    edgecalls = [
        mock.call("Project/Import/job001", "Project/MotionCorr/job002"),
        mock.call("Project/Import/job001", "Project/CtfFind/job003"),
    ]
    mock_Digraph.return_value.edge.assert_has_calls(edgecalls)
    mock_Digraph.return_value.render.assert_called_once()

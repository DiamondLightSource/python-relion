import pytest
from relion._parser.pipeline import ProcessNode, PipelineNode
import pathlib


@pytest.fixture
def node_with_links():
    node = ProcessNode(pathlib.Path("Project/Import/job001"))
    next_node_01 = ProcessNode(pathlib.Path("Project/MotionCorr/job002"))
    next_node_02 = ProcessNode(pathlib.Path("Project/CtfFind/job003"))
    node.link_to(next_node_01)
    node.link_to(next_node_02)
    return node


def test_process_node_iterator_behaviour(node_with_links):
    next_node_01 = ProcessNode(pathlib.Path("Project/MotionCorr/job002"))
    next_node_02 = ProcessNode(pathlib.Path("Project/CtfFind/job003"))
    assert next_node_01 in node_with_links
    assert next_node_02 in node_with_links


def test_process_node_length_behaviour(node_with_links):
    assert len(node_with_links) == 2


def test_process_node_removal_of_a_linked_node(node_with_links):
    next_node_01 = ProcessNode(pathlib.Path("Project/MotionCorr/job002"))
    next_node_02 = ProcessNode(pathlib.Path("Project/CtfFind/job003"))
    node_with_links.unlink_from(next_node_02)
    assert next_node_01 in node_with_links
    assert next_node_02 not in node_with_links
    assert len(node_with_links) == 1


@pytest.fixture
def pnode_with_links(node_with_links):
    return PipelineNode(node_with_links, 1)


@pytest.fixture
def pnode_with_links_02(node_with_links):
    return PipelineNode(node_with_links, 2)


def test_pipeline_node_inheritance_of_links(pnode_with_links):
    next_node_01 = ProcessNode(pathlib.Path("Project/MotionCorr/job002"))
    next_node_02 = ProcessNode(pathlib.Path("Project/CtfFind/job003"))
    assert next_node_01 in pnode_with_links
    assert next_node_02 in pnode_with_links
    assert len(pnode_with_links) == 2


def test_pipeline_node_less_than_behaviour(pnode_with_links, pnode_with_links_02):
    """
    For a PipelineNode the origin_distance (designed to be used as the distance from
    the node without any parent nodes) is used to define the ordering of nodes
    """
    assert pnode_with_links < pnode_with_links_02

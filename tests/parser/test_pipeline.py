import pytest
from relion._parser.pipeline import ProcessNode, ProcessGraph
import pathlib


@pytest.fixture
def node_with_links():
    node = ProcessNode(pathlib.Path("Project/Import/job001"))
    next_node_01 = ProcessNode(pathlib.Path("Project/MotionCorr/job002"))
    next_node_02 = ProcessNode(pathlib.Path("Project/CtfFind/job003"))
    node.link_to(next_node_01)
    node.link_to(next_node_02)
    return node


@pytest.fixture
def graph(node_with_links):
    next_node_01 = ProcessNode(pathlib.Path("Project/MotionCorr/job002"))
    next_node_02 = ProcessNode(pathlib.Path("Project/CtfFind/job003"))
    node_links = [node_with_links, next_node_01, next_node_02]
    return ProcessGraph(node_links)


@pytest.fixture
def no_link_graph():
    node = ProcessNode(pathlib.Path("Project/Import/job001"))
    next_node_01 = ProcessNode(pathlib.Path("Project/MotionCorr/job002"))
    next_node_02 = ProcessNode(pathlib.Path("Project/CtfFind/job003"))
    node_links = [node, next_node_01, next_node_02]
    return ProcessGraph(node_links)


def test_process_node_equality_with_another_process_node(node_with_links):
    node = ProcessNode(pathlib.Path("Project/Import/job001"))
    assert not node == node_with_links
    next_node_01 = ProcessNode(pathlib.Path("Project/MotionCorr/job002"))
    next_node_02 = ProcessNode(pathlib.Path("Project/CtfFind/job003"))
    node.link_to(next_node_01)
    node.link_to(next_node_02)
    assert node == node_with_links


def test_process_node_equality_with_string_or_path(node_with_links):
    assert node_with_links == "Project/Import/job001"
    assert not node_with_links == "Project/MotionCorr/job002"
    assert node_with_links == pathlib.Path("Project") / "Import" / "job001"


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


def test_process_node_child_checking_behaviour(node_with_links):
    next_node_01 = ProcessNode(pathlib.Path("Project/MotionCorr/job002"))
    assert node_with_links._is_child(next_node_01)


def test_process_node_less_than_behaviour(node_with_links):
    next_node_01 = ProcessNode(pathlib.Path("Project/MotionCorr/job002"))
    assert node_with_links < next_node_01


def test_process_graph_equality(graph, no_link_graph):
    assert not graph == no_link_graph
    no_link_graph._node_list[0].link_to(no_link_graph._node_list[1])
    no_link_graph._node_list[0].link_to(no_link_graph._node_list[2])
    assert graph == no_link_graph


def test_process_graph_length(graph):
    assert len(graph) == 3


def test_process_graph_access_to_elements_via_index(graph):
    next_node_01 = ProcessNode(pathlib.Path("Project/MotionCorr/job002"))
    assert graph[1] == next_node_01


def test_process_graph_extend_behaves_like_a_list_extend(graph):
    old_node_list = graph._node_list
    external_node_01 = ProcessNode(pathlib.Path("Project/External/job004"))
    external_node_02 = ProcessNode(pathlib.Path("Project/External/job005"))
    old_node_list.extend([external_node_01, external_node_02])
    graph.extend(ProcessGraph([external_node_01, external_node_02]))
    assert graph._node_list == old_node_list


def test_process_graph_can_get_the_index_of_a_provided_element(graph):
    next_node_01 = ProcessNode(pathlib.Path("Project/MotionCorr/job002"))
    assert graph.index(next_node_01) == 1
    assert graph.index("Project/MotionCorr/job002") == 1


def test_process_graph_node_explore_collects_all_nodes_from_provided_node_onwards(
    graph, node_with_links
):
    next_node_01 = ProcessNode(pathlib.Path("Project/MotionCorr/job002"))
    next_node_02 = ProcessNode(pathlib.Path("Project/CtfFind/job003"))
    explored = []
    graph.node_explore(next_node_01, explored)
    assert explored == [next_node_01]
    explored = []
    graph.node_explore(node_with_links, explored)
    assert explored == [node_with_links, next_node_01, next_node_02]
    explored = []
    with pytest.raises(ValueError):
        graph.node_explore("Project/Import/job001", explored)

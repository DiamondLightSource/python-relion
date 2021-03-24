import pytest
from relion._parser.pipeline import ProcessNode, ProcessGraph
import pathlib
import copy


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


@pytest.fixture
def overlapping_graph():
    node = ProcessNode("Project/Import/job001")
    next_node = ProcessNode("Project/External/job004")
    node.link_to(next_node)
    node_links = [node, next_node]
    return ProcessGraph(node_links)


@pytest.fixture
def new_origin_graph():
    node = ProcessNode("Project/External/job004")
    next_node = ProcessNode("Project/CtfFind/job003")
    node.link_to(next_node)
    node_links = [node, next_node]
    return ProcessGraph(node_links)


@pytest.fixture
def no_link_graph():
    node = ProcessNode("Project/Import/job001")
    next_node_01 = ProcessNode("Project/MotionCorr/job002")
    next_node_02 = ProcessNode("Project/CtfFind/job003")
    node_links = [node, next_node_01, next_node_02]
    return ProcessGraph(node_links)


def test_process_node_equality_with_another_process_node(node_with_links):
    node = ProcessNode("Project/Import/job001")
    assert not node == node_with_links
    next_node_01 = ProcessNode("Project/MotionCorr/job002")
    next_node_02 = ProcessNode("Project/CtfFind/job003")
    node.link_to(next_node_01)
    node.link_to(next_node_02)
    for n in node_with_links:
        assert n in node
    assert node == node_with_links


def test_process_node_equality_with_string_or_path(node_with_links):
    assert node_with_links == "Project/Import/job001"
    assert not node_with_links == "Project/MotionCorr/job002"
    assert node_with_links == pathlib.PurePosixPath("Project") / "Import" / "job001"


def test_process_node_iterator_behaviour(node_with_links):
    next_node_01 = ProcessNode("Project/MotionCorr/job002")
    next_node_02 = ProcessNode("Project/CtfFind/job003")
    assert next_node_01 in node_with_links
    assert next_node_02 in node_with_links


def test_process_node_length_behaviour(node_with_links):
    assert len(node_with_links) == 2


def test_process_node_removal_of_a_linked_node(node_with_links):
    next_node_01 = ProcessNode("Project/MotionCorr/job002")
    next_node_02 = ProcessNode("Project/CtfFind/job003")
    node_with_links.unlink_from(next_node_02)
    assert next_node_01 in node_with_links
    assert next_node_02 not in node_with_links
    assert len(node_with_links) == 1


def test_process_node_child_checking_behaviour(node_with_links):
    next_node_01 = ProcessNode("Project/MotionCorr/job002")
    assert node_with_links._is_child(next_node_01)


def test_process_node_less_than_behaviour(node_with_links):
    next_node_01 = ProcessNode("Project/MotionCorr/job002")
    assert node_with_links < next_node_01


def test_process_graph_equality(graph, no_link_graph):
    assert not graph == no_link_graph
    no_link_graph._node_list[0].link_to(no_link_graph._node_list[1])
    no_link_graph._node_list[0].link_to(no_link_graph._node_list[2])
    assert graph == no_link_graph


def test_process_graph_length(graph):
    assert len(graph) == 3


def test_process_graph_access_to_elements_via_index(graph):
    next_node_01 = ProcessNode("Project/MotionCorr/job002")
    assert graph[1] == next_node_01


def test_process_graph_extend_behaves_like_a_list_extend(graph):
    old_node_list = graph._node_list
    external_node_01 = ProcessNode("Project/External/job004")
    external_node_02 = ProcessNode("Project/External/job005")
    old_node_list.extend([external_node_01, external_node_02])
    graph.extend(ProcessGraph([external_node_01, external_node_02]))
    assert graph._node_list == old_node_list


def test_process_graph_can_get_the_index_of_a_provided_element(graph):
    next_node_01 = ProcessNode("Project/MotionCorr/job002")
    assert graph.index(next_node_01) == 1
    assert graph.index("Project/MotionCorr/job002") == 1


def test_process_graph_node_explore_collects_all_nodes_from_provided_node_onwards(
    graph, node_with_links
):
    next_node_01 = ProcessNode("Project/MotionCorr/job002")
    next_node_02 = ProcessNode("Project/CtfFind/job003")
    explored = []
    graph.node_explore(next_node_01, explored)
    assert explored == [next_node_01]
    explored = []
    graph.node_explore(node_with_links, explored)
    assert explored == [node_with_links, next_node_01, next_node_02]
    explored = []
    with pytest.raises(ValueError):
        graph.node_explore("Project/Import/job001", explored)


def test_process_graph_add_node(graph, node_with_links):
    next_node_01 = ProcessNode("Project/MotionCorr/job002")
    next_node_02 = ProcessNode("Project/CtfFind/job003")
    new_node = ProcessNode("Project/External/job004")
    graph.add_node(new_node)
    assert graph._node_list == [node_with_links, next_node_01, next_node_02, new_node]


def test_process_graph_remove_node_without_any_links(graph, node_with_links):
    next_node_01 = ProcessNode("Project/MotionCorr/job002")
    next_node_02 = ProcessNode("Project/CtfFind/job003")
    graph.remove_node(next_node_01)
    assert list(graph) == [node_with_links, next_node_02]
    graph.remove_node("Project/CtfFind/job003")
    assert list(graph) == [node_with_links]


def test_process_graph_link_from_to_does_the_linking_correctly(no_link_graph):
    node = ProcessNode("Project/Import/job001")
    next_node_01 = ProcessNode("Project/MotionCorr/job002")
    no_link_graph.link_from_to(node, next_node_01)
    assert list(no_link_graph[0])[0] == next_node_01


def test_process_graph_remove_node_and_check_links_still_work(graph):
    # Can't use graph fixture here as linking a child node to a new node wouldn't change the parent node in the way required for a fixture
    node = ProcessNode("Project/Import/job001")
    next_node_01 = ProcessNode("Project/MotionCorr/job002")
    next_node_02 = ProcessNode("Project/CtfFind/job003")
    node.link_to(next_node_01)
    node.link_to(next_node_02)
    new_graph = ProcessGraph([node, next_node_01, next_node_02])
    last_node = ProcessNode("Project/External/job004")
    new_graph.link_from_to(next_node_01, last_node)
    new_graph.add_node(last_node)
    assert list(list(new_graph[0])[0])[0] == last_node
    new_graph.remove_node(new_graph[1])
    assert list(new_graph) == [node, next_node_02, last_node]
    assert list(new_graph[0])[1] == last_node


def test_process_graph_find_origins_finds_nodes_without_parents(graph, node_with_links):
    orgs = graph.find_origins()
    assert orgs == [node_with_links]
    new_node = ProcessNode("Project/External/job004")
    graph.add_node(new_node)
    orgs = graph.find_origins()
    assert orgs == [node_with_links, new_node]


def test_process_graph_merge_with_new_origin(graph, new_origin_graph):
    merged = graph.merge(new_origin_graph)
    assert merged
    assert len(graph) == 4
    assert list(graph[0])[1] == list(graph[3])[0]


def test_process_graph_merge_with_common_origin(graph, overlapping_graph):
    merged = graph.merge(overlapping_graph)
    assert merged
    assert len(graph) == 4
    assert len(graph[0]) == 3
    assert "Project/External/job004" in graph[0]


def test_process_graph_merge_does_not_merge_if_merging_graph_is_not_connected_to_original_graph(
    graph,
):
    new_graph = ProcessGraph([ProcessNode("Project/External/job005")])
    merged = graph.merge(new_graph)
    assert not merged


def test_process_graph_split_connected(graph, new_origin_graph):
    new_node = ProcessNode("Project/External/job005")
    graph.merge(new_origin_graph)
    graph_snapshot = copy.deepcopy(graph)
    graph.add_node(new_node)
    connected = graph.split_connected()
    assert len(connected) == 2
    assert connected == [graph_snapshot, ProcessGraph([new_node])]


def test_process_graph_wipe(graph):
    graph.wipe()
    assert len(graph) == 0

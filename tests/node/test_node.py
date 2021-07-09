import pytest

from relion.node import Node


@pytest.fixture
def node_with_links():
    node = Node("Project/Import/job001")
    next_node_01 = Node("Project/MotionCorr/job002")
    next_node_02 = Node("Project/CtfFind/job003")
    node.link_to(next_node_01)
    node.link_to(next_node_02)
    return node


def test_process_node_equality_with_another_process_node(node_with_links):
    node = Node("Project/Import/job001")
    assert not node == node_with_links
    next_node_01 = Node("Project/MotionCorr/job002")
    next_node_02 = Node("Project/CtfFind/job003")
    node.link_to(next_node_01)
    node.link_to(next_node_02)
    for n in node_with_links:
        assert n in node
    assert node == node_with_links


def test_process_node_iterator_behaviour(node_with_links):
    next_node_01 = Node("Project/MotionCorr/job002")
    next_node_02 = Node("Project/CtfFind/job003")
    assert next_node_01 in node_with_links
    assert next_node_02 in node_with_links


def test_process_node_length_behaviour(node_with_links):
    assert len(node_with_links) == 2


def test_process_node_removal_of_a_linked_node(node_with_links):
    next_node_01 = Node("Project/MotionCorr/job002")
    next_node_02 = Node("Project/CtfFind/job003")
    node_with_links.unlink_from(next_node_02)
    assert next_node_01 in node_with_links
    assert next_node_02 not in node_with_links
    assert len(node_with_links) == 1


def test_process_node_child_checking_behaviour(node_with_links):
    next_node_01 = Node("Project/MotionCorr/job002")
    assert node_with_links._is_child(next_node_01)


def test_process_node_less_than_behaviour(node_with_links):
    next_node_01 = Node("Project/MotionCorr/job002")
    assert node_with_links < next_node_01


def test_node_call_fills_completed_correctly():
    node_A = Node("A")
    node_B = Node("B")
    node_C = Node("C")
    node_A.link_to(node_B)
    node_A.link_to(node_C)
    node_A()
    assert node_B._completed == [node_A]
    assert node_C._completed == [node_A]


def test_node_environment_insert():
    node = Node("A")
    node.environment["test"] = 0
    assert node["test"] == 0


def test_unlink_from():
    node_A = Node("A")
    node_B = Node("B")
    node_A.link_to(node_B)
    assert node_A._out == [node_B]
    node_A.unlink_from(node_B)
    assert node_A._out == []


def test_node_call_adds_completed_status_to_out_nodes():
    node_A = Node("A")
    node_B = Node("B")
    node_C = Node("C")
    node_A.link_to(node_B)
    node_A.link_to(node_C)
    node_A()
    assert node_B._completed == [node_A]
    assert node_C._completed == [node_A]


def test_propagate_adds_correctly_to_environment():
    node_A = Node("A")
    node_A.environment["a"] = 1
    node_A.propagate(("a", "b"))
    assert node_A.environment["b"] == 1
    assert node_A.environment.propagate.store == {"b": 1}

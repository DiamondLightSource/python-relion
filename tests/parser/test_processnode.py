from __future__ import annotations

import pathlib

import pytest

from relion._parser.processnode import ProcessNode


@pytest.fixture
def node_with_links():
    node = ProcessNode("Project/Import/job001")
    next_node_01 = ProcessNode("Project/MotionCorr/job002")
    next_node_02 = ProcessNode("Project/CtfFind/job003")
    node.link_to(next_node_01)
    node.link_to(next_node_02)
    return node


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

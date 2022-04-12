from __future__ import annotations

import functools
import uuid

from relion.node.environment import Environment


@functools.total_ordering
class Node:
    """
    A basic class to hold data and a function for the processing of
    that data. To allow data to be passed between nodes lists of parent
    and child nodes are kept in _in and _out.
    """

    def __init__(self, name, independent=False, **kwargs):
        self._name = name
        self.nodeid = str(uuid.uuid4())[:8]
        self._out = []
        self._in = []
        self._completed = []
        self.environment = Environment()
        self._link_traffic = {}
        self._share_traffic = {}
        self._append_traffic = {}
        self._call_count = 0
        self._in_multi_call = False
        self._can_append = independent
        self.shape = "oval"
        for key, value in kwargs.items():
            self.environment[key] = value

    def __eq__(self, other):
        if isinstance(other, Node):
            if self.name == other.name and len(self._out) == len(other._out):
                for n in self._out:
                    if n not in other._out:
                        return False
                return True
        return False

    def __hash__(self):
        return hash(("relion.node.Node", self._name, tuple(self._out), self.nodeid))

    def __repr__(self):
        return f"Node({repr(str(self._name))})"

    def __iter__(self):
        return iter(self._out)

    def __len__(self):
        return len(self._out)

    def __lt__(self, other):
        if self._is_child(other):
            return True
        return False

    def __rshift__(self, other):
        if isinstance(other, Node):
            self.link_to(other)

    def __call__(self, *args, **kwargs):
        res = []
        self.environment.load_iterator()
        incomplete = self.environment.step()
        self._in_multi_call = True
        while incomplete:
            curr_res = self.func(*args, **kwargs)
            if isinstance(curr_res, list):
                res.extend(curr_res)
            else:
                res.append(curr_res)
            incomplete = self.environment.step()
        self._in_multi_call = False
        for node in self._out:
            node._completed.append(self)
        self._call_count += 1
        if len(res) == 0:
            return
        if len(res) == 1:
            return res[0]
        return res

    def __getitem__(self, key):
        return self.environment[key]

    def func(self, *args, **kwargs):
        """
        Function called when the Node is called.
        Does nothing for a Node: should be overwritten in derived classes
        """
        pass

    @property
    def name(self):
        return str(self._name)

    def change_name(self, new_name):
        self._name = new_name

    def link_to(
        self,
        next_node,
        traffic=None,
        result_as_traffic: bool = False,
        share: list = None,
    ):
        """
        Link to another Node. The Node being linked to needs to be a child of this Node.
        A data transfer between the nodes can be specified. This transfer will only happen
        if the nodes are placed within a Graph and the Graph is called.

        Keyword arguments:
        next_node -- the Node being linked to
        traffic -- a dict or list of dicts to be passed as a message to next_node (default None)
        result_as_traffic -- use the return value of calling this Node as traffic to next_node (default False)
        share -- list of tuples (key01, key02) of keys specifying the passing of a value in this node's environment with
        key01 to the environment of next_node with key02. This data transfer happens after this node is called
        and the data transferred may therefore be affected by the call (default None)
        """
        if next_node not in self._out:
            self._out.append(next_node)
            next_node._in.append(self)
            if traffic is None:
                self._link_traffic[next_node.nodeid] = {}
            else:
                self._link_traffic[next_node.nodeid] = traffic
            if result_as_traffic:
                self._link_traffic[next_node.nodeid] = None
            if share is not None:
                self._share_traffic[next_node.nodeid] = share

    def propagate(self, share):
        """
        Propagate data to downstream nodes.

        Keyword arguments:
        share -- tuple (key01, key02), the value in this node's environment with key01 will be placed
        into the environment of all child nodes (and their children) with key02
        """
        self.environment.propagate.update({share[1]: self.environment[share[0]]})

    def unlink_from(self, next_node):
        """
        Remove the link to a node

        Keyword arguments:
        next_node -- node in this node's _out that is being unlinked
        """
        if next_node in self._out:
            self._out.remove(next_node)

    def _is_child_checker(self, possible_child, checks):
        if self == possible_child:
            checks.extend([True])
        for child in self:
            checks.extend(child._is_child_checker(possible_child, checks=checks))
        return checks

    def _is_child(self, possible_child):
        if True in self._is_child_checker(possible_child, checks=[]):
            return True
        else:
            return False

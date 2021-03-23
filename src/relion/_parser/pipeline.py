import pathlib
from graphviz import Digraph
import collections.abc


class ProcessNode:
    def __init__(self, path, **kwargs):
        self._path = pathlib.Path(path)
        self._out = []
        self.attributes = {}
        for key, value in kwargs.items():
            self.attributes[key] = value
        self.attributes["status"] = kwargs.get("status")
        self.attributes["start_time_stamp"] = kwargs.get("start_time_stamp")
        self.attributes["end_time_stamp"] = kwargs.get("end_time_stamp")
        self.attributes["start_time"] = kwargs.get("start_time")
        self.attributes["end_time"] = kwargs.get("end_time")

    def __eq__(self, other):
        if isinstance(other, ProcessNode):
            return self._path == other._path and self._out == other._out
        else:
            try:
                return str(self._path) == str(other)
            except Exception:
                return False
        return False

    def __hash__(self):
        return hash(("relion._parser.pipeline.ProcessNode", self._path))

    def __repr__(self):
        return f"Node({repr(str(self._path))})"

    def __iter__(self):
        return iter(self._out)

    def __len__(self):
        return len(self._out)

    def __lt__(self, other):
        if self._is_child(other):
            return True
        return False

    def link_to(self, next_node):
        if next_node not in self._out:
            self._out.append(next_node)

    def unlink_from(self, next_node):
        if next_node in self._out:
            self._out.remove(next_node)

    def _is_child_checker(self, possible_child, checks=[]):
        if self == possible_child:
            checks.extend([True])
        for child in self:
            checks.extend(child._is_child_checker(possible_child, checks=checks))
        return checks

    def _is_child(self, possible_child):
        if True in self._is_child_checker(possible_child):
            return True
        else:
            return False


class ProcessGraph(collections.abc.Sequence):
    def __init__(self, node_list):
        self._node_list = node_list

    def __eq__(self, other):
        if isinstance(other, ProcessGraph):
            return self._node_list == other._node_list
        return False

    def __hash__(self):
        return hash(("relion._parser.pipeline.ProcessGraph", iter(self._node_list)))

    def __iter__(self):
        return iter(self._node_list)

    def __len__(self):
        return len(self._node_list)

    def __getitem__(self, index):
        if not isinstance(index, int):
            raise ValueError("Index of ProcessGraph must be an integer")
        return self._node_list[index]

    def extend(self, other):
        if not isinstance(other, ProcessGraph):
            raise ValueError("Can only extend a ProcessGraph with another ProcessGraph")
        self._node_list.extend(other._node_list)

    def index(self, node):
        return self._node_list.index(node)

    def node_explore(self, node, explored):
        if not isinstance(node, ProcessNode):
            raise ValueError(
                "ProcessGraph.node_explore must be called with a ProcessNode as the starting point; a string or similar is insufficient"
            )
        if node not in explored:
            explored.append(node)
        for next_node in node:
            self.node_explore(next_node, explored)

    def add_node(self, new_node):
        if isinstance(new_node, ProcessNode):
            self._node_list.append(new_node)
        else:
            raise ValueError("Attempted to add a node that was not a ProcessNode")

    def remove_node(self, node_name):
        behind_nodes = []
        for currnode in self:
            if node_name in currnode:
                print("check passed")
                behind_nodes.append(currnode)
                currnode.unlink_from(node_name)
        for bnode in behind_nodes:
            for next_node in self._node_list[self._node_list.index(node_name)]:
                bnode.link_to(next_node)
        self._node_list.remove(node_name)

    def find_origins(self):
        child_nodes = []
        for node in self:
            child_nodes.extend([next_node for next_node in node])
        origins = [p for p in self if p not in child_nodes]
        return origins

    def merge(self, other):
        if len(set(self).intersection(set(other))) > 0:
            for new_node in other:
                if new_node not in self:
                    self.add_node(new_node)
            return True
        else:
            return False

    def split_connected(self):
        connected_graphs = []
        for origin in self.find_origins():
            curr_graph = []
            self.node_explore(origin, curr_graph)
            connected_graphs.append(ProcessGraph(curr_graph))
        for_removal = []
        for i, g in enumerate(connected_graphs):
            for ng in connected_graphs[i + 1 :]:
                if g.merge(connected_graphs[i + 1 :]):
                    if ng not in for_removal:
                        for_removal.append(ng)
        for r in for_removal:
            connected_graphs.remove(r)
        return connected_graphs

    def wipe(self):
        self._node_list = []


class Pipeline:
    def __init__(self, origin):
        self.origin = origin
        self._nodes = ProcessGraph([])
        self._connected = {}
        self.origins = {}

    def _split_connected(self):
        connected_graphs = self._nodes.split_connected()
        ancillary_count = 1
        for cg in connected_graphs:
            if self.origin in cg:
                self._connected["main"] = cg
                self.origins["main"] = self._nodes[self._nodes.index(self.origin)]
            else:
                self._connected[f"ancillary:{ancillary_count}"] = cg
                self.origins[f"ancillary:{ancillary_count}"] = cg.find_origins()[0]
                ancillary_count += 1

    def show_all_nodes(self):
        digraph = Digraph(format="svg")
        digraph.attr(rankdir="LR")
        for node in self._nodes:
            digraph.node(name=str(node._path))
            for next_node in node:
                digraph.edge(str(node._path), str(next_node._path))
        digraph.render("pipeline.gv")

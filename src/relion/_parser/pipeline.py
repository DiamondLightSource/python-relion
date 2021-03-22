import numpy as np
import pathlib
from graphviz import Digraph


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
            return self._path == other._path
        return False

    def __repr__(self):
        return f"Node({repr(str(self._path))})"

    def __iter__(self):
        return iter(self._out)

    def __len__(self):
        return len(self._out)

    def link_to(self, next_node):
        if next_node not in self._out:
            self._out.append(next_node)

    def unlink_from(self, next_node):
        if next_node in self._out:
            self._out.remove(next_node)


class PipelineNode(ProcessNode):
    def __init__(self, in_node, origin_distance):
        super().__init__(in_node._path)
        self._out = in_node._out
        self.odist = origin_distance

    def __eq__(self, other):
        if isinstance(other, PipelineNode):
            return self.odist == other.odist
        return False

    def __repr__(self):
        return f"Node({repr(str(self._path))}, {self.odist})"

    def __lt__(self, other):
        if isinstance(other, PipelineNode):
            return self.odist < other.odist
        raise ValueError(
            "Attempted to compare a PipelineNode with something other than a PipelineNode"
        )


class Pipeline:
    def __init__(self, image_path, origin):
        self._image_path = image_path
        self.origin = origin
        self._nodes = []
        self._pnodes = []
        self._connected = {}
        self._pconnected = {}
        self.origins = {}

    def _add_node(self, new_node):
        if isinstance(new_node, ProcessNode):
            self._nodes.append(new_node)
            return True
        elif isinstance(new_node, PipelineNode):
            self._pnodes.append(new_node)
            return True
        raise ValueError("Attempted to add a node that was not a ProcessNode")

    def _node_explore(self, node, explored):
        if node not in explored:
            explored.append(node)
        for next_node in node:
            self._node_explore(next_node, explored)

    @staticmethod
    def _remove_node_from_graph(graph, node):
        behind_nodes = []
        for currnode in graph:
            if node in currnode:
                behind_nodes.append(currnode)
                currnode.unlink_from(node)
        for bnode in behind_nodes:
            for next_node in graph[graph.index(node)]:
                bnode.link_to(next_node)
        graph.remove(node)
        return graph

    @staticmethod
    def _find_an_origin(node_list):
        child_nodes = []
        for node in node_list:
            child_nodes.extend([next_node for next_node in node])
        origins = [p for p in node_list if p not in child_nodes]
        return origins[0]

    def _split_connected(self):
        self._connected["main"] = []
        self.origins["main"] = self._nodes[self._nodes.index(self.origin)]
        self._node_explore(self.origins["main"], self._connected["main"])
        unexplored = [p for p in self._nodes if p not in self._connected["main"]]
        while_counter = 1
        while len(unexplored) > 0:
            self._connected[f"ancillary:{while_counter}"] = []
            next_origin = self._find_an_origin(unexplored)
            self.origins[f"ancillary:{while_counter}"] = next_origin
            self._node_explore(
                next_origin, self._connected[f"ancillary:{while_counter}"]
            )

            for p in self._connected[f"ancillary:{while_counter}"]:
                if p in unexplored:
                    unexplored.remove(p)
            while_counter += 1

    def _traverse_and_count(self):
        # Use Dijkstra's to get node distances from the origin node
        for key in self._connected.keys():
            visited = []
            dists = [np.inf for _ in self._connected[key]]
            dists[0] = 0
            for i in range(len(self._connected[key]) - 1):
                curr_d = np.inf
                min_index = np.inf
                for dindex, d in enumerate(dists):
                    if d < curr_d and self._connected[key][dindex] not in visited:
                        curr_d = d
                        min_index = dindex
                visited.append(self._connected[key][min_index])
                for next_node in self._connected[key][min_index]:
                    alt = dists[min_index] + 1
                    if alt < dists[self._connected[key].index(next_node)]:
                        dists[self._connected[key].index(next_node)] = alt
            self._pconnected[key] = [
                PipelineNode(node, dist)
                for node, dist in zip(self._connected[key], dists)
            ]

    def _wipe_nodes(self):
        self._nodes = []
        self._pnodes = []
        self._connected = {}
        self._pconnected = {}
        self._origins = {}

    def show_all_nodes(self):
        digraph = Digraph(format="svg")
        digraph.attr(rankdir="LR")
        for node in self._nodes:
            digraph.node(name=str(node._path))
            for next_node in node:
                digraph.edge(str(node._path), str(next_node._path))
        digraph.render("pipeline.gv")

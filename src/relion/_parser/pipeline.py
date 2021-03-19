from gemmi import cif
import numpy as np
import os
import pathlib


class RelionNode:
    def __init__(self, path):
        self._path = pathlib.Path(path)
        self._out = []

    def __eq__(self, other):
        if isinstance(other, RelionNode):
            return self._path == other._path
        return False

    def __repr__(self):
        return f"Node({repr(str(self._path))})"

    def __iter__(self):
        return iter(self._out)

    def link_to(self, next_node):
        if next_node not in self._out:
            self._out.append(next_node)


class PipelineNode(RelionNode):
    def __init__(self, in_node, origin_distance):
        super().__init__(in_node._path)
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
        if isinstance(origin, RelionNode):
            self._job_nodes = [origin]
            self._job_pnodes = [PipelineNode(origin, 0)]
        else:
            raise ValueError("The Pipeline origin must be a RelionNode")
        self._file_nodes = []
        self._file_pnodes = []
        self._nodes = []
        self._pnodes = []
        self._connected = {}
        self._pconnected = {}
        self.origins = {}

    def _add_node(self, new_node):
        if isinstance(new_node, RelionNode):
            self._nodes.append(new_node)
            return True
        elif isinstance(new_node, PipelineNode):
            self._pnodes.append(new_node)
            return True
        raise ValueError("Attempted to add a node that was not a RelionNode")

    def _node_explore(self, node, explored):
        if node not in explored:
            explored.append(node)
        for next_node in node:
            self._node_explore(next_node, explored)

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

    @staticmethod
    def _request_star_values(star_path, column, search=None):
        if search is None:
            search = column
        gemmi_readable_path = os.fspath(star_path)
        star_doc = cif.read_file(gemmi_readable_path)
        for block_index, block in enumerate(star_doc):
            if list(block.find_loop(search)):
                block_number = block_index
        data_block = star_doc[block_number]
        values = data_block.find_loop(column)
        return list(values)

    def _load_file_nodes_from_star(self, star_path):
        self._file_nodes = [
            RelionNode(pathlib.Path(p))
            for p in self._request_star_values(star_path, "_rlnPipeLineNodeName")
        ]

    def _load_job_nodes_from_star(self, star_path):
        self._job_nodes = [
            RelionNode(pathlib.Path(p))
            for p in self._request_star_values(star_path, "_rlnPipeLineProcessName")
        ]

    def load_nodes_from_star(self, star_path):
        self._load_file_nodes_from_star(star_path)
        self._load_job_nodes_from_star(star_path)
        self._nodes.extend(self._file_nodes)
        self._nodes.extend(self._job_nodes)
        binding_pairs = [
            (RelionNode(p1), RelionNode(p2))
            for p1, p2 in zip(
                self._request_star_values(star_path, "_rlnPipeLineEdgeFromNode"),
                self._request_star_values(
                    star_path,
                    "_rlnPipeLineEdgeProcess",
                    search="_rlnPipeLineEdgeFromNode",
                ),
            )
        ]
        binding_pairs.extend(
            [
                (RelionNode(p1), RelionNode(p2))
                for p1, p2 in zip(
                    self._request_star_values(
                        star_path,
                        "_rlnPipeLineEdgeProcess",
                        search="_rlnPipeLineEdgeToNode",
                    ),
                    self._request_star_values(star_path, "_rlnPipeLineEdgeToNode"),
                )
            ]
        )
        for f, t in binding_pairs:
            self._nodes[self._nodes.index(f)].link_to(self._nodes[self._nodes.index(t)])
        self._split_connected()
        self._traverse_and_count()

import collections.abc

try:
    from graphviz import Digraph
except ImportError:
    pass
from relion._parser.processnode import ProcessNode


class ProcessGraph(collections.abc.Sequence):
    def __init__(self, node_list):
        self._node_list = node_list

    def __eq__(self, other):
        if isinstance(other, ProcessGraph):
            if len(self) == len(other):
                for n in self:
                    if n not in other:
                        return False
                return True
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

    def link_from_to(self, from_node, to_node):
        self[self.index(from_node)].link_to(to_node)

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
        node_names = [p._path for p in self]
        other_names = [p._path for p in other]
        if len(set(node_names).intersection(set(other_names))) > 0:
            for new_node in other:
                if new_node._path not in self:
                    self.add_node(new_node)
                else:
                    for next_node in new_node:
                        if next_node._path not in self[self.index(new_node._path)]:
                            self[self.index(new_node._path)].link_to(next_node)
            return True
        else:
            return False

    def split_connected(self):
        if len(self._node_list) == 0:
            return []
        connected_graphs = []
        for origin in self.find_origins():
            curr_graph = []
            self.node_explore(origin, curr_graph)
            connected_graphs.append(ProcessGraph(curr_graph))
        for_removal = []
        for i, g in enumerate(connected_graphs):
            for ng in connected_graphs[i + 1 :]:
                if g.merge(ng):
                    if ng not in for_removal:
                        for_removal.append(ng)
        for r in for_removal:
            connected_graphs.remove(r)
        return connected_graphs

    def _split_connected(self, connected_dict, origin, origins_dict):
        connected_graphs = self.split_connected()
        ancillary_count = 1
        for cg in connected_graphs:
            if origin in cg:
                connected_dict["main"] = cg
                origins_dict["main"] = self[self.index(origin)]
            else:
                connected_dict[f"ancillary:{ancillary_count}"] = cg
                origins_dict[f"ancillary:{ancillary_count}"] = cg.find_origins()[0]
                ancillary_count += 1

    def show_all_nodes(self):
        try:
            digraph = Digraph(format="svg")
            digraph.attr(rankdir="LR")
            for node in self:
                digraph.node(name=str(node._path))
                for next_node in node:
                    digraph.edge(str(node._path), str(next_node._path))
            digraph.render("pipeline.gv")
        except Exception:
            raise Warning(
                "Failed to create nodes display. Your environment may not have graphviz available."
            )

    def wipe(self):
        self._node_list = []

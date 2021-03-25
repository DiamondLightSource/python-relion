from graphviz import Digraph
from relion._parser.processgraph import ProcessGraph


class Pipeline:
    def __init__(self, origin, graphin=ProcessGraph([])):
        self.origin = origin
        self._nodes = graphin
        self._connected = {}
        self.origins = {}

    @staticmethod
    def _split_connected(nodes, connected_dict, origin, origins_dict):
        connected_graphs = nodes.split_connected()
        ancillary_count = 1
        for cg in connected_graphs:
            if origin in cg:
                connected_dict["main"] = cg
                origins_dict["main"] = nodes[nodes.index(origin)]
            else:
                connected_dict[f"ancillary:{ancillary_count}"] = cg
                origins_dict[f"ancillary:{ancillary_count}"] = cg.find_origins()[0]
                ancillary_count += 1

    def show_all_nodes(self):
        digraph = Digraph(format="svg")
        digraph.attr(rankdir="LR")
        for node in self._nodes:
            digraph.node(name=str(node._path))
            for next_node in node:
                digraph.edge(str(node._path), str(next_node._path))
        digraph.render("pipeline.gv")

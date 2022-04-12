from __future__ import annotations

from relion.node import Node

try:
    from graphviz import Digraph
except ImportError:
    pass


class Graph(Node):
    def __init__(self, name, node_list, auto_connect=False):
        super().__init__(name)
        self._node_list = node_list
        try:
            self.origins = self.find_origins()
        except IndexError:
            self.origins = []
        self._call_returns = {}
        self._called_nodes = []
        self._traversed = []
        if auto_connect:
            self._check_connections()

    def __eq__(self, other):
        if isinstance(other, Graph):
            if len(self.nodes) == len(other.nodes):
                for n in self.nodes:
                    if n not in other.nodes:
                        return False
                return True
        return False

    def __hash__(self):
        return hash(
            ("relion.node.graph.Graph", self._name, iter(self._node_list), self.nodeid)
        )

    def __len__(self):
        return len(self._node_list)

    def __getitem__(self, index):
        if not isinstance(index, int):
            raise ValueError("Index of Graph must be an integer")
        return self._node_list[index]

    def __call__(self, *args, **kwargs):
        for node in self._node_list:
            node.environment.set_escalate(self.environment)
        res = super().__call__(*args, **kwargs)
        return res

    @property
    def nodes(self):
        return self._node_list

    def func(self, *args, **kwargs):
        self._call_returns = {}
        if self._in_multi_call:
            for node in self.origins:
                node._completed = self._completed
        self.traverse()
        self._traversed = []
        self._called_nodes = []
        for node in self.nodes:
            node.environment.reset()
            node._completed = []
        if self._call_returns == {}:
            return None
        else:
            return self._call_returns

    def _check_connections(self):
        for node in self._node_list:
            for i_node in node._in:
                if i_node not in self._node_list:
                    i_node.link_to(self)
                    i_node._link_traffic[self.nodeid].update(
                        i_node._link_traffic[node.nodeid]
                    )

    def extend(self, other):
        if not isinstance(other, Graph):
            raise ValueError("Can only extend a ProtoGraph with another Graph")
        self._node_list.extend(other._node_list)

    def index(self, node):
        return self._node_list.index(node)

    def link_from_to(self, from_node, to_node):
        self[self.index(from_node)].link_to(to_node)

    def node_explore(self, node, explored):
        if not isinstance(node, Node):
            raise ValueError(
                "Graph.node_explore must be called with a ProtoNode as the starting point; a string or similar is insufficient"
            )
        if node not in explored:
            explored.append(node)
        for next_node in node:
            self.node_explore(next_node, explored)

    def add_node(self, new_node, auto_connect=False):
        if isinstance(new_node, Node):
            self._node_list.append(new_node)
            if auto_connect:
                for i_node in new_node._in:
                    if i_node not in self._node_list:
                        i_node.link_to(self)
            new_node.environment.set_escalate(self.environment)
        else:
            raise ValueError("Attempted to add a node that was not a Node")

    def remove_node(self, node_name, advance=False):
        behind_nodes = []
        for currnode in self._node_list:
            if currnode.name == str(node_name):
                if currnode.environment.propagate.released:
                    for next_node in currnode:
                        next_node.environment.update_prop(
                            currnode.environment.propagate.store
                        )
                        if advance:
                            next_node.environment.update(
                                currnode.environment.propagate.store
                            )
            if node_name in currnode:
                behind_nodes.append(currnode)
                currnode.unlink_from(node_name)
            if node_name in currnode._in:
                currnode._in.remove(node_name)
        for bnode in behind_nodes:
            for next_node in self._node_list[self._node_list.index(node_name)]:
                bnode.link_to(next_node)
        self._node_list.remove(node_name)

    def find_origins(self):
        child_nodes = []
        for node in self.nodes:
            child_nodes.extend(node)
        origins = [p for p in self.nodes if p not in child_nodes]
        return origins

    def merge(self, other):
        node_names = [p._name for p in self.nodes]
        other_names = [p._name for p in other.nodes]
        if len(set(node_names).intersection(set(other_names))) > 0:
            for new_node in other.nodes:
                if new_node not in self.nodes:
                    self.add_node(new_node)
                else:
                    for next_node in new_node:
                        if next_node not in self.nodes[self.index(new_node)]:
                            self.nodes[self.index(new_node)].link_to(next_node)
            return True
        else:
            return False

    def traverse(self):
        for o in self.origins:
            self._follow(o, traffic={}, share=[], append=o._can_append)

    def _follow(self, node, traffic, share, append=False):
        called = False
        if node not in self.nodes:
            return
        if node.nodeid in self._called_nodes:
            called = True
        node.environment.update(traffic, can_append_list=append)

        for sh in share:
            node.environment[sh[1]] = sh[0]

        if (
            all(n in node._completed for n in node._in)
            and node.nodeid not in self._called_nodes
        ):
            called = True

            self._call_returns[node.nodeid] = node()
            self._called_nodes.append(node.nodeid)

        for next_node in node:
            next_node.environment.update_prop(node.environment.propagate)
            next_traffic = node._link_traffic.get(next_node.nodeid, {})
            if next_traffic is None:
                next_traffic = self._call_returns[node.nodeid]
            next_share = []
            if node._share_traffic.get(next_node.nodeid) is not None:
                for sh in node._share_traffic[next_node.nodeid]:
                    next_share.append((node.environment[sh[0]], sh[1]))
            if (node.nodeid, next_node.nodeid) not in self._traversed and called:
                self._traversed.append((node.nodeid, next_node.nodeid))
                self._follow(
                    next_node,
                    next_traffic,
                    next_share,
                    append=node._can_append,
                )
            elif not called:
                self._follow(
                    next_node,
                    next_traffic,
                    next_share,
                    append=node._can_append,
                )

    def show(self):
        try:
            digraph = Digraph(format="svg", strict=True)
            digraph.attr(rankdir="LR", splines="ortho")
            for node in self._node_list:
                if isinstance(node, Graph):
                    digraph.node(name=str(node.name), shape="box")
                    for gnode in node._node_list:
                        if isinstance(gnode, Graph):
                            digraph.node(name=str(gnode.name), shape="box")
                        else:
                            digraph.node(name=str(gnode.name), shape=gnode.shape)
                        digraph.edge(str(node.name), str(gnode.name), style="dashed")
                        for next_gnode in gnode:
                            digraph.edge(str(gnode.name), str(next_gnode.name))
                else:
                    digraph.node(name=str(node.name), shape=node.shape)
                for next_node in node:
                    if next_node in self._node_list:
                        digraph.edge(str(node.name), str(next_node.name))
            digraph.render(f"{self.name}.gv")
        except Exception:
            raise Warning(
                "Failed to create nodes display. Your environment may not have graphviz available."
            )

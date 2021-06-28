import functools
import uuid

from relion.node.environment import Environment


@functools.total_ordering
class Node:
    def __init__(self, name, **kwargs):
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
        incomplete = self.environment.step()
        while incomplete:
            curr_res = self.func(*args, **kwargs)
            if isinstance(curr_res, list):
                res.extend(curr_res)
            else:
                res.append(curr_res)
            incomplete = self.environment.step()
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
        pass

    @property
    def name(self):
        return str(self._name)

    def change_name(self, new_name):
        self._name = new_name

    def link_to(
        self, next_node, traffic=None, result_as_traffic=False, share=None, append=False
    ):
        if next_node not in self._out:
            self._out.append(next_node)
            next_node._in.append(self)
            if append:
                self._append_traffic[next_node.nodeid] = True
            if traffic is None:
                self._link_traffic[next_node.nodeid] = {}
            else:
                self._link_traffic[next_node.nodeid] = traffic
            if result_as_traffic:
                self._link_traffic[next_node.nodeid] = None
            if share is not None:
                self._share_traffic[next_node.nodeid] = share

    def share_with(self, node, share):
        if self._link_traffic.get(node.nodeid) is None:
            print(
                "Node not already linked or linked with result_as_traffic in share_with"
            )
            return
        if isinstance(self._link_traffic[node.nodeid], dict):
            self._link_traffic[node.nodeid].update(
                {share[1]: self.environment.get(share[0])}
            )
        elif isinstance(self._link_traffic[node.nodeid], list):
            for tr in self._link_traffic[node.nodeid]:
                tr.update({share[1]: self.environment.get(share[0])})

    def propagate(self, share):
        self.environment.propagate.update({share[1]: self.environment[share[0]]})

    def unlink_from(self, next_node):
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

import pathlib
import functools


@functools.total_ordering
class ProcessNode:
    def __init__(self, path, **kwargs):
        self._path = pathlib.PurePosixPath(path)
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
            if self._path == other._path and len(self._out) == len(other._out):
                for n in self._out:
                    if n not in other._out:
                        return False
                return True
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

    @property
    def name(self):
        return str(self._path)

    def change_name(self, new_name):
        self._path = new_name

    def link_to(self, next_node):
        if next_node not in self._out:
            self._out.append(next_node)

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

from __future__ import annotations

from relion.node.graph import Graph


class DBGraph(Graph):
    def __init__(self, name, node_list, auto_connect=False):
        super().__init__(name, node_list, auto_connect=auto_connect)
        self.environment["source"] = self.name
        for n in self._node_list:
            for tab in n.tables:
                tab._last_update[self.name] = 0

    def __call__(self, *args, **kwargs):
        res = super().__call__(*args, **kwargs)
        collapsed_res = []
        if isinstance(res, dict):
            for curr_res in res.values():
                if curr_res is not None and curr_res not in collapsed_res:
                    collapsed_res.append(curr_res)
        elif isinstance(res, list):
            for el in res:
                if el is None:
                    continue
                if not el:
                    continue
                for curr_res in el.values():
                    if curr_res is not None and curr_res not in collapsed_res:
                        collapsed_res.append(curr_res)
        return collapsed_res

    def update_times(self, source=None):
        times = []
        for n in self._node_list:
            times.extend(n.update_times(source))
        return times

    def message(self, constructor=None):
        messages = []
        for node in self._node_list:

            messages.extend(node.message, constructor)
        return messages

from __future__ import annotations

import pathlib
from datetime import datetime

from relion.node import Node


class ProcessNode(Node):
    def __init__(self, path, **kwargs):
        super().__init__(str(path), **kwargs)
        self._path = pathlib.PurePosixPath(path)
        self.environment["status"] = kwargs.get("status")
        self.environment["start_time_stamp"] = kwargs.get("start_time_stamp")
        self.environment["end_time_stamp"] = kwargs.get("end_time_stamp")
        self.environment["start_time"] = kwargs.get("start_time")
        self.environment["end_time"] = kwargs.get("end_time")
        self.environment["drop"] = kwargs.get("drop") or []
        self.db_node = None

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
        return hash(("relion._parser.ProcessNode", self._path))

    def func(self, *args, **kwargs):
        if self.environment.get("result") is None:
            return
        if self.environment.get("end_time_stamp") is None:
            return []
        self.environment["end_time"] = datetime.timestamp(
            self.environment["end_time_stamp"]
        )
        if not self.environment["status"]:
            return {}
        if self.environment.get("results_last_collected") is None or self.environment[
            "results_last_collected"
        ] < datetime.timestamp(self.environment["end_time_stamp"]):
            self.environment["results_last_collected"] = datetime.timestamp(
                self.environment["end_time_stamp"]
            )

            db_results = self.environment["result"].db_unpack(
                self.environment["result"][self.environment["job"]]
            )
            if self.environment["inject"]:
                for inj in self.environment["inject"]:
                    if inj[1] in self.environment["drop"]:
                        continue
                    if isinstance(db_results, dict):
                        db_results[inj[0]] = self.environment[inj[1]]
                    elif isinstance(db_results, list):
                        for r in db_results:
                            r[inj[0]] = self.environment[inj[1]]
            return db_results
        return {}

    def change_name(self, new_name):
        self._path = new_name
        self._name = str(new_name)

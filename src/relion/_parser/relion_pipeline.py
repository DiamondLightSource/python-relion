from gemmi import cif
import os
import pathlib
from graphviz import Digraph
import copy
import datetime
import calendar
from relion._parser.pipeline import Pipeline, ProcessNode, ProcessGraph


class RelionPipeline(Pipeline):
    def __init__(self, origin):
        super().__init__(origin)
        self._job_nodes = []

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
        return ProcessGraph(
            [
                ProcessNode(pathlib.Path(p))
                for p in self._request_star_values(star_path, "_rlnPipeLineNodeName")
            ]
        )

    def _load_job_nodes_from_star(self, star_path):
        return ProcessGraph(
            [
                ProcessNode(pathlib.Path(p))
                for p in self._request_star_values(star_path, "_rlnPipeLineProcessName")
            ]
        )

    def load_nodes_from_star(self, star_path):
        self._nodes.wipe()
        file_nodes = self._load_file_nodes_from_star(star_path)
        job_nodes = self._load_job_nodes_from_star(star_path)
        self._nodes.extend(file_nodes)
        self._nodes.extend(job_nodes)
        binding_pairs = [
            (ProcessNode(p1), ProcessNode(p2))
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
                (ProcessNode(p1), ProcessNode(p2))
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
            self._nodes[self._nodes.index(f._path)].link_to(
                self._nodes[self._nodes.index(t._path)]
            )
        self._split_connected()
        self._set_job_nodes(star_path)
        print([(p, p._out) for p in self._nodes])

    def check_job_node_statuses(self, basepath):
        for node in self._job_nodes:
            success = basepath / node._path / "RELION_JOB_EXIT_SUCCESS"
            failure = basepath / node._path / "RELION_JOB_EXIT_FAILURE"
            if failure.is_file():
                node.attributes["status"] = False
                node.attributes["end_time_stamp"] = datetime.datetime.fromtimestamp(
                    failure.stat().st_ctime
                )
            elif success.is_file():
                node.attributes["status"] = True
                node.attributes["end_time_stamp"] = datetime.datetime.fromtimestamp(
                    success.stat().st_ctime
                )
            else:
                node.attributes["status"] = None

    def _set_job_nodes(self, star_path):
        self._job_nodes = copy.deepcopy(self._nodes)
        file_nodes = self._load_file_nodes_from_star(star_path)
        for fnode in file_nodes:
            self._job_nodes.remove_node(fnode._path)

    def show_job_nodes(self, basepath):
        digraph = Digraph(format="svg")
        digraph.attr(rankdir="LR")
        for node in self._job_nodes:
            if node.attributes["status"]:
                node_colour = "mediumseagreen"
            elif node.attributes["status"] is None:
                node_colour = "lightgray"
            else:
                node_colour = "orangered"
            if node.attributes["start_time_stamp"] is not None:
                sstamp = node.attributes["start_time_stamp"]
            else:
                sstamp = "???"
            if node.attributes["end_time_stamp"] is not None:
                estamp = node.attributes["end_time_stamp"]
            else:
                estamp = "???"
            digraph.node(
                name=str(node._path),
                shape="hexagon",
                style="filled",
                fillcolor=node_colour,
                color="purple",
                tooltip=f"Start: {sstamp}&#013;End: {estamp}",
            )
            for next_node in node:
                stime = next_node.attributes["start_time"]
                etime = next_node.attributes["end_time"]

                if stime is not None and etime is not None:
                    digraph.edge(
                        str(node._path),
                        str(next_node._path),
                        label=f"{stime} / {etime}",
                    )
                elif stime is not None:
                    digraph.edge(
                        str(node._path),
                        str(next_node._path),
                        label=f"{stime} / ???",
                    )
                elif etime is not None:
                    digraph.edge(
                        str(node._path),
                        str(next_node._path),
                        label=f"??? / {etime}",
                    )
                else:
                    digraph.edge(
                        str(node._path), str(next_node._path), label="??? / ???"
                    )
        digraph.render(basepath / "Pipeline" / "relion_pipeline_jobs.gv")

    def collect_job_times(self, schedule_logs):
        for job in self._job_nodes:
            jtime = self._lookup_job_time(schedule_logs, job)
            job.attributes["start_time_stamp"] = jtime
        self._calculate_relative_job_times()

    def _calculate_relative_job_times(self):
        times = [j.attributes["start_time_stamp"] for j in self._job_nodes]
        etimes = [j.attributes["end_time_stamp"] for j in self._job_nodes]
        already_started_times = [t for t in times if t is not None]
        origin = sorted(already_started_times)[0]
        relative_times = [(t - origin).total_seconds() for t in times if t is not None]
        just_seconds = lambda tdelta: tdelta - datetime.timedelta(
            microseconds=tdelta.microseconds
        )
        for node, rt in zip(self._job_nodes, relative_times):
            node.attributes["start_time"] = just_seconds(datetime.timedelta(seconds=rt))
        relative_etimes = [
            (t - origin).total_seconds() for t in etimes if t is not None
        ]
        for node, rt in zip(self._job_nodes, relative_etimes):
            node.attributes["end_time"] = just_seconds(datetime.timedelta(seconds=rt))

    def _lookup_job_time(self, schedule_logs, job):
        time = None
        job_found = False
        for log in schedule_logs:
            with open(log, "r") as slf:
                lines = slf.readlines()
                for lindex, line in enumerate(lines):
                    if "Executing" in line and str(job._path) in line and not job_found:
                        split_line = lines[lindex - 1].split()
                        time_split = split_line[4].split(":")
                        dtime = datetime.datetime(
                            int(split_line[5]),
                            list(calendar.month_abbr).index(split_line[2]),
                            int(split_line[3]),
                            int(time_split[0]),
                            int(time_split[1]),
                            int(time_split[2]),
                        )
                        time = dtime
                        job_found = True
        return time

    @property
    def current_job(self):
        for node in self._job_nodes:
            if (
                node.attributes["start_time_stamp"] is not None
                and node.attributes["status"] is None
            ):
                for next_node in node:
                    if next_node.attributes["start_time_stamp"] is not None:
                        break
                    else:
                        return node._path
        return None

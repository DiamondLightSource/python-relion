from gemmi import cif
import os
import pathlib
import warnings

try:
    from graphviz import Digraph
except ImportError:
    pass
import copy
import datetime
import calendar
from relion._parser.processnode import ProcessNode
from relion._parser.processgraph import ProcessGraph


class RelionPipeline:
    def __init__(self, origin, graphin=ProcessGraph([]), locklist=None):
        self.origin = origin
        self._nodes = graphin
        self._connected = {}
        self.origins = {}
        self._job_nodes = ProcessGraph([])
        self._jobtype_nodes = ProcessGraph([])
        self._connected_jobs = {}
        self.job_origins = {}
        self._jobs_collapsed = False
        self.locklist = locklist or []

    def __iter__(self):
        if not self._jobs_collapsed:
            self._collapse_jobs_to_jobtypes()
        return iter(self._jobtype_nodes)

    @property
    def _plock(self):
        return DummyLock()

    def _star_doc(self, star_path):
        gemmi_readable_path = os.fspath(star_path)
        if star_path in self.locklist:
            with self._plock as pl:
                if pl.obtained:
                    star_doc = cif.read_file(gemmi_readable_path)
                else:
                    # effectively return an empty star file
                    star_doc = cif.Document()
            return star_doc
        return cif.read_file(gemmi_readable_path)

    def _request_star_values(self, star_doc, column, search=None):
        if search is None:
            search = column
        block_number = None
        for block_index, block in enumerate(star_doc):
            if list(block.find_loop(search)):
                block_number = block_index
                break
        else:
            return []
        data_block = star_doc[block_number]
        values = data_block.find_loop(column)
        return list(values)

    def _load_file_nodes_from_star(self, star_doc):
        return ProcessGraph(
            [
                ProcessNode(pathlib.Path(p))
                for p in self._request_star_values(star_doc, "_rlnPipeLineNodeName")
            ]
        )

    def _load_job_nodes_from_star(self, star_doc):
        return ProcessGraph(
            [
                ProcessNode(pathlib.Path(p), alias=al)
                for p, al in zip(
                    self._request_star_values(star_doc, "_rlnPipeLineProcessName"),
                    self._request_star_values(star_doc, "_rlnPipeLineProcessAlias"),
                )
            ]
        )

    def load_nodes_from_star(self, star_path):
        self._nodes.wipe()
        star_doc_from_path = self._star_doc(star_path)
        file_nodes = self._load_file_nodes_from_star(star_doc_from_path)
        job_nodes = self._load_job_nodes_from_star(star_doc_from_path)
        self._nodes.extend(file_nodes)
        self._nodes.extend(job_nodes)
        binding_pairs = [
            (ProcessNode(p1), ProcessNode(p2))
            for p1, p2 in zip(
                self._request_star_values(
                    star_doc_from_path, "_rlnPipeLineEdgeFromNode"
                ),
                self._request_star_values(
                    star_doc_from_path,
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
                        star_doc_from_path,
                        "_rlnPipeLineEdgeProcess",
                        search="_rlnPipeLineEdgeToNode",
                    ),
                    self._request_star_values(
                        star_doc_from_path, "_rlnPipeLineEdgeToNode"
                    ),
                )
            ]
        )
        for f, t in binding_pairs:
            self._nodes[self._nodes.index(f._path)].link_to(
                self._nodes[self._nodes.index(t._path)]
            )
        self._nodes._split_connected(self._connected, self.origin, self.origins)
        self._set_job_nodes(star_doc_from_path)

    def check_job_node_statuses(self, basepath):
        for node in self._job_nodes:
            success = basepath / node._path / "RELION_JOB_EXIT_SUCCESS"
            failure = basepath / node._path / "RELION_JOB_EXIT_FAILURE"
            aborted = basepath / node._path / "RELION_JOB_EXIT_ABORTED"
            if failure.is_file():
                node.attributes["status"] = False
                node.attributes["end_time_stamp"] = datetime.datetime.fromtimestamp(
                    failure.stat().st_ctime
                )
            elif aborted.is_file():
                node.attributes["status"] = False
                node.attributes["end_time_stamp"] = datetime.datetime.fromtimestamp(
                    aborted.stat().st_ctime
                )
            elif success.is_file():
                node.attributes["status"] = True
                node.attributes["end_time_stamp"] = datetime.datetime.fromtimestamp(
                    success.stat().st_ctime
                )
            else:
                node.attributes["status"] = None

    def _set_job_nodes(self, star_doc):
        self._job_nodes = copy.deepcopy(self._nodes)
        file_nodes = self._load_file_nodes_from_star(star_doc)
        for fnode in file_nodes:
            self._job_nodes.remove_node(fnode._path)
        self._job_nodes._split_connected(
            self._connected_jobs, self.origin, self.job_origins
        )

    def _collapse_jobs_to_jobtypes(self):
        ordered_graph = []
        if len(self._nodes) == 0:
            self._jobtype_nodes = ProcessGraph([])
            return
        self._job_nodes.node_explore(
            self._job_nodes[self._job_nodes.index(self.origin)], ordered_graph
        )
        self._jobtype_nodes = ProcessGraph(copy.deepcopy(ordered_graph))
        for node in self._jobtype_nodes:
            node.attributes["job"] = node._path.name
            node._path = node._path.parent
        self._jobs_collapsed = True

    def show_all_nodes(self):
        self._nodes.show_all_nodes()

    def show_job_nodes(self, basepath):
        try:
            digraph = Digraph(format="svg")
        except Exception:
            warnings.warn(
                "Failed to create nodes display. Your environment may not have graphviz avaliable."
            )
            return
        digraph.attr(rankdir="LR")
        running_node = self.current_job
        for node in self._job_nodes:
            if node._path == running_node:
                bordercolour = "teal"
            else:
                bordercolour = "purple"
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
            if (
                node.attributes.get("alias") is not None
                and node.attributes.get("alias") != "None"
            ):
                nodename = node.attributes.get("alias")
            else:
                nodename = str(node._path)
            digraph.node(
                name=nodename,
                shape="hexagon",
                style="filled",
                fillcolor=node_colour,
                color=bordercolour,
                tooltip=f"Start: {sstamp}&#013;End: {estamp}&#013;Path: {node._path}",
            )
            for next_node in node:
                stime = next_node.attributes["start_time"]
                etime = next_node.attributes["end_time"]
                jcount = next_node.attributes["job_count"]

                if (
                    next_node.attributes.get("alias") is not None
                    and next_node.attributes.get("alias") != "None"
                ):
                    next_nodename = next_node.attributes.get("alias")
                else:
                    next_nodename = str(next_node._path)

                if stime is not None and etime is not None and jcount != 1:
                    digraph.edge(
                        nodename,
                        next_nodename,
                        label=f"{stime} / {etime} [{jcount}]",
                    )
                elif stime is not None and etime is not None:
                    digraph.edge(
                        nodename,
                        next_nodename,
                        label=f"{stime} / {etime}",
                    )
                elif stime is not None:
                    digraph.edge(
                        nodename,
                        next_nodename,
                        label=f"{stime} / ???",
                    )
                elif etime is not None:
                    digraph.edge(
                        nodename,
                        next_nodename,
                        label=f"??? / {etime}",
                    )
                else:
                    digraph.edge(nodename, next_nodename, label="??? / ???")
        digraph.render(basepath / "Pipeline" / "relion_pipeline_jobs.gv")

    def collect_job_times(self, schedule_logs):
        for job in self._job_nodes:
            jtime, jcount = self._lookup_job_time(schedule_logs, job)
            job.attributes["start_time_stamp"] = jtime
            job.attributes["job_count"] = jcount
        self._calculate_relative_job_times()

    def _calculate_relative_job_times(self):
        times = [j.attributes["start_time_stamp"] for j in self._job_nodes]
        etimes = [j.attributes["end_time_stamp"] for j in self._job_nodes]
        already_started_times = [t for t in times if t is not None]
        try:
            origin = sorted(already_started_times)[0]
        except IndexError:
            return
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
        jobcount = 0
        for log in schedule_logs:
            with open(log, "r") as slf:
                lines = slf.readlines()
                for lindex, line in enumerate(lines):
                    if "Executing" in line and str(job._path) in line:
                        split_line = lines[lindex - 1].split()
                        time_split = split_line[4].split(":")
                        dtime = datetime.datetime(
                            year=int(split_line[5]),
                            month=list(calendar.month_abbr).index(split_line[2]),
                            day=int(split_line[3]),
                            hour=int(time_split[0]),
                            minute=int(time_split[1]),
                            second=int(time_split[2]),
                        )
                        if time is None or dtime < time:
                            time = dtime
                        jobcount += 1
        return time, jobcount

    @property
    def current_jobs(self):
        running_jobs = []
        for node in self._job_nodes:
            if (
                node.attributes["start_time_stamp"] is not None
                and node.attributes["status"] is None
            ):
                running_jobs.append(node)
        if len(running_jobs) == 0:
            return None
        else:
            return running_jobs


class DummyLock:
    def __init__(self):
        self.obtained = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        raise NotImplementedError(
            "A dummy pipeline lock is being used. An actual pipeline lock should be implemented"
        )

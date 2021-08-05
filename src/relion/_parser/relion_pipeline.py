import os
import pathlib
import warnings

from gemmi import cif

try:
    from graphviz import Digraph
except ImportError:
    pass
import calendar
import copy
import datetime

from relion._parser.processgraph import ProcessGraph
from relion._parser.processnode import ProcessNode


class RelionPipeline:
    def __init__(self, origin, graphin=ProcessGraph("nodes", []), locklist=None):
        self.origin = origin
        self._nodes = graphin
        self._connected = {}
        self.origins = {}
        self._job_nodes = ProcessGraph("job nodes", [])
        self._jobtype_nodes = ProcessGraph("job type nodes", [])
        self._connected_jobs = {}
        self.job_origins = {}
        self._jobs_collapsed = False
        self.locklist = locklist or []
        self.preprocess = []

    def __iter__(self):
        if not self._jobs_collapsed:
            self._collapse_jobs_to_jobtypes()
        return iter(self._jobtype_nodes._node_list)

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
            "file nodes",
            [
                ProcessNode(pathlib.Path(p))
                for p in self._request_star_values(star_doc, "_rlnPipeLineNodeName")
            ],
        )

    def _load_job_nodes_from_star(self, star_doc):
        drops = {}
        drops["InitialModel"] = ["batch_number"]
        return ProcessGraph(
            "job nodes",
            [
                ProcessNode(
                    pathlib.Path(p),
                    independent=True,
                    alias=al,
                    drop=drops.get(p.split("/")[0]),
                )
                for p, al in zip(
                    self._request_star_values(star_doc, "_rlnPipeLineProcessName"),
                    self._request_star_values(star_doc, "_rlnPipeLineProcessAlias"),
                )
            ],
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
            if str(f._path.parent.parent) == "Select" and f._path.name.startswith(
                "particles_split"
            ):
                self._nodes[self._nodes.index(f._path)].environment[
                    "batch_number"
                ] = f._path.stem.replace("particles_split", "")
                self._nodes[self._nodes.index(f._path)].environment["inject"] = [
                    ("batch_number", "batch_number")
                ]
                self._nodes[self._nodes.index(f._path)].propagate(
                    ("batch_number", "batch_number")
                )
                self._nodes[self._nodes.index(f._path)].propagate(("inject", "inject"))
            if str(f._path.parent.parent) == "InitialModel" and "class" in f._path.name:
                self._nodes[self._nodes.index(f._path)].environment[
                    "init_model_class_num"
                ] = int(f._path.stem.split("class")[-1].split("_")[0])
                self._nodes[self._nodes.index(f._path)].propagate(
                    ("init_model_class_num", "init_model_class_num")
                )
        self._nodes._split_connected(self._connected, self.origin, self.origins)
        self._set_job_nodes(star_doc_from_path)

    def check_job_node_statuses(self, basepath):
        for node in self._job_nodes:
            success = basepath / node._path / "RELION_JOB_EXIT_SUCCESS"
            failure = basepath / node._path / "RELION_JOB_EXIT_FAILURE"
            aborted = basepath / node._path / "RELION_JOB_EXIT_ABORTED"
            # the try/except blocks below catch the case where Relion removes
            # the SUCCESS/FAILURE file in between checking for its existence
            # and checking its modification time
            try:
                node.environment["end_time_stamp"] = datetime.datetime.fromtimestamp(
                    failure.stat().st_mtime
                )
                node.environment["status"] = False
                continue
            except FileNotFoundError:
                pass
            try:
                node.environment["end_time_stamp"] = datetime.datetime.fromtimestamp(
                    aborted.stat().st_mtime
                )
                node.environment["status"] = False
                continue
            except FileNotFoundError:
                pass
            try:
                node.environment["end_time_stamp"] = datetime.datetime.fromtimestamp(
                    success.stat().st_mtime
                )
                node.environment["status"] = True
                continue
            except FileNotFoundError:
                pass
            node.environment["status"] = None

    def _set_job_nodes(self, star_doc):
        self._job_nodes = copy.deepcopy(self._nodes)
        file_nodes = self._load_file_nodes_from_star(star_doc)
        for fnode in file_nodes:
            if str(
                fnode._path.parent.parent
            ) == "Select" and fnode._path.name.startswith("particles_split"):
                self._job_nodes.remove_node(fnode._path, advance=True)
            else:
                self._job_nodes.remove_node(fnode._path)
        self._job_nodes._split_connected(
            self._connected_jobs, self.origin, self.job_origins
        )

    def _collapse_jobs_to_jobtypes(self):
        ordered_graph = []
        if len(self._nodes) == 0:
            self._jobtype_nodes = ProcessGraph("job type nodes", [])
            return
        self._job_nodes.node_explore(
            self._job_nodes[self._job_nodes.index(self.origin)], ordered_graph
        )
        self._jobtype_nodes = ProcessGraph(
            "job type nodes", copy.deepcopy(ordered_graph)
        )
        for node in self._jobtype_nodes:
            node.environment["job"] = node._path.name
            job_string = str(node._path.name)
            node._path = node._path.parent
            for inode in node._in:
                inode._link_traffic[node.nodeid] = inode._link_traffic[node.nodeid]
            node._name = str(node._path)
            if node.name == "InitialModel":
                node.environment["ini_model_job_string"] = job_string
            else:
                node.environment["job_string"] = job_string
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
            if node.environment["status"]:
                node_colour = "mediumseagreen"
            elif node.environment["status"] is None:
                node_colour = "lightgray"
            else:
                node_colour = "orangered"
            if node.environment["start_time_stamp"] is not None:
                sstamp = node.environment["start_time_stamp"]
            else:
                sstamp = "???"
            if node.environment["end_time_stamp"] is not None:
                estamp = node.environment["end_time_stamp"]
            else:
                estamp = "???"
            if (
                node.environment.get("alias") is not None
                and node.environment.get("alias") != "None"
            ):
                nodename = node.environment.get("alias")
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
                stime = next_node.environment["start_time"]
                etime = next_node.environment["end_time"]
                jcount = next_node.environment["job_count"]

                if (
                    next_node.environment["alias"] is not None
                    and next_node.environment["alias"] != "None"
                ):
                    next_nodename = next_node.environment["alias"]
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

    def collect_job_times(self, schedule_logs, preproc_log=None):
        for job in self._job_nodes:
            jtime, jcount = self._lookup_job_time(schedule_logs, job)
            job.environment["start_time_stamp"] = jtime
            job.environment["job_count"] = jcount
        self._calculate_relative_job_times()
        self.preprocess = self._get_pipeline_jobs(preproc_log)

    def _get_pipeline_jobs(self, logfile):
        if logfile is None:
            return []
        if logfile.is_file():
            with open(logfile, "r") as lfile:
                try:
                    return [
                        self._job_nodes[
                            self._job_nodes.index(
                                pathlib.PurePosixPath(line.split()[1])
                            )
                        ]
                        for line in lfile
                        if line.startswith(" - ")
                    ]
                # if it doesn't find the job in self._job_nodes then return empty list
                # should sort itself out later
                except ValueError:
                    return []
        return []

    def _calculate_relative_job_times(self):
        times = [j.environment["start_time_stamp"] for j in self._job_nodes]
        etimes = [j.environment["end_time_stamp"] for j in self._job_nodes]
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
            node.environment["start_time"] = just_seconds(
                datetime.timedelta(seconds=rt)
            )
        relative_etimes = [
            (t - origin).total_seconds() for t in etimes if t is not None
        ]
        for node, rt in zip(self._job_nodes, relative_etimes):
            node.environment["end_time"] = just_seconds(datetime.timedelta(seconds=rt))

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
                node.environment["start_time_stamp"] is not None
                and node.environment["status"] is None
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

from __future__ import annotations

from datetime import datetime

from relion.dbmodel import modeltables
from relion.node import Node


class DBNode(Node):
    def __init__(self, name, tables, **kwargs):
        super().__init__(name, **kwargs)
        self.shape = "octagon"
        if not isinstance(tables, list):
            raise TypeError(f"{self} could not be initialised: tables must be a list")
        self.tables = tables
        self._append_sent = {}
        for table in self.tables:
            table._last_update[self.name] = 0
            self._append_sent[table] = {a: set() for a in table._append}

        self._sent = [[] for _ in self.tables]
        self._unsent = [[] for _ in self.tables]
        self._all_sent = [[] for _ in self.tables]

    def __eq__(self, other):
        if isinstance(other, DBNode):
            if self.name == other.name and len(self._out) == len(other._out):
                for n in self._out:
                    if n not in other._out:
                        return False
                return True
        return False

    def __hash__(self):
        return hash(("relion.dbmodel.dbnode.DBNode", self._name))

    def __repr__(self):
        return f"Node({repr(str(self._name))})"

    def __bool__(self):
        if self.tables:
            return True
        return False

    def func(self, *args, **kwargs):
        if self.environment.empty:
            return []
        extra_options = self.environment["extra_options"]
        if self.environment["end_time"] is not None:
            end_time = datetime.timestamp(self.environment["end_time"])
        else:
            end_time = None
        msg_cons = self.environment["message_constructors"]
        self.insert(end_time, extra_options)
        return self.message(msg_cons)

    def update_times(self, source=None):
        if source is None:
            all_times = []
            for tab in self.tables:
                for k in tab._last_update.keys():
                    all_times.append(tab._last_update[k])
        return [tab._last_update[source] for tab in self.tables]

    def insert(self, end_time, extra_options):
        source_option = self.environment["source"]
        for i, tab in enumerate(self.tables):
            self._do_check()
            if end_time is None:
                return
            if (
                tab._last_update[source_option or self.name] is None
                or end_time > tab._last_update[source_option or self.name]
            ):
                tab._last_update[source_option or self.name] = end_time

            pid = modeltables.insert(
                tab,
                end_time,
                source_option or self.name,
                extra_options,
                self.environment,
            )
            if pid is not None:
                self._unsent[i].append(pid)
                if pid in self._sent[i]:
                    self._sent[i].remove(pid)

    def _do_check(self):
        try:
            if self.environment["check_for"] is not None:
                table = self.environment["foreign_table"]
                check_for_foreign_name = self.environment["check_for_foreign_name"]
                if check_for_foreign_name is None:
                    check_for_foreign_name = self.environment["check_for"]
                indices = table.get_row_index(
                    check_for_foreign_name,
                    self.environment[self.environment["check_for"]],
                )
                if indices is None:
                    self.environment[self.environment["table_key"]] = None
                    return
                try:
                    entries = [table[table._primary_key][ci] for ci in indices]
                    if self.environment["first"]:
                        self.environment[self.environment["table_key"]] = entries[0]
                    else:
                        self.environment[self.environment["table_key"]] = entries
                    return
                except TypeError:
                    self.environment[self.environment["table_key"]] = table[
                        table._primary_key
                    ][indices]
                    return
            else:
                return
        except KeyError:
            return

    def message(self, constructors=None):
        if constructors is None:
            return {}
        messages = {msg_type: [] for msg_type in constructors.keys()}
        for tab_index, ids in enumerate(self._unsent):
            for pid in ids:
                for msg_type, constructor in constructors.items():
                    unsent_appended = {}
                    for acol in self.tables[tab_index]._append:
                        row = self.tables[tab_index].get_row_by_primary_key(pid)
                        try:
                            unsent_appended[acol] = [
                                e
                                for e in row[acol]
                                if e
                                not in self._append_sent[self.tables[tab_index]][acol]
                            ]
                        except TypeError:
                            if (
                                row[acol]
                                not in self._append_sent[self.tables[tab_index]][acol]
                            ):
                                unsent_appended[acol] = [row[acol]]
                        if unsent_appended.get(acol):
                            if isinstance(row[acol], list):
                                self._append_sent[self.tables[tab_index]][acol].update(
                                    set(row[acol])
                                )
                            else:
                                self._append_sent[self.tables[tab_index]][acol].add(
                                    row[acol]
                                )
                        # if there are no new values in the append but there has been a change such that
                        # the pid has ended up in _unsent then send one message to pick up the changes
                        elif row[acol]:
                            try:
                                unsent_appended[acol] = [row[acol][0]]
                            except TypeError:
                                unsent_appended[acol] = [row[acol]]
                    if pid in self._all_sent[tab_index]:
                        message = constructor(
                            self.tables[tab_index],
                            pid,
                            resend=True,
                            unsent_appended=unsent_appended,
                        )
                    else:
                        message = constructor(
                            self.tables[tab_index],
                            pid,
                            unsent_appended=unsent_appended,
                        )
                    if isinstance(message, dict):
                        messages[msg_type].append(message)
                    elif isinstance(message, list):
                        messages[msg_type].extend(message)
                    else:
                        raise TypeError(
                            f"message must be a dictionary or list but was {type(message)}: {message}"
                        )
                self._unsent[tab_index].remove(pid)
                self._sent[tab_index].append(pid)
                self._all_sent[tab_index].append(pid)
        need_to_pop = []
        for key, value in messages.items():
            if not value:
                need_to_pop.append(key)
        for key in need_to_pop:
            messages.pop(key)
        return messages

from __future__ import annotations

import functools


def update_append(d01, d02):
    for key, value in d02.items():
        if key in d01:
            if not isinstance(d01[key], list):
                d01[key] = [d01[key]]
            d01[key].append(value)
        else:
            d01[key] = value


class Propagate:
    def __init__(self):
        self.store = {}
        self.released = False

    def __getitem__(self, key):
        return self.store[key]

    def __setitem__(self, key, value):
        self.store[key] = value
        if not self.released:
            self.released = True

    def keys(self):
        if self.released:
            return self.store.keys()
        else:
            return []

    def update(self, in_dict):
        self.store.update(in_dict)
        if not self.released:
            self.released = True


class Escalate:
    def __init__(self):
        self.store = {}
        self.released = False

    def __getitem__(self, key):
        return self.store[key]

    def __setitem__(self, key, value):
        if self.released:
            self.store[key] = value

    def start(self, esc):
        self.store = esc
        self.released = True


class Iterate:
    def __init__(self, initial_store):
        self.appended = []
        self.store = initial_store

    def squash(self):
        if self.appended:
            squashed_appended = []
            for a in self.appended:
                squashed_appended.extend(a)
            if (
                len(squashed_appended) != len(self.store)
                and len(self.store)
                and self.store != ["__do not iterate__"]
            ):
                raise ValueError(
                    f"Attempted to update ProtoNode Environment with concatenated lists (from updating with can_append_list option) that was a different size to the pre-existing iterator: {len(squashed_appended)} vs. {len(self.store)}, {list(self.store)}"
                )
            if self.store != ["__do not iterate__"]:
                for i, tr in enumerate(squashed_appended):
                    self.store[i].update(tr)
            else:
                self.store = squashed_appended
            self.appended = []

    def __iter__(self):
        return iter(self.store)

    def update(self, u, can_append_list=False):
        if isinstance(u, dict):
            for s in self.store:
                s.update(u)
        if isinstance(u, list):
            if can_append_list:
                self.appended.append(u)
                return
            if (
                self.store == [{}]
                or self.store == []
                or self.store == ["__do not iterate__"]
            ):
                self.store = u
                return
            if len(u) != len(self.store):
                raise ValueError(
                    f"Attempted to update ProtoNode Environment with a list that was a different size to the pre-existing iterator: {len(u)} vs. {len(self.store)}"
                )
            for i, tr in enumerate(u):
                self.store[i].update(tr)


class Environment:
    def __init__(self, base=None):
        set_base(base, self)
        self.propagate = Propagate()
        self.escalate = Escalate()
        self.temp = {}

    def __getitem__(self, key):
        if key in self.base.keys():
            return self.base[key]
        if key in self.temp.keys():
            return self.temp[key]
        if self.propagate.released:
            if key in self.propagate.keys():
                return self.propagate[key]
        if self.escalate.released:
            return self.escalate[key]

    def __setitem__(self, key, value):
        if key in self.base.keys():
            self.base[key] = value
            return
        if key in self.temp.keys():
            self.temp[key] = value
            return
        self.base[key] = value

    def step(self):
        try:
            self.temp = next(self.iterator)
            if self.temp == {}:
                self.empty = True
            else:
                self.empty = False
            if self.temp == "__do not iterate__":
                self.temp = {}
            return True
        except StopIteration:
            self.reset()
            return False

    def get(self, key):
        return self[key]

    def update(self, traffic, append=False, can_append_list=False):
        if isinstance(traffic, dict):
            if append:
                update_append(self.base, traffic)
            else:
                self.base.update(traffic)
            return
        elif isinstance(traffic, list):
            self.iterate.update(traffic, can_append_list=can_append_list)

    def load_iterator(self):
        self.iterate.squash()
        self.iterator = iter(self.iterate)

    def set_escalate(self, esc):
        self.escalate.start(esc)

    def update_prop(self, prop):
        self.propagate.update(prop)

    def reset(self):
        self.iterate = Iterate(["__do not iterate__"])


@functools.singledispatch
def set_base(base, env: Environment):
    raise TypeError(
        "For a ProtoNode Environment the base must be a dictionary or a list"
    )


@set_base.register(type(None))
def _(base: type(None), env: Environment):
    env.base = {}
    env.iterate = Iterate(["__do not iterate__"])


@set_base.register(dict)
def _(base: dict, env: Environment):
    env.base = base
    env.iterate = Iterate(["__do not iterate__"])


@set_base.register(list)
def _(base: list, env: Environment):
    env.base = {}
    env.iterate = Iterate(base)

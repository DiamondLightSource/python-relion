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

    def update(self, traffic, append=False):
        if isinstance(traffic, dict):
            if append:
                update_append(self.base, traffic)
            else:
                self.base.update(traffic)
            return
        elif isinstance(traffic, list):
            if list(self.iterator) == [{}] or list(self.iterator) == []:
                self.iterator = iter(traffic)
                return
            if len(list(traffic)) != len(list(self.iterator)):
                raise ValueError(
                    "Attempted to update ProtoNode Environment with a list that was a different size to the pre-existing iterator"
                )
            for i, tr in enumerate(traffic):
                self.iterator[i].update(tr)

    def set_escalate(self, esc):
        self.escalate.start(esc)

    def update_prop(self, prop):
        self.propagate.update(prop)

    def reset(self):
        self.iterator = iter(["__do not iterate__"])


@functools.singledispatch
def set_base(base, env: Environment):
    raise TypeError(
        "For a ProtoNode Environment the base must be a dictionary or a list"
    )


@set_base.register(type(None))
def _(base: type(None), env: Environment):
    env.base = {}
    env.iterator = iter(["__do not iterate__"])


@set_base.register(dict)
def _(base: dict, env: Environment):
    env.base = base
    env.iterator = iter(["__do not iterate__"])


@set_base.register(list)
def _(base: list, env: Environment):
    env.base = {}
    env.iterator = iter(base)

import functools


@functools.total_ordering
class NULL:
    def __init__(self):
        pass

    def __repr__(self):
        return ""

    def __lt__(self, other):
        return True

    def __eq__(self, other):
        return isinstance(other, NULL)

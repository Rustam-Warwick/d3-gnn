from enum import Enum


class Op(Enum):
    NONE = 0
    ADD = 1
    REMOVE = 2
    UPDATE = 3
    SYNC = 4
    AGG = 5


class GraphQuery:
    def __init__(self, op: Op, part: int, element) -> None:
        self.op = op
        self.part = part
        self.element = element

    def __init__(self, op: Op, element) -> None:
        self.op = op
        self.part = None
        self.element = element


def query_for_part(query: GraphQuery, part: int) -> GraphQuery:
    return GraphQuery(query.op, part, query.element)
